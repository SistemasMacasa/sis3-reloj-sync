# sis3_reloj/gui_tab_sis3.py
import os
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, date
from pathlib import Path
import re
import json
import time
import threading
import types

from .zk_client import read_attendance, read_users, clear_attendance
from .file_sink import write_attendance_jsonl
from .config import BASE_DIR
from .state_store import load_state, save_state

from .sis3_sink import Sis3Config, send_attendance_to_sis3, probe_sis3


# ───────────────────────────────────────────────────────────────
# UX: Mensajes humanos (sin tecnicismos)
# ───────────────────────────────────────────────────────────────

def _human_status(s: str) -> str:
    s = (s or "").strip().lower()
    if s == "idle":
        return "Listo"
    if s == "running":
        return "Procesando…"
    if s == "error":
        return "Error"
    return s or "Listo"


def _human_reason(reason: str | None) -> str:
    reason = (reason or "").strip()
    mapping = {
        "no_new_records": "No hay checadas nuevas.",
        "header_disconnected": "Modo simulación: se guardó local (no se envió a SIS3).",
        "missing_sis3_config": "Falta configuración de SIS3 (URL/KEY).",
        "recovery_no_files": "No encontré archivos en ese rango.",
    }
    return mapping.get(reason, f"Sin cambios ({reason})" if reason else "Sin cambios.")

# ───────────────────────────────────────────────────────────────
# SIS3 config helpers
# ───────────────────────────────────────────────────────────────

def _build_sis3_cfg(cfg):
    sis3_base_url = (os.getenv("SIS3_BASE_URL") or "").strip() or str(getattr(cfg, "sis3_base_url", "") or "")
    sis3_api_key  = (os.getenv("SIS3_API_KEY") or "").strip() or str(getattr(cfg, "sis3_api_key", "") or "")
    sis3_timeout  = int((os.getenv("SIS3_TIMEOUT_SEC") or str(getattr(cfg, "sis3_timeout_sec", 20) or 20)).strip() or "20")

    if not sis3_base_url or not sis3_api_key:
        return None, {"ok": False, "error": "missing_sis3_config"}

    return Sis3Config(base_url=sis3_base_url, api_key=sis3_api_key, timeout_sec=sis3_timeout), None


# ───────────────────────────────────────────────────────────────
# Pipeline SIS3: incremental + local file + send + clear + checkpoint
# ───────────────────────────────────────────────────────────────

def _attendance_incremental_pipeline_sis3(ip: str, port: int, password: int, cfg, log, *, runtime_connected: bool = True) -> dict:
    log(f"[SIS3] Conectando a {ip}:{port} ...")

    try:
        all_records = read_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS3] ❌ Error al leer asistencia: {e}")
        return {"ok": False, "stage": "read_attendance", "error": str(e)}

    log(f"[SIS3] Se obtuvieron {len(all_records)} registros de asistencia (crudo).")

    output_dir = (BASE_DIR / cfg.output_dir).resolve()
    state_path = (output_dir / "sis3" / "state.json")
    state = load_state(state_path)

    if not state_path.exists():
        save_state(state_path, state)
        log(f"[SIS3] State creado: {state_path}")

    log(f"[SIS3] Checkpoint actual: {state.last_ok_ts.isoformat() if state.last_ok_ts else '(vacío)'}")

    if state.last_ok_ts:
        before = len(all_records)
        records = [
            r for r in all_records
            if isinstance(getattr(r, "timestamp", None), datetime) and r.timestamp > state.last_ok_ts
        ]
        log(f"[SIS3] Incremental activo. Filtrados {before - len(records)}. Nuevos: {len(records)}")
    else:
        records = all_records
        log("[SIS3] Incremental: checkpoint vacío. Se procesan todos.")

    if len(records) == 0:
        log("[SIS3] No hay registros nuevos. Nada que enviar ni limpiar.")
        return {"ok": True, "skipped": True, "reason": "no_new_records"}

    # Guardado: queremos carpeta SIS3 para que sea coherente con state_path y recovery
    path_local = write_attendance_jsonl(records, output_dir, subdir="sis3")
    log(f"[SIS3] Archivo guardado en: {path_local}")


    # Header desconectado = dry run (no envío, no clear)
    if not runtime_connected:
        log("[SIS3] Header=DESCONectado → DRY-RUN: se guarda local, no se envía y no se limpia.")
        return {"ok": True, "skipped": True, "reason": "header_disconnected", "local_path": str(path_local)}

    sis3_cfg, err = _build_sis3_cfg(cfg)
    if err:
        log("[SIS3] ❌ Falta configuración SIS3 (URL/KEY). No se envía y NO se limpia.")
        return {"ok": False, "stage": "sis3_sink", "error": "missing_sis3_config", "local_path": str(path_local)}

    file_tag = Path(str(path_local)).name
    try:
        sis3_result = send_attendance_to_sis3(
            records,
            sis3_cfg,
            device_ip=ip,
            device_port=port,
            file_tag=file_tag,
            mode="incremental",
            log=lambda m: log(f"[SIS3] {m}"),
        )
    except Exception as e:
        log(f"[SIS3] ❌ Error enviando a SIS3: {e}")
        log("[SIS3] No se limpia el dispositivo (SIS3 no confirmado).")
        return {"ok": False, "stage": "sis3_sink", "error": str(e), "local_path": str(path_local)}

    if not (sis3_result and sis3_result.get("ok") is True):
        log("[SIS3] No se limpia el dispositivo (SIS3 no confirmado o falló).")
        return {"ok": False, "stage": "sis3_sink", "sis3": sis3_result, "local_path": str(path_local)}

    # En transición (SIS2 conectado), NO limpiamos el reloj.
    # La idempotencia vive en SIS3 + checkpoint local evita reenvíos.
    if not bool(getattr(cfg, "sis2_disconnected", False)):
        log("[SIS3] Transición activa (SIS2 conectado) → NO se limpia el dispositivo.")
        max_ts = max((r.timestamp for r in records if isinstance(getattr(r, "timestamp", None), datetime)), default=None)
        if max_ts:
            state.last_ok_ts = max_ts
            save_state(state_path, state)
            log(f"[SIS3] Checkpoint actualizado (sin limpiar): last_ok_ts={max_ts.isoformat()}")
        return {"ok": True, "count": len(records), "local_path": str(path_local), "sis3": sis3_result, "no_clear": True}

    # Post-transición (SIS2 desconectado): aquí sí limpiamos
    try:
        log("[SIS3] OK confirmado. Limpiando registros de asistencia en el dispositivo...")
        ok_clear = clear_attendance(ip, port, password)

    except Exception as e:
        log(f"[SIS3] ⚠️ Error limpiando dispositivo: {e}")
        return {"ok": False, "stage": "clear", "error": str(e), "sis3": sis3_result}

    if not ok_clear:
        log("[SIS3] ⚠️ Limpieza no confirmada (retorno False). No se actualiza checkpoint.")
        return {"ok": False, "stage": "clear", "error": "clear_attendance returned False", "sis3": sis3_result}

    log("[SIS3] ✅ Dispositivo limpiado correctamente.")

    max_ts = max((r.timestamp for r in records if isinstance(getattr(r, "timestamp", None), datetime)), default=None)
    if max_ts:
        state.last_ok_ts = max_ts
        save_state(state_path, state)
        log(f"[SIS3] Checkpoint actualizado: last_ok_ts={max_ts.isoformat()}")

    return {"ok": True, "count": len(records), "local_path": str(path_local), "sis3": sis3_result}


# ───────────────────────────────────────────────────────────────
# UI Tab + Runner async
# ───────────────────────────────────────────────────────────────

def build_tab_sis3(parent, *, get_conn, get_config, log,
                  register_probe=None, is_sis3_connected=None):
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    # Status bar (más legible / tipo tablero)
    status_wrap = ttk.LabelFrame(frame, text="Estado de SIS3", padding=(10, 8))
    status_wrap.pack(fill=tk.X, pady=(0, 10))

    # fila 0: Estado + última ejecución
    ttk.Label(status_wrap, text="Estado:").grid(row=0, column=0, sticky="w")
    lbl_status = ttk.Label(status_wrap, text="Listo")
    lbl_status.grid(row=0, column=1, sticky="w", padx=(6, 18))

    ttk.Label(status_wrap, text="Última ejecución:").grid(row=0, column=2, sticky="w")
    lbl_last = ttk.Label(status_wrap, text="—")
    lbl_last.grid(row=0, column=3, sticky="w", padx=(6, 0))

    # fila 1: Resultado con wrap (para que no se “estire” raro)
    ttk.Label(status_wrap, text="Resultado:").grid(row=1, column=0, sticky="nw", pady=(8, 0))
    lbl_summary = ttk.Label(status_wrap, text="—", wraplength=620, justify="left")
    lbl_summary.grid(row=1, column=1, columnspan=3, sticky="w", padx=(6, 0), pady=(8, 0))

    status_wrap.columnconfigure(3, weight=1)


    runner = _SIS3Runner(
        tk_parent=frame,
        get_conn=get_conn,
        get_config=get_config,
        log=log,
        ui_set_status=lambda s: lbl_status.config(text=_human_status(s)),
        ui_set_last=lambda s: lbl_last.config(text=s),
        ui_set_summary=lambda s: lbl_summary.config(text=s),
        is_sis3_connected=is_sis3_connected,
    )

    def _probe_for_header():
        return runner.probe_sis3_for_header()

    if register_probe:
        register_probe(_probe_for_header)

    # Botones (centrados + jerarquía visual)
    actions = ttk.Frame(frame)
    actions.pack(fill=tk.X, pady=(6, 10))

    # contenedor centrador
    actions_center = ttk.Frame(actions)
    actions_center.pack(anchor="center")  # ← aquí se centra todo

    # estilos (no rompe lógica; solo look)
    style = ttk.Style()
    try:
        style.configure("SIS3.Primary.TButton", padding=(14, 10))
        style.configure("SIS3.Secondary.TButton", padding=(14, 10))
        style.configure("SIS3.Tertiary.TButton", padding=(14, 10))
    except Exception:
        pass

    # ancho uniforme (más corto y limpio que 36)
    BTN_W = 30

    # 1) Acción principal
    ttk.Button(
        actions_center,
        text="Enviar checadas nuevas",
        width=BTN_W,
        style="SIS3.Primary.TButton",
        command=lambda: runner.run("attendance"),
    ).pack(anchor="center", pady=(0, 10))

    # 2) Acción secundaria
    ttk.Button(
        actions_center,
        text="Probar conexión a SIS3",
        width=BTN_W,
        style="SIS3.Secondary.TButton",
        command=lambda: runner.run("probe"),
    ).pack(anchor="center", pady=(0, 10))

    # 3) Acción terciaria
    ttk.Button(
        actions_center,
        text="Ver empleados del reloj",
        width=BTN_W,
        style="SIS3.Tertiary.TButton",
        command=lambda: runner.run("read_users"),
    ).pack(anchor="center")

    return frame
class _SIS3Runner:
    def __init__(self, *, tk_parent, get_conn, get_config, log,
                 ui_set_status, ui_set_last, ui_set_summary,
                 is_sis3_connected=None):
        self.tk_parent = tk_parent
        self.get_conn = get_conn
        self.get_config = get_config
        self.log = log
        self.ui_set_status = ui_set_status
        self.ui_set_last = ui_set_last
        self.ui_set_summary = ui_set_summary
        self.is_sis3_connected = is_sis3_connected

        self._lock = threading.Lock()
        self._running = False

    def run(self, action: str):
        if self._running:
            self.log("[SIS3] Pipeline ya está corriendo; ignorando solicitud duplicada.")
            return
        t = threading.Thread(target=self._run_guarded, args=(action,), daemon=True)
        t.start()

    def _ui(self, fn):
        self.tk_parent.after(0, fn)

    def _runtime_connected(self) -> bool:
        if callable(self.is_sis3_connected):
            try:
                return bool(self.is_sis3_connected())
            except Exception:
                return False
        return True

    def probe_sis3_for_header(self):
        """
        Probe rápido (sin messagebox) para checkbox header:
        manda 1 registro dummy a endpoint? NO.
        Aquí solo valida que hay config y que el endpoint responde a un POST de prueba ligero.
        (Más adelante si quieres, metemos /health o HEAD si tu API lo permite.)
        """
        try:
            cfg = self.get_config()
            sis3_cfg, err = _build_sis3_cfg(cfg)
            if err or not sis3_cfg:
                return False, "[SIS3] Desconectado (falta URL/KEY)."

            # Probe real: pega al endpoint con 0 records (no inserta).
            probe_sis3(sis3_cfg, log=lambda m: self.log(f"[SIS3] {m}"))
            return True, "[SIS3] Conectado (API OK)."

        except Exception as e:
            return False, f"[SIS3] Desconectado (error): {e}"
    def _run_guarded(self, action: str):
        if not self._lock.acquire(blocking=False):
            self.log("[SIS3] Lock ocupado; pipeline ya está en ejecución.")
            return

        self._running = True
        started_dt = datetime.now()

        self._ui(lambda: self.ui_set_status("running"))
        self._ui(lambda: self.ui_set_summary("Procesando… por favor espera."))

        self.log(f"[SIS3] START action={action} @ {started_dt:%Y-%m-%d %H:%M:%S}")

        try:
            try:
                ip, port, password = self.get_conn()
            except ValueError:
                self._ui(lambda: messagebox.showerror("Error", "Port y Password deben ser numéricos."))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            cfg = self.get_config()

            if action == "probe":
                try:
                    ok_probe, msg = self.probe_sis3_for_header()
                    summary = "SIS3: " + ("Conectado" if ok_probe else "Desconectado")
                    self._ui(lambda: messagebox.showinfo("SIS3", msg))
                    ok = bool(ok_probe)
                except Exception as e:
                    summary = "SIS3: error"
                    self._ui(lambda: messagebox.showerror("SIS3", f"Error de conexión:\n{e}"))
                    ok = False

            elif action == "attendance":

                res = _attendance_incremental_pipeline_sis3(
                    ip, port, password, cfg, self.log,
                    runtime_connected=self._runtime_connected()
                )

                if res.get("ok") and res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Checadas: {human}"
                    self._ui(lambda: messagebox.showinfo("Listo", human))
                    ok = True

                elif res.get("ok"):
                    count = res.get("count", 0)
                    summary = f"Checadas: Enviadas {count} nueva(s)"
                    self._ui(lambda: messagebox.showinfo("Listo", f"Checadas enviadas correctamente.\nNuevas: {count}"))
                    ok = True

                else:
                    summary = f"Checadas: ERROR ({res.get('stage')})"
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudieron enviar checadas.\nDetalle: {res.get('stage')}\n{res}"))
                    ok = False

            elif action == "read_users":
                self.log(f"[SIS3] Conectando a {ip}:{port} para leer usuarios...")
                try:
                    users = read_users(ip, port, password)
                except Exception as e:
                    self.log(f"[SIS3] ❌ Error al leer usuarios: {e}")
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudo consultar empleados:\n{e}"))
                    self._ui(lambda: self.ui_set_status("Error"))
                    return

                def _clean(s: str) -> str:
                    s = (s or "")
                    return "".join(ch if (ch.isprintable() and ch not in "\r\n\t") else "?" for ch in s).strip()

                total = len(users)
                self.log(f"[SIS3] Usuarios leídos: {total}")
                self.log("[SIS3] Listado completo:")

                for u in users:
                    try:
                        uid = u.get("user_id") or u.get("id") or u.get("uid") or "?"
                        name = u.get("name") or u.get("nombre") or "(sin nombre)"
                        card = u.get("card") or u.get("card_number") or ""
                    except Exception:
                        uid = getattr(u, "user_id", None) or getattr(u, "id", None) or "?"
                        name = getattr(u, "name", None) or "(sin nombre)"
                        card = getattr(u, "card", None) or ""

                    uid = _clean(str(uid))
                    name = _clean(str(name))
                    card = _clean(str(card))

                    self.log(f"  - {uid} | {name} | {card}")

                summary = f"Empleados: {total} encontrados (ver Log)"
                self._ui(lambda: messagebox.showinfo("Listo", f"Empleados encontrados en el reloj: {total}\n(Consulta el Log para el detalle)"))
                ok = True

            else:
                self.log(f"[SIS3] Acción desconocida: {action}")
                self._ui(lambda: messagebox.showerror("Error", f"Acción desconocida: {action}"))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            ended_dt = datetime.now()
            self._ui(lambda: self.ui_set_last(f"{ended_dt:%Y-%m-%d %H:%M:%S}"))
            self._ui(lambda: self.ui_set_summary(summary))
            self._ui(lambda: self.ui_set_status("Idle" if ok else "Error"))

        except Exception as ex:
            self.log(f"[SIS3] ERROR action={action} → {ex!r}")
            self._ui(lambda: self.ui_set_status("Error"))
            self._ui(lambda: self.ui_set_summary(f"ERROR: {ex!r}"))
            self._ui(lambda: messagebox.showerror("Error", f"Fallo inesperado:\n{ex!r}"))

        finally:
            self._running = False
            try:
                self._lock.release()
            except Exception:
                pass
