# sis3_reloj/gui_tab_sis3.py
import os
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
from pathlib import Path
import time
import threading

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
    sis3_api_key = (os.getenv("SIS3_API_KEY") or "").strip() or str(getattr(cfg, "sis3_api_key", "") or "")
    sis3_timeout = int((os.getenv("SIS3_TIMEOUT_SEC") or str(getattr(cfg, "sis3_timeout_sec", 20) or 20)).strip() or "20")

    if not sis3_base_url or not sis3_api_key:
        return None, {"ok": False, "error": "missing_sis3_config"}

    return Sis3Config(base_url=sis3_base_url, api_key=sis3_api_key, timeout_sec=sis3_timeout), None


# ───────────────────────────────────────────────────────────────
# Pipeline SIS3: incremental + local file + send + (optional) clear + checkpoint
# ───────────────────────────────────────────────────────────────

def _attendance_incremental_pipeline_sis3(
    ip: str,
    port: int,
    password: int,
    cfg,
    log,
    *,
    runtime_connected: bool = True,
) -> dict:
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

    # Guardado coherente con state/recovery
    path_local = write_attendance_jsonl(records, output_dir, subdir="sis3")
    log(f"[SIS3] Archivo guardado en: {path_local}")

    # Modo simulación (dry-run): no envío, no clear
    if not runtime_connected:
        log("[SIS3] Modo simulación → DRY-RUN: se guarda local, no se envía y no se limpia.")
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
# (idéntico en estructura a SIS2: card Estado + grid de tiles + botón primario)
# ───────────────────────────────────────────────────────────────
def build_tab_sis3(
    parent,
    *,
    get_conn,
    get_config,
    log,
    register_probe=None,          # legacy
    is_sis3_connected=None,       # legacy (si existe, se respeta)
    ui_set_reloj_badge=None,      # app.set_reloj_badge_state(ok, phase=..., msg=..., auto_reset_ms=...)
    ui_clear_log=None,            # app.clear_log()
):
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    # Styles “tile”
    style = ttk.Style()
    try:
        style.configure("SIS3.Tile.View.TButton", padding=(14, 12))
        style.configure("SIS3.Tile.Send.TButton", padding=(14, 12))
        style.configure("SIS3.Tile.Primary.TButton", padding=(14, 12))

        style.configure("SIS3.Tile.View.TButton", foreground="#1d4ed8")
        style.configure("SIS3.Tile.Send.TButton", foreground="#047857")
    except Exception:
        pass

    # Styles badge SIS3 (local)
    try:
        style.configure("SIS3.Badge.Disconnected.TLabel", foreground="#7f1d1d", background="#fee2e2")
        style.configure("SIS3.Badge.Connecting.TLabel", foreground="#7c2d12", background="#ffedd5")
        style.configure("SIS3.Badge.Connected.TLabel", foreground="#14532d", background="#dcfce7")
    except Exception:
        pass

    # ─────────────────────────────────────────────
    # Card unificada: Estado (SIS3 + Ejecución)
    # ─────────────────────────────────────────────
    card = ttk.LabelFrame(frame, text="Estado", padding=(10, 10))
    card.pack(fill=tk.X, pady=(0, 10))

    lbl_sis3_badge = ttk.Label(
        card,
        text="SIS3: Desconectado",
        style="SIS3.Badge.Disconnected.TLabel",
        width=18,
        anchor="center",
    )
    lbl_sis3_badge.grid(row=0, column=0, sticky="w")

    btn_probe = ttk.Button(card, text="Probar conexión API")
    btn_probe.grid(row=0, column=1, sticky="w", padx=(10, 0))

    test_var = tk.BooleanVar(value=False)
    chk_test = ttk.Checkbutton(card, text="Prueba", variable=test_var)
    chk_test.grid(row=0, column=2, sticky="w", padx=(10, 0))

    card.columnconfigure(3, weight=1)

    ttk.Separator(card, orient="horizontal").grid(
        row=1, column=0, columnspan=4, sticky="ew", pady=(10, 10)
    )

    ttk.Label(card, text="Estado:").grid(row=2, column=0, sticky="w")
    lbl_status = ttk.Label(card, text="Listo")
    lbl_status.grid(row=2, column=1, sticky="w", padx=(6, 0))

    ttk.Label(card, text="Última ejecución:").grid(row=2, column=2, sticky="w", padx=(18, 0))
    lbl_last = ttk.Label(card, text="—")
    lbl_last.grid(row=2, column=3, sticky="w", padx=(6, 0))

    ttk.Label(card, text="Resultado:").grid(row=3, column=0, sticky="nw", pady=(8, 0))
    lbl_summary = ttk.Label(card, text="—", wraplength=620, justify="left")
    lbl_summary.grid(row=3, column=1, columnspan=3, sticky="w", padx=(6, 0), pady=(8, 0))

    runner = _SIS3Runner(
        tk_parent=frame,
        get_conn=get_conn,
        get_config=get_config,
        log=log,
        ui_set_status=lambda s: lbl_status.config(text=_human_status(s)),
        ui_set_last=lambda s: lbl_last.config(text=s),
        ui_set_summary=lambda s: lbl_summary.config(text=s),
        ui_set_sis3_badge=lambda ok, phase=None, msg=None, auto_reset_ms=None: _set_local_sis3_badge(
            frame, lbl_sis3_badge, ok=ok, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms, log=log
        ),
        ui_set_reloj_badge=ui_set_reloj_badge,
        ui_clear_log=ui_clear_log,
        is_test_mode=lambda: bool(test_var.get()),
        is_sis3_connected=is_sis3_connected,
    )

    # Probe async
    btn_probe.configure(command=lambda: runner.run("probe"))

    # Legacy hook
    if register_probe:
        register_probe(lambda: runner.probe_sis3_for_header())

    # ─────────────────────────────────────────────
    # Tiles (idénticas al grid SIS2)
    # ─────────────────────────────────────────────
    tiles = ttk.Frame(frame)
    tiles.pack(fill=tk.X, pady=(6, 10))

    BTN_W = 34
    tiles.columnconfigure(0, weight=1)
    tiles.columnconfigure(1, weight=1)

    ttk.Button(
        tiles,
        text="Ver empleados del reloj",
        width=BTN_W,
        style="SIS3.Tile.View.TButton",
        command=lambda: runner.run("read_users"),
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6), pady=(0, 8))

    ttk.Button(
        tiles,
        text="Ver asistencias del reloj",
        width=BTN_W,
        style="SIS3.Tile.View.TButton",
        command=lambda: runner.run("read_attendance"),
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0), pady=(0, 8))

    ttk.Button(
        tiles,
        text="Enviar checadas nuevas",
        width=BTN_W,
        style="SIS3.Tile.Send.TButton",
        command=lambda: runner.run("attendance"),
    ).grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(0, 10))

    ttk.Button(
        tiles,
        text="Probar conexión a SIS3",
        width=BTN_W,
        style="SIS3.Tile.Send.TButton",
        command=lambda: runner.run("probe"),
    ).grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(0, 10))

    ttk.Button(
        frame,
        text="Sincronizar todo",
        style="SIS3.Tile.Primary.TButton",
        command=lambda: runner.run("full"),
    ).pack(fill=tk.X)

    return frame


def _set_local_sis3_badge(
    tk_parent,
    lbl,
    *,
    ok: bool | None,
    phase: str | None,
    msg: str | None,
    auto_reset_ms: int | None,
    log,
):
    """
    Badge local SIS3 con auto-reset (socket-like).
    Guardamos after_id dentro del propio label para evitar duplicados.
    """
    # cancelar reset previo
    try:
        after_id = getattr(lbl, "_sis3_badge_after_id", None)
        if after_id is not None:
            tk_parent.after_cancel(after_id)
            setattr(lbl, "_sis3_badge_after_id", None)
    except Exception:
        try:
            setattr(lbl, "_sis3_badge_after_id", None)
        except Exception:
            pass

    ph = (phase or "").strip().lower()

    if ph == "connecting":
        try:
            lbl.config(text="SIS3: Conectando…", style="SIS3.Badge.Connecting.TLabel")
        except Exception:
            pass

    elif ph == "connected" or ok is True:
        try:
            lbl.config(text="SIS3: Conectado", style="SIS3.Badge.Connected.TLabel")
        except Exception:
            pass

        if auto_reset_ms and int(auto_reset_ms) > 0:
            def _reset():
                try:
                    lbl.config(text="SIS3: Desconectado", style="SIS3.Badge.Disconnected.TLabel")
                except Exception:
                    pass
                try:
                    setattr(lbl, "_sis3_badge_after_id", None)
                except Exception:
                    pass

            try:
                aid = tk_parent.after(int(auto_reset_ms), _reset)
                setattr(lbl, "_sis3_badge_after_id", aid)
            except Exception:
                pass

    else:
        try:
            lbl.config(text="SIS3: Desconectado", style="SIS3.Badge.Disconnected.TLabel")
        except Exception:
            pass

    if msg:
        log(msg)


class _SIS3Runner:
    def __init__(
        self,
        *,
        tk_parent,
        get_conn,
        get_config,
        log,
        ui_set_status,
        ui_set_last,
        ui_set_summary,
        ui_set_sis3_badge=None,
        ui_set_reloj_badge=None,
        ui_clear_log=None,
        is_test_mode=None,
        is_sis3_connected=None,   # legacy
    ):
        self.tk_parent = tk_parent
        self.get_conn = get_conn
        self.get_config = get_config
        self.log = log

        self.ui_set_status = ui_set_status
        self.ui_set_last = ui_set_last
        self.ui_set_summary = ui_set_summary

        self.ui_set_sis3_badge = ui_set_sis3_badge
        self.ui_set_reloj_badge = ui_set_reloj_badge
        self.ui_clear_log = ui_clear_log

        self.is_test_mode = is_test_mode or (lambda: False)
        self.is_sis3_connected = is_sis3_connected

        self._lock = threading.Lock()
        self._running = False

    def _ui(self, fn):
        self.tk_parent.after(0, fn)

    def _clear_log(self):
        if callable(self.ui_clear_log):
            self._ui(lambda: self.ui_clear_log())

    def _sis3_badge(self, ok: bool | None, *, phase: str | None = None, msg: str | None = None, auto_reset_ms: int | None = None):
        if callable(self.ui_set_sis3_badge):
            self._ui(lambda: self.ui_set_sis3_badge(ok, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms))
        elif msg:
            self.log(msg)

    def _reloj_badge(self, ok: bool | None, *, phase: str | None = None, msg: str | None = None, auto_reset_ms: int | None = None):
        if callable(self.ui_set_reloj_badge):
            self._ui(lambda: self.ui_set_reloj_badge(ok, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms))
        elif msg:
            self.log(msg)

    def run(self, action: str):
        if self._running:
            self.log("[SIS3] Pipeline ya está corriendo; ignorando solicitud duplicada.")
            return
        t = threading.Thread(target=self._run_guarded, args=(action,), daemon=True)
        t.start()

    def _runtime_connected(self) -> bool:
        """
        - Si Prueba está activa -> dry-run (False).
        - Si existe legacy is_sis3_connected -> se respeta.
        - Default -> True.
        """
        if bool(self.is_test_mode()):
            return False

        if callable(self.is_sis3_connected):
            try:
                return bool(self.is_sis3_connected())
            except Exception:
                return False

        return True

    def probe_sis3_for_header(self):
        """
        Probe rápido (sin messagebox) para integraciones legacy.
        Con probe_sis3(): 422 (validación) cuenta como OK, sin ensuciar BD.
        """
        try:
            cfg = self.get_config()
            sis3_cfg, err = _build_sis3_cfg(cfg)
            if err or not sis3_cfg:
                return False, "[SIS3] Desconectado (falta URL/KEY)."

            probe_res = probe_sis3(sis3_cfg, log=lambda m: self.log(f"[SIS3] {m}"))
            # probe_res típicamente: {ok: True, probe: "validation_422", status_code: 422, ...}
            return True, "[SIS3] Conectado (API OK)."

        except Exception as e:
            return False, f"[SIS3] Desconectado (error): {e}"

    def _run_guarded(self, action: str):
        if not self._lock.acquire(blocking=False):
            self.log("[SIS3] Lock ocupado; pipeline ya está en ejecución.")
            return

        self._running = True
        started_dt = datetime.now()

        self._clear_log()
        self._ui(lambda: self.ui_set_status("running"))
        self._ui(lambda: self.ui_set_summary("Procesando… por favor espera."))
        self.log(f"[SIS3] START action={action} @ {started_dt:%Y-%m-%d %H:%M:%S}")

        ok = False
        summary = "—"

        try:
            # ─────────────────────────────────────────
            # 1) PROBE API (NO requiere reloj)
            # ─────────────────────────────────────────
            if action == "probe":
                self._sis3_badge(None, phase="connecting", msg="[SIS3] Probando conexión a SIS3…")
                ok_probe, msg = self.probe_sis3_for_header()

                if ok_probe:
                    self._sis3_badge(True, phase="connected", msg="[SIS3] API OK. Conexión cerrada.", auto_reset_ms=1500)
                    summary = "SIS3: OK"
                    ok = True
                    self._ui(lambda: messagebox.showinfo("SIS3", msg))
                else:
                    self._sis3_badge(False, phase="disconnected", msg="[SIS3] API no accesible.")
                    summary = "SIS3: ERROR"
                    ok = False
                    self._ui(lambda: messagebox.showerror("SIS3", msg))

                ended_dt = datetime.now()
                self._ui(lambda: self.ui_set_last(f"{ended_dt:%Y-%m-%d %H:%M:%S}"))
                self._ui(lambda: self.ui_set_summary(summary))
                self._ui(lambda: self.ui_set_status("Idle" if ok else "Error"))
                return

            # ─────────────────────────────────────────
            # 2) Acciones que sí requieren reloj
            # ─────────────────────────────────────────
            try:
                ip, port, password = self.get_conn()
            except ValueError:
                self._ui(lambda: messagebox.showerror("Error", "Port y Password deben ser numéricos."))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            cfg = self.get_config()

            if action in ("read_users", "read_attendance", "attendance", "full"):
                self._reloj_badge(None, phase="connecting", msg="[SIS3] Conectando al reloj…")

            if action == "read_users":
                self.log(f"[SIS3] Conectando a {ip}:{port} para leer usuarios...")
                try:
                    users = read_users(ip, port, password)
                except Exception as e:
                    self._reloj_badge(False, phase="disconnected", msg=f"[SIS3] Reloj error: {e}")
                    self.log(f"[SIS3] ❌ Error al leer usuarios: {e}")
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudo consultar empleados:\n{e}"))
                    self._ui(lambda: self.ui_set_status("Error"))
                    return

                total = len(users)
                self.log(f"[SIS3] Usuarios leídos: {total}")
                summary = f"Empleados: {total} encontrados (ver Log)"
                ok = True

                self._reloj_badge(True, phase="connected", msg="[SIS3] Reloj OK. Conexión cerrada.", auto_reset_ms=1500)
                self._ui(lambda: messagebox.showinfo("Listo", f"Empleados encontrados en el reloj: {total}\n(Consulta el Log para el detalle)"))

            elif action == "read_attendance":
                self.log(f"[SIS3] Conectando a {ip}:{port} para leer asistencia...")
                try:
                    records = read_attendance(ip, port, password)
                except Exception as e:
                    self._reloj_badge(False, phase="disconnected", msg=f"[SIS3] Reloj error: {e}")
                    self.log(f"[SIS3] ❌ Error al leer asistencia: {e}")
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudo consultar asistencias:\n{e}"))
                    self._ui(lambda: self.ui_set_status("Error"))
                    return

                total = len(records)
                self.log(f"[SIS3] Asistencias leídas: {total}")
                summary = f"Asistencias: {total} encontradas (ver Log)"
                ok = True

                self._reloj_badge(True, phase="connected", msg="[SIS3] Reloj OK. Conexión cerrada.", auto_reset_ms=1500)
                self._ui(lambda: messagebox.showinfo("Listo", f"Asistencias encontradas en el reloj: {total}\n(Consulta el Log para una muestra)"))

            elif action in ("attendance", "full"):
                # full = probe + attendance (si probe falla, se aborta)
                if action == "full":
                    self._sis3_badge(None, phase="connecting", msg="[SIS3] Probando conexión a SIS3…")
                    ok_probe, msg = self.probe_sis3_for_header()
                    if not ok_probe:
                        self._sis3_badge(False, phase="disconnected", msg="[SIS3] API no accesible.")
                        self._reloj_badge(False, phase="disconnected", msg="[SIS3] Operación cancelada.")
                        summary = "Todo: ERROR (SIS3 no accesible)"
                        ok = False
                        self._ui(lambda: messagebox.showerror("Error", msg))
                        # cierre limpio (sin excepción extra)
                        ended_dt = datetime.now()
                        self._ui(lambda: self.ui_set_last(f"{ended_dt:%Y-%m-%d %H:%M:%S}"))
                        self._ui(lambda: self.ui_set_summary(summary))
                        self._ui(lambda: self.ui_set_status("Error"))
                        return

                    self._sis3_badge(True, phase="connected", msg="[SIS3] API OK. Conexión cerrada.", auto_reset_ms=1500)

                # pipeline
                self._sis3_badge(None, phase="connecting", msg="[SIS3] Enviando a SIS3…")
                res = _attendance_incremental_pipeline_sis3(
                    ip, port, password, cfg, self.log,
                    runtime_connected=self._runtime_connected(),
                )

                if res.get("ok"):
                    self._reloj_badge(True, phase="connected", msg="[SIS3] Reloj OK. Conexión cerrada.", auto_reset_ms=1500)

                if res.get("ok") and res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Checadas: {human}"
                    ok = True

                    if res.get("reason") == "header_disconnected":
                        self._sis3_badge(False, phase="disconnected", msg="[SIS3] Simulación: no se envió a SIS3.")
                    else:
                        self._sis3_badge(True, phase="connected", msg="[SIS3] OK. Conexión cerrada.", auto_reset_ms=1500)

                    self._ui(lambda: messagebox.showinfo("Listo", human))

                elif res.get("ok"):
                    count = res.get("count", 0)
                    extra = " | NO se limpió" if res.get("no_clear") else ""
                    summary = f"Checadas: Enviadas {count} nueva(s){extra}"
                    ok = True

                    self._sis3_badge(True, phase="connected", msg="[SIS3] Envío OK. Conexión cerrada.", auto_reset_ms=1500)
                    self._ui(lambda: messagebox.showinfo("Listo", f"Checadas enviadas correctamente.\nNuevas: {count}"))

                else:
                    summary = f"Checadas: ERROR ({res.get('stage')})"
                    ok = False
                    self._sis3_badge(False, phase="disconnected", msg="[SIS3] Error enviando a SIS3.")
                    self._reloj_badge(False, phase="disconnected", msg="[SIS3] Error con el reloj o pipeline.")
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudieron enviar checadas.\nDetalle: {res.get('stage')}\n{res}"))

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

            self._sis3_badge(False, phase="disconnected", msg="[SIS3] Desconectado (error).")
            self._reloj_badge(False, phase="disconnected", msg="[SIS3] Reloj: desconectado (error).")

        finally:
            self._running = False
            try:
                self._lock.release()
            except Exception:
                pass
