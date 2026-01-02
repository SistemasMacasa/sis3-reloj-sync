# sis3_reloj/gui_tab_sis2.py
import os
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime
import time
import threading

from .config import BASE_DIR
from .state_store import load_state, save_state, get_state_path
from .sis2_sink import (
    Sis2Config,
    send_attendance_to_sis2,
    send_probe_to_sis2_db,
    fetch_pending_personal_from_sis2_db,
    mark_personal_synced_in_sis2_db,
)
from .zk_client import (
    read_attendance,
    read_users,
    clear_attendance,
    upsert_user,
)

# ───────────────────────────────────────────────────────────────
# UX: Mensajes humanos
# ───────────────────────────────────────────────────────────────
def _human_reason(reason: str | None) -> str:
    reason = (reason or "").strip()
    mapping = {
        "no_new_records": "No hay checadas nuevas.",
        "no_pending_personal": "No hay cambios de empleados.",
        "sis2_disconnected": "Modo post-SIS2: no se envió a SIS2.",
        "test_mode_no_clear": "Prueba activada: NO se limpió el reloj.",
        "test_mode_no_mark": "Prueba activada: NO se marcó como sincronizado en SIS2.",
        "missing_db_password": "Falta contraseña DB.",
    }
    return mapping.get(reason, f"Sin cambios ({reason})" if reason else "Sin cambios.")


def _human_status(s: str) -> str:
    s = (s or "").strip().lower()
    if s == "idle":
        return "Listo"
    if s == "running":
        return "Procesando…"
    if s == "error":
        return "Error"
    return s or "Listo"


# ───────────────────────────────────────────────────────────────
# Tab builder
# ───────────────────────────────────────────────────────────────
def build_tab_sis2(
    parent,
    *,
    get_conn,
    get_config,
    log,
    register_probe=None,          # legacy
    is_sis2_connected=None,       # legacy
    bind_sis2_controls=None,      # app enlaza badge
    ui_set_sis2_badge=None,       # app.set_sis2_badge_state(ok, phase=..., msg=..., auto_reset_ms=...)
    ui_set_reloj_badge=None,      # app.set_reloj_badge_state(ok, phase=..., msg=..., auto_reset_ms=...)
    ui_clear_log=None,            # app.clear_log()
):
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    # Styles “tile”
    style = ttk.Style()
    try:
        style.configure("SIS2.Tile.View.TButton", padding=(14, 12))
        style.configure("SIS2.Tile.Send.TButton", padding=(14, 12))
        style.configure("SIS2.Tile.Primary.TButton", padding=(14, 12))

        style.configure("SIS2.Tile.View.TButton", foreground="#1d4ed8")
        style.configure("SIS2.Tile.Send.TButton", foreground="#047857")
    except Exception:
        pass

    # ─────────────────────────────────────────────
    # Card: Estado
    # ─────────────────────────────────────────────
    card = ttk.LabelFrame(frame, text="Estado", padding=(10, 10))
    card.pack(fill=tk.X, pady=(0, 10))

    lbl_badge = ttk.Label(
        card,
        text="SIS2: Desconectado",
        style="SIS2.Badge.Disconnected.TLabel",
        width=18,
        anchor="center",
    )
    lbl_badge.grid(row=0, column=0, sticky="w")

    btn_probe = ttk.Button(card, text="Probar conexión DB")
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

    if callable(bind_sis2_controls):
        bind_sis2_controls(lbl_badge, None)

    runner = _SIS2Runner(
        tk_parent=frame,
        get_conn=get_conn,
        get_config=get_config,
        log=log,
        ui_set_status=lambda s: lbl_status.config(text=_human_status(s)),
        ui_set_last=lambda s: lbl_last.config(text=s),
        ui_set_summary=lambda s: lbl_summary.config(text=s),
        ui_set_sis2_badge=ui_set_sis2_badge,
        ui_set_reloj_badge=ui_set_reloj_badge,
        ui_clear_log=ui_clear_log,
        is_test_mode=lambda: bool(test_var.get()),
    )

    btn_probe.configure(command=lambda: runner.run("probe_db"))

    if register_probe:
        register_probe(lambda: runner.probe_db_for_ui_sync_legacy())

    # ─────────────────────────────────────────────
    # Tiles
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
        style="SIS2.Tile.View.TButton",
        command=lambda: runner.run("read_users"),
    ).grid(row=0, column=0, sticky="ew", padx=(0, 6), pady=(0, 8))

    ttk.Button(
        tiles,
        text="Ver asistencias del reloj",
        width=BTN_W,
        style="SIS2.Tile.View.TButton",
        command=lambda: runner.run("read_attendance"),
    ).grid(row=0, column=1, sticky="ew", padx=(6, 0), pady=(0, 8))

    ttk.Button(
        tiles,
        text="Enviar registros nuevos de empleados",
        width=BTN_W,
        style="SIS2.Tile.Send.TButton",
        command=lambda: runner.run("sync_users"),
    ).grid(row=1, column=0, sticky="ew", padx=(0, 6), pady=(0, 10))

    ttk.Button(
        tiles,
        text="Enviar registros nuevos de asistencia",
        width=BTN_W,
        style="SIS2.Tile.Send.TButton",
        command=lambda: runner.run("attendance"),
    ).grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(0, 10))

    ttk.Button(
        frame,
        text="Sincronizar todo",
        style="SIS2.Tile.Primary.TButton",
        command=lambda: runner.run("full"),
    ).pack(fill=tk.X)

    return frame


# ───────────────────────────────────────────────────────────────
# Pipelines (empleados BD→reloj + asistencias incremental)
# ───────────────────────────────────────────────────────────────
def _build_sis2_cfg(cfg) -> Sis2Config:
    db_password = (os.getenv("SIS2_DB_PASSWORD") or "").strip() or str(getattr(cfg, "sis2_db_password", "") or "")

    return Sis2Config(
        enabled=bool(getattr(cfg, "sis2_enabled", False)),
        mode=str(getattr(cfg, "sis2_mode", "db")),
        drop_dir=(BASE_DIR / str(getattr(cfg, "sis2_drop_dir", "out"))).resolve(),
        base_url=str(getattr(cfg, "sis2_base_url", "") or ""),
        api_key=str(getattr(cfg, "sis2_api_key", "") or ""),
        timeout_sec=int(getattr(cfg, "sis2_timeout_sec", 10) or 10),
        db_server=str(getattr(cfg, "sis2_db_server", "") or ""),
        db_database=str(getattr(cfg, "sis2_db_database", "admin_macasa_prod") or "admin_macasa_prod"),
        db_username=str(getattr(cfg, "sis2_db_username", "") or ""),
        db_password=db_password,
    )


def _users_bd_to_device_pipeline(
    ip: str,
    port: int,
    password: int,
    cfg,
    log,
    *,
    ui_set_sis2_badge=None,
    runtime_mark_enabled: bool = True,
) -> dict:
    """
    Lógica legacy: SIS2(DB) -> Checador
      - Fuente: Tb_Personal (Estatus='A' y SincronizadoEnDispositivo=0)
      - Identidad: IdPersonal => device.user_id
      - Nombre: Nombre + ApellidoP + ApellidoM (ya viene armado desde sis2_sink)
      - PIN: ClaveChecador
      - Al finalizar OK: marcar SincronizadoEnDispositivo=1 (si runtime_mark_enabled=True)
    """
    sis2_cfg = _build_sis2_cfg(cfg)

    if (str(sis2_cfg.mode or "").strip().lower() != "db"):
        return {"ok": False, "stage": "users", "error": "users_mode_not_db"}

    if not sis2_cfg.db_password:
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(False, phase="disconnected", msg="[SIS2] Falta contraseña DB.")
        return {"ok": False, "stage": "users", "error": "missing_db_password"}

    if callable(ui_set_sis2_badge):
        ui_set_sis2_badge(None, phase="connecting", msg="[SIS2] Leyendo personal pendiente…")

    pending = fetch_pending_personal_from_sis2_db(
        sis2_cfg,
        limit=500,
        log=lambda m: log(f"[SIS2] {m}"),
    )

    if not pending:
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(True, phase="connected", msg="[SIS2] Personal: sin cambios.", auto_reset_ms=1200)
        return {"ok": True, "skipped": True, "reason": "no_pending_personal"}

    applied = 0
    failed = 0
    marked = 0

    for p in pending:
        try:
            idp = int(p.get("IdPersonal"))
        except Exception:
            failed += 1
            continue

        user_id = str(idp)
        name = str(p.get("full_name") or p.get("FullName") or p.get("nombre_completo") or "").strip()
        pin = str(p.get("ClaveChecador") or "").strip()

        try:
            privilege = int(p.get("Privilegio") or 0)
        except Exception:
            privilege = 0

        try:
            card = int(p.get("NumeroTarjeta") or 0)
        except Exception:
            card = 0

        try:
            ok_dev = upsert_user(
                ip, port, password,
                user_id=user_id,
                name=name,
                privilege=privilege,
                user_password=pin,
                card=card,
                enabled=True,
            )
            if not ok_dev:
                failed += 1
                continue

            applied += 1

            if runtime_mark_enabled:
                ok_mark = mark_personal_synced_in_sis2_db(
                    sis2_cfg,
                    idp,
                    log=lambda m: log(f"[SIS2] {m}"),
                )
                if ok_mark:
                    marked += 1
                else:
                    log(f"[SIS2] ⚠️ No se pudo marcar SincronizadoEnDispositivo=1 para IdPersonal={idp}")
            else:
                # Prueba: no marcar en BD
                pass

        except Exception as e:
            failed += 1
            log(f"[SIS2] ⚠️ Upsert a reloj falló IdPersonal={idp}: {e}")

    ok_all = (failed == 0)
    if callable(ui_set_sis2_badge):
        ui_set_sis2_badge(True if ok_all else False,
                          phase="connected" if ok_all else "disconnected",
                          msg="[SIS2] Personal: proceso terminado.",
                          auto_reset_ms=1200)

    out = {
        "ok": ok_all,
        "applied": applied,
        "failed": failed,
        "marked": marked,
        "count": len(pending),
        "skipped": False,
    }
    if not runtime_mark_enabled:
        out["test_mode"] = True
        out["reason"] = "test_mode_no_mark"

    return out


def _attendance_incremental_pipeline(
    ip: str,
    port: int,
    password: int,
    cfg,
    log,
    *,
    ui_set_sis2_badge=None,
    runtime_clear_enabled: bool = True,
) -> dict:
    log(f"[SIS2] Conectando a {ip}:{port} ...")

    try:
        all_records = read_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ❌ Error al leer asistencia: {e}")
        return {"ok": False, "stage": "read_attendance", "error": str(e)}

    log(f"[SIS2] Se obtuvieron {len(all_records)} registros de asistencia (crudo).")

    # checkpoint global SIS2
    state_path = get_state_path("sis2")
    state = load_state(state_path)

    if not state_path.exists():
        save_state(state_path, state)
        log(f"[SIS2] State creado: {state_path}")
    else:
        log(f"[SIS2] State: {state_path}")

    log(f"[SIS2] Checkpoint actual: {state.last_ok_ts.isoformat() if state.last_ok_ts else '(vacío)'}")

    if state.last_ok_ts:
        before = len(all_records)
        records = [
            r for r in all_records
            if isinstance(getattr(r, "timestamp", None), datetime) and r.timestamp > state.last_ok_ts
        ]
        log(f"[SIS2] Incremental activo. Filtrados {before - len(records)}. Nuevos: {len(records)}")
    else:
        records = all_records
        log("[SIS2] Incremental: checkpoint vacío. Se procesan todos.")

    if len(records) == 0:
        log("[SIS2] No hay registros nuevos. Nada que enviar.")
        return {"ok": True, "skipped": True, "reason": "no_new_records"}

    # post-SIS2 (no enviamos a DB)
    if bool(getattr(cfg, "sis2_disconnected", False)):
        log("[SIS2] Modo post-SIS2 activo: NO se envía a SIS2.")
        return {"ok": True, "skipped": True, "reason": "sis2_disconnected", "count": len(records)}

    sis2_cfg = _build_sis2_cfg(cfg)

    if (str(sis2_cfg.mode or "").strip().lower() == "db") and (not sis2_cfg.db_password):
        log("[SIS2] ❌ Falta contraseña DB. Define SIS2_DB_PASSWORD o [sis2_db] password en config.ini.")
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(False, phase="disconnected", msg="[SIS2] Falta contraseña DB.")
        return {"ok": False, "stage": "sink", "error": "missing_db_password"}

    try:
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(None, phase="connecting", msg="[SIS2] Conectando a BD…")

        sink_result = send_attendance_to_sis2(records, sis2_cfg, log=lambda m: log(f"[SIS2] {m}"))
    except Exception as e:
        log(f"[SIS2] ❌ Error enviando a SIS2: {e}")
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(False, phase="disconnected", msg="[SIS2] Desconectado (falló envío).")
        return {"ok": False, "stage": "sis2_sink", "error": str(e)}

    if not (sink_result and sink_result.get("ok") is True):
        log("[SIS2] ❌ SIS2 no confirmó OK. No se limpia ni se actualiza checkpoint.")
        if callable(ui_set_sis2_badge):
            ui_set_sis2_badge(False, phase="disconnected", msg="[SIS2] Desconectado (sin confirmación).")
        return {"ok": False, "stage": "sis2_sink", "sink": sink_result}

    if callable(ui_set_sis2_badge):
        ui_set_sis2_badge(True, phase="connected", msg="[SIS2] Envío OK. Conexión cerrada.", auto_reset_ms=1500)

    # Test mode: NO limpiar, pero sí avanzar checkpoint (patrón nuevo)
    max_ts = max((r.timestamp for r in records if isinstance(getattr(r, "timestamp", None), datetime)), default=None)
    if not runtime_clear_enabled:
        log("[SIS2] Prueba activada: NO se limpió el reloj. ✅ Se actualiza checkpoint.")
        if max_ts:
            state.last_ok_ts = max_ts
            save_state(state_path, state)
            log(f"[SIS2] Checkpoint actualizado (sin limpiar): last_ok_ts={max_ts.isoformat()}")

        return {"ok": True, "count": len(records), "sink": sink_result, "skipped": True, "reason": "test_mode_no_clear"}

    # limpiar reloj
    try:
        log("[SIS2] OK confirmado → limpiando registros de asistencia en el dispositivo...")
        ok_clear = clear_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ⚠️ Error limpiando dispositivo: {e}")
        return {"ok": False, "stage": "clear", "error": str(e), "sink": sink_result}

    if not ok_clear:
        log("[SIS2] ⚠️ Limpieza no confirmada (retorno False). No se actualiza checkpoint.")
        return {"ok": False, "stage": "clear", "error": "clear_attendance returned False", "sink": sink_result}

    log("[SIS2] ✅ Dispositivo limpiado correctamente.")

    if max_ts:
        state.last_ok_ts = max_ts
        save_state(state_path, state)
        log(f"[SIS2] Checkpoint actualizado: last_ok_ts={max_ts.isoformat()}")

    return {"ok": True, "count": len(records), "sink": sink_result, "cleared": True}


# ───────────────────────────────────────────────────────────────
# Runner
# ───────────────────────────────────────────────────────────────
class _SIS2Runner:
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
        ui_set_sis2_badge=None,
        ui_set_reloj_badge=None,
        ui_clear_log=None,
        is_test_mode=None,
    ):
        self.tk_parent = tk_parent
        self.get_conn = get_conn
        self.get_config = get_config
        self.log = log

        self.ui_set_status = ui_set_status
        self.ui_set_last = ui_set_last
        self.ui_set_summary = ui_set_summary

        self.ui_set_sis2_badge = ui_set_sis2_badge
        self.ui_set_reloj_badge = ui_set_reloj_badge
        self.ui_clear_log = ui_clear_log
        self.is_test_mode = is_test_mode or (lambda: False)

        self._lock = threading.Lock()
        self._running = False

    def _ui(self, fn):
        self.tk_parent.after(0, fn)

    def _badge(self, ok, *, phase=None, msg=None, auto_reset_ms=None):
        if not callable(self.ui_set_sis2_badge):
            if msg:
                self.log(msg)
            return
        self._ui(lambda: self.ui_set_sis2_badge(ok, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms))

    def _reloj_badge(self, ok, *, phase=None, msg=None, auto_reset_ms=None):
        if not callable(self.ui_set_reloj_badge):
            return
        self._ui(lambda: self.ui_set_reloj_badge(ok, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms))

    def _clear_log(self):
        if callable(self.ui_clear_log):
            self._ui(lambda: self.ui_clear_log())

    def run(self, action: str):
        if self._running:
            self.log("[SIS2] Pipeline ya está corriendo; ignorando solicitud duplicada.")
            return
        threading.Thread(target=self._run_guarded, args=(action,), daemon=True).start()

    def probe_db_for_ui_sync_legacy(self):
        ok, human = self._probe_db_internal()
        return ok, human

    def _probe_db_internal(self) -> tuple[bool, str]:
        try:
            cfg = self.get_config()
            sis2_cfg = _build_sis2_cfg(cfg)

            if not sis2_cfg.db_password:
                self._badge(False, phase="disconnected", msg="[SIS2] Falta contraseña DB.")
                return False, "Falta contraseña DB. Define SIS2_DB_PASSWORD o [sis2_db] password en config.ini."

            self._badge(None, phase="connecting", msg="[SIS2] Probando conexión a BD…")
            res = send_probe_to_sis2_db(sis2_cfg, log=lambda m: self.log(f"[SIS2] {m}"))
            ok = bool(res.get("ok"))

            if ok:
                self._badge(True, phase="connected", msg="[SIS2] DB OK. Conexión cerrada.", auto_reset_ms=1500)
                return True, "Conexión OK a la BD de SIS2."
            else:
                self._badge(False, phase="disconnected", msg="[SIS2] BD no accesible.")
                return False, "No se pudo conectar a la BD de SIS2."

        except Exception as e:
            self._badge(False, phase="disconnected", msg=f"[SIS2] Error en probe: {e}")
            return False, f"No se pudo conectar a la BD de SIS2: {e}"

    def _run_reloj_op(self, label: str, fn, *, ok_reset_ms: int = 900):
        self._reloj_badge(None, phase="connecting", msg=f"[SIS2] Reloj: {label}…")
        try:
            res = fn()
            ok = True
            if isinstance(res, dict) and ("ok" in res):
                ok = bool(res.get("ok"))
            if ok:
                self._reloj_badge(True, phase="connected", msg="[SIS2] Reloj OK. Conexión cerrada.", auto_reset_ms=ok_reset_ms)
            else:
                self._reloj_badge(False, phase="disconnected", msg="[SIS2] Reloj: operación no confirmada.")
            return res
        except Exception as e:
            self._reloj_badge(False, phase="disconnected", msg=f"[SIS2] Reloj: error: {e}")
            raise

    def _run_guarded(self, action: str):
        if not self._lock.acquire(blocking=False):
            self.log("[SIS2] Lock ocupado; pipeline ya está en ejecución.")
            return

        self._running = True
        started_dt = datetime.now()

        self._clear_log()
        self._ui(lambda: self.ui_set_status("running"))
        self._ui(lambda: self.ui_set_summary("Procesando… por favor espera."))
        self.log(f"[SIS2] START action={action} @ {started_dt:%Y-%m-%d %H:%M:%S}")

        try:
            if action == "probe_db":
                ok, human = self._probe_db_internal()
                if ok:
                    self._ui(lambda: messagebox.showinfo("SIS2", human))
                else:
                    self._ui(lambda: messagebox.showerror("SIS2", human))

                ended_dt = datetime.now()
                self._ui(lambda: self.ui_set_last(f"{ended_dt:%Y-%m-%d %H:%M:%S}"))
                self._ui(lambda: self.ui_set_summary(f"BD: {'OK' if ok else 'ERROR'}"))
                self._ui(lambda: self.ui_set_status("Idle" if ok else "Error"))
                return

            # Conexión al reloj
            try:
                ip, port, password = self.get_conn()
            except ValueError:
                self._ui(lambda: messagebox.showerror("Error", "Port y Password deben ser numéricos."))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            cfg = self.get_config()

            if action == "read_users":
                def _op():
                    self.log(f"[SIS2] Conectando a {ip}:{port} para leer usuarios...")
                    return read_users(ip, port, password)

                users = self._run_reloj_op("leyendo usuarios", _op, ok_reset_ms=800)

                def _clean(s: str) -> str:
                    s = s or ""
                    return "".join(ch if (ch.isprintable() and ch not in "\r\n\t") else "?" for ch in s).strip()

                total = len(users)
                self.log(f"[SIS2] Usuarios leídos: {total}")
                self.log("[SIS2] Listado completo:")
                for u in users:
                    uid = (u.get("user_id") if isinstance(u, dict) else getattr(u, "user_id", None)) or "?"
                    name = (u.get("name") if isinstance(u, dict) else getattr(u, "name", None)) or "(sin nombre)"
                    card = (u.get("card") if isinstance(u, dict) else getattr(u, "card", None)) or ""
                    self.log(f"[SIS2]   - {_clean(str(uid))} | {_clean(str(name))} | {_clean(str(card))}")

                summary = f"Empleados: {total} encontrados (ver Log)"
                ok = True
                self._ui(lambda: messagebox.showinfo("Listo", f"Empleados encontrados en el reloj: {total}\n(Consulta el Log para el detalle)"))

            elif action == "read_attendance":
                def _op():
                    self.log(f"[SIS2] Conectando a {ip}:{port} para leer asistencia...")
                    return read_attendance(ip, port, password)

                records = self._run_reloj_op("leyendo asistencias", _op, ok_reset_ms=800)

                total = len(records)
                self.log(f"[SIS2] Asistencias leídas: {total}")

                sample = records[:25]
                self.log("[SIS2] Muestra (hasta 25):")
                for r in sample:
                    self.log(f"[SIS2]   - user_id={getattr(r,'user_id','?')} ts={getattr(r,'timestamp',None)} punch={getattr(r,'punch',None)} status={getattr(r,'status',None)}")

                summary = f"Asistencias: {total} encontradas (muestra=25 en Log)"
                ok = True
                self._ui(lambda: messagebox.showinfo("Listo", f"Asistencias encontradas en el reloj: {total}\n(Consulta el Log para una muestra)"))

            elif action == "sync_users":
                # Esta operación toca reloj (upsert_user) y DB (leer pendientes + marcar synced)
                def _op():
                    return _users_bd_to_device_pipeline(
                        ip, port, password, cfg, self.log,
                        ui_set_sis2_badge=lambda ok_, phase=None, msg=None, auto_reset_ms=None: self._badge(
                            ok_, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms
                        ),
                        runtime_mark_enabled=(not bool(self.is_test_mode())),
                    )

                res = self._run_reloj_op("sincronizando personal (BD→Reloj)", _op, ok_reset_ms=1100)

                if res.get("ok") and res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Empleados: {human}"
                    ok = True
                    self._ui(lambda: messagebox.showinfo("Listo", human))
                elif res.get("ok"):
                    applied = res.get("applied", 0)
                    failed = res.get("failed", 0)
                    marked = res.get("marked", 0)

                    extra = ""
                    if res.get("test_mode"):
                        extra = "\n\n" + _human_reason("test_mode_no_mark")

                    summary = f"Empleados: {applied} aplicado(s), {failed} error(es), {marked} marcados en SIS2"
                    ok = True
                    self._ui(lambda: messagebox.showinfo(
                        "Listo",
                        f"Personal sincronizado.\nAplicados: {applied}\nErrores: {failed}\nMarcados: {marked}{extra}"
                    ))
                else:
                    err = res.get("error") or res.get("stage") or "users"
                    human = _human_reason(res.get("error")) if res.get("error") in ("missing_db_password",) else str(err)
                    summary = f"Empleados: ERROR ({human})"
                    ok = False
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudo sincronizar personal.\nDetalle: {res}"))

            elif action == "attendance":
                def _op():
                    return _attendance_incremental_pipeline(
                        ip, port, password, cfg, self.log,
                        ui_set_sis2_badge=lambda ok_, phase=None, msg=None, auto_reset_ms=None: self._badge(
                            ok_, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms
                        ),
                        runtime_clear_enabled=(not bool(self.is_test_mode())),
                    )

                res = self._run_reloj_op("enviando asistencias", _op, ok_reset_ms=1000)

                if res.get("ok") and res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Checadas: {human}"
                    ok = True
                    self._ui(lambda: messagebox.showinfo("Listo", human))
                elif res.get("ok"):
                    count = res.get("count", 0)
                    summary = f"Checadas: Enviadas {count} nueva(s)"
                    extra = "\n\nSe limpió el reloj." if res.get("cleared") else ""
                    ok = True
                    self._ui(lambda: messagebox.showinfo("Listo", f"Checadas enviadas correctamente.\nNuevas: {count}{extra}"))
                else:
                    summary = f"Checadas: ERROR ({res.get('stage')})"
                    ok = False
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudieron enviar checadas.\nDetalle: {res.get('stage')}\n{res}"))

            elif action == "full":
                def _op():
                    started = time.time()
                    self.log(f"[SIS2] Iniciando proceso completo en {ip}:{port} ...")

                    users_result = _users_bd_to_device_pipeline(
                        ip, port, password, cfg, self.log,
                        ui_set_sis2_badge=lambda ok_, phase=None, msg=None, auto_reset_ms=None: self._badge(
                            ok_, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms
                        ),
                        runtime_mark_enabled=(not bool(self.is_test_mode())),
                    )

                    attendance_result = _attendance_incremental_pipeline(
                        ip, port, password, cfg, self.log,
                        ui_set_sis2_badge=lambda ok_, phase=None, msg=None, auto_reset_ms=None: self._badge(
                            ok_, phase=phase, msg=msg, auto_reset_ms=auto_reset_ms
                        ),
                        runtime_clear_enabled=(not bool(self.is_test_mode())),
                    )

                    elapsed = time.time() - started
                    ok_all = bool(users_result.get("ok")) and bool(attendance_result.get("ok"))
                    summary_all = f"Todo: listo en {elapsed:.1f}s"

                    # Empleados msg
                    if users_result.get("skipped"):
                        empleados_msg = _human_reason(users_result.get("reason"))
                    elif users_result.get("ok"):
                        empleados_msg = (
                            f"Empleados: {users_result.get('applied', 0)} aplicados | "
                            f"{users_result.get('failed', 0)} errores | "
                            f"{users_result.get('marked', 0)} marcados"
                        )
                        if users_result.get("test_mode"):
                            empleados_msg += " | Prueba: NO marcó en SIS2"
                    else:
                        empleados_msg = f"Empleados: ERROR ({users_result.get('error') or 'pipeline'})"

                    # Checadas msg
                    if attendance_result.get("skipped"):
                        checadas_msg = _human_reason(attendance_result.get("reason"))
                    else:
                        checadas_msg = f"Checadas enviadas: {attendance_result.get('count', 0)}"
                        checadas_msg += " | Reloj limpiado" if attendance_result.get("cleared") else " | Reloj NO limpiado"

                    msg = "\n".join([
                        "Sincronización completa finalizada.",
                        f"Tiempo: {elapsed:.1f}s",
                        "",
                        empleados_msg,
                        checadas_msg
                    ])
                    self.log("[SIS2] " + msg.replace("\n", " | "))

                    return {"ok": ok_all, "summary": summary_all, "msg": msg}

                res = self._run_reloj_op("sincronización completa", _op, ok_reset_ms=1200)

                ok = bool(res.get("ok"))
                summary = str(res.get("summary") or "Todo: finalizado")
                msg = str(res.get("msg") or "Sincronización completa finalizada.")

                if ok:
                    self._ui(lambda: messagebox.showinfo("Listo", msg))
                else:
                    self._ui(lambda: messagebox.showerror("Error", msg))

            else:
                self.log(f"[SIS2] Acción desconocida: {action}")
                self._ui(lambda: messagebox.showerror("Error", f"Acción desconocida: {action}"))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            ended_dt = datetime.now()
            self._ui(lambda: self.ui_set_last(f"{ended_dt:%Y-%m-%d %H:%M:%S}"))
            self._ui(lambda: self.ui_set_summary(summary))
            self._ui(lambda: self.ui_set_status("Idle" if ok else "Error"))

        except Exception as ex:
            self.log(f"[SIS2] ERROR action={action} → {ex!r}")
            self._ui(lambda: self.ui_set_status("Error"))
            self._ui(lambda: self.ui_set_summary(f"ERROR: {ex!r}"))
            self._ui(lambda: messagebox.showerror("Error", f"Fallo inesperado:\n{ex!r}"))

        finally:
            self._running = False
            try:
                self._lock.release()
            except Exception:
                pass
