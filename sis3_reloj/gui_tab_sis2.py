# sis3_reloj/gui_tab_sis2.py
import os
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, date
import re
import types
from pathlib import Path
import json
import time
import threading

from .sis2_sink import Sis2Config, send_attendance_to_sis2, send_probe_to_sis2_db
from .zk_client import (
    read_attendance,
    read_users,
    clear_attendance,
    upsert_user,
    delete_user,
)
from .file_sink import write_attendance_jsonl, write_users_jsonl
from .config import BASE_DIR
from .state_store import load_state, save_state


# ───────────────────────────────────────────────────────────────
# UX: Mensajes humanos (sin tecnicismos)
# ───────────────────────────────────────────────────────────────

def _human_reason(reason: str | None) -> str:
    reason = (reason or "").strip()

    mapping = {
        "no_new_records": "No hay checadas nuevas.",
        "no_pending_file": "No hay cambios de empleados.",
        "empty_pending_file": "No hay cambios de empleados.",
        "header_disconnected": "Modo simulación: se guardó local (no se envió a SIS2).",
        "sis2_disconnected": "Modo post-SIS2: se guardó local (no se envió a SIS2).",
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


def build_tab_sis2(parent, *, get_conn, get_config, log,
                  register_probe=None, set_header_state=None,
                  is_sis2_connected=None):
    """
    parent: frame del tab
    get_conn(): (ip:str, port:int, password:int)  -> conexión al RELOJ (ZK)
    get_config(): AppConfig                      -> config.ini (incluye sis2_db)
    log(msg:str)
    register_probe(fn)                           -> registra fn que regresa (ok,msg) para checkbox header
    set_header_state(ok,msg)                     -> opcional, no se usa aquí (lo maneja gui.py)
    """
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    # ─────────────────────────────────────────────
    # Status bar (visible para operación)
    # ─────────────────────────────────────────────
    status_wrap = ttk.Frame(frame)
    status_wrap.pack(fill=tk.X, pady=(0, 10))

    lbl_status_title = ttk.Label(status_wrap, text="Estado:")
    lbl_status_title.grid(row=0, column=0, sticky="w")

    lbl_status = ttk.Label(status_wrap, text="Listo")
    lbl_status.grid(row=0, column=1, sticky="w", padx=(6, 20))

    lbl_last_title = ttk.Label(status_wrap, text="Última ejecución:")
    lbl_last_title.grid(row=0, column=2, sticky="w")

    lbl_last = ttk.Label(status_wrap, text="—")
    lbl_last.grid(row=0, column=3, sticky="w", padx=(6, 20))

    lbl_summary_title = ttk.Label(status_wrap, text="Resultado:")
    lbl_summary_title.grid(row=1, column=0, sticky="w", pady=(6, 0))

    lbl_summary = ttk.Label(status_wrap, text="—")
    lbl_summary.grid(row=1, column=1, columnspan=3, sticky="w", padx=(6, 0), pady=(6, 0))

    status_wrap.columnconfigure(3, weight=1)

    # ─────────────────────────────────────────────
    # Runner async con lock (anti doble-click / anti freeze)
    # ─────────────────────────────────────────────
    runner = _SIS2Runner(
        tk_parent=frame,
        get_conn=get_conn,
        get_config=get_config,
        log=log,
        ui_set_status=lambda s: lbl_status.config(text=_human_status(s)),
        ui_set_last=lambda s: lbl_last.config(text=s),
        ui_set_summary=lambda s: lbl_summary.config(text=s),
        is_sis2_connected=is_sis2_connected,
    )

    # Hook para checkbox del header
    def _probe_for_header():
        return runner.probe_db_for_header()

    if register_probe:
        register_probe(_probe_for_header)

    # ─────────────────────────────────────────────
    # Botones (UX)
    # ─────────────────────────────────────────────
    # Contenedor para alinear botones a la izquierda
    actions = ttk.Frame(frame)
    actions.pack(fill=tk.X, pady=(5, 10), anchor="w")

    BTN_W = 36  # ancho en "text units" (ajustable)

    btn_full = ttk.Button(
        actions,
        text="Sincronizar todo (empleados y checadas)",
        width=BTN_W,
        command=lambda: runner.run("full"),
    )
    btn_full.pack(anchor="w", pady=(0, 8))

    btn_sync_users = ttk.Button(
        actions,
        text="Actualizar empleados (altas y cambios)",
        width=BTN_W,
        command=lambda: runner.run("sync_users"),
    )
    btn_sync_users.pack(anchor="w", pady=(0, 8))

    btn_read = ttk.Button(
        actions,
        text="Enviar checadas nuevas",
        width=BTN_W,
        command=lambda: runner.run("attendance"),
    )
    btn_read.pack(anchor="w", pady=(0, 8))

    btn_users = ttk.Button(
        actions,
        text="Ver empleados del reloj (solo consulta)",
        width=BTN_W,
        command=lambda: runner.run("read_users"),
    )
    btn_users.pack(anchor="w", pady=(0, 0))

    # ─────────────────────────────────────────────
    # Recovery (desde archivos asistencia-*.jsonl)
    # ─────────────────────────────────────────────
    reco = ttk.LabelFrame(frame, text="Recovery (desde archivos asistencia-*)")
    reco.pack(fill=tk.X, pady=(12, 0), anchor="w")

    row1 = ttk.Frame(reco)
    row1.pack(fill=tk.X, padx=10, pady=(10, 6))

    runner.var_recovery = tk.BooleanVar(value=False)
    ttk.Checkbutton(row1, text="Activar recovery", variable=runner.var_recovery).pack(side=tk.LEFT)

    ttk.Label(row1, text="Desde (YYYY-MM-DD):").pack(side=tk.LEFT, padx=(16, 6))
    runner.ent_rec_from = ttk.Entry(row1, width=12)
    runner.ent_rec_from.insert(0, "2025-12-11")
    runner.ent_rec_from.pack(side=tk.LEFT)

    ttk.Label(row1, text="Hasta (YYYY-MM-DD):").pack(side=tk.LEFT, padx=(12, 6))
    runner.ent_rec_to = ttk.Entry(row1, width=12)
    runner.ent_rec_to.insert(0, "2025-12-15")
    runner.ent_rec_to.pack(side=tk.LEFT)

    row2 = ttk.Frame(reco)
    row2.pack(fill=tk.X, padx=10, pady=(0, 10))

    ttk.Button(
        row2,
        text="Previsualizar (dry-run)",
        command=lambda: runner.run("recovery_preview"),
    ).pack(side=tk.LEFT, padx=(0, 6))

    ttk.Button(
        row2,
        text="Reprocesar selección",
        command=lambda: runner.run("recovery_send"),
    ).pack(side=tk.LEFT, padx=(0, 6))

    runner.lbl_recovery = ttk.Label(row2, text="Estado: listo")
    runner.lbl_recovery.pack(side=tk.LEFT, padx=(12, 0))

    return frame



# ───────────────────────────────────────────────────────────────
# Utilidades: pendientes de usuarios
# ───────────────────────────────────────────────────────────────

def _pending_users_dir(cfg) -> Path:
    return (BASE_DIR / cfg.output_dir / "sis2" / "pending").resolve()


def _find_latest_pending_users_file(cfg) -> Path | None:
    pend_dir = _pending_users_dir(cfg)
    if not pend_dir.exists():
        return None
    files = sorted(pend_dir.glob("users-pending-*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None


def _parse_jsonl(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _archive_processed_file(path: Path) -> Path:
    processed_dir = path.parent / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)
    new_path = processed_dir / path.name
    try:
        path.replace(new_path)
    except Exception:
        new_path.write_bytes(path.read_bytes())
        path.unlink(missing_ok=True)
    return new_path

# ───────────────────────────────────────────────────────────────
# Recovery: leer desde archivos asistencia-*.jsonl (out/sis2)
# ───────────────────────────────────────────────────────────────

_ASIST_FILE_RE = re.compile(
    r"^(?:asistencia|asis-sis2)-(\d{8})-(\d{6})\.(jsonl|json)$",
    re.IGNORECASE
)

def _parse_ymd(s: str) -> date:
    return datetime.strptime((s or "").strip(), "%Y-%m-%d").date()

def _date_from_yyyymmdd(s: str) -> date:
    return datetime.strptime(s, "%Y%m%d").date()

def _asistencia_dir(cfg) -> Path:
    # Preferimos el estándar: BASE_DIR/out/sis2 (por tus capturas)
    cand1 = (BASE_DIR / "out" / "sis2").resolve()
    if cand1.exists():
        return cand1

    # Fallback: BASE_DIR/<output_dir>/sis2 (si config.ini lo define distinto)
    return (BASE_DIR / cfg.output_dir / "sis2").resolve()


def _scan_asistencia_files(cfg) -> list[tuple[date, Path]]:
    folder = _asistencia_dir(cfg)
    if not folder.exists():
        return []
    out: list[tuple[date, Path]] = []
    for p in folder.iterdir():
        if not p.is_file():
            continue
        m = _ASIST_FILE_RE.match(p.name)
        if not m:
            continue
        yyyymmdd = m.group(1)
        out.append((_date_from_yyyymmdd(yyyymmdd), p))
    out.sort(key=lambda x: (x[0], x[1].name))
    return out

def _filter_asistencia_by_range(files: list[tuple[date, Path]], d_from: date, d_to: date) -> list[tuple[date, Path]]:
    return [(d, p) for (d, p) in files if d_from <= d <= d_to]

def _safe_parse_ts(v):
    # soporta datetime, "2025-12-16 08:58:00", ISO, etc.
    if isinstance(v, datetime):
        return v
    s = str(v or "").strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            pass
    try:
        # último intento: fromisoformat
        return datetime.fromisoformat(s)
    except Exception:
        return None

def _load_attendance_records_from_jsonl(path: Path) -> list:
    rows = _parse_jsonl(path)  # ya existe en tu archivo
    records = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        rr = dict(r)
        # normalizar timestamp
        ts = rr.get("timestamp") or rr.get("ts") or rr.get("datetime") or rr.get("time")
        ts_dt = _safe_parse_ts(ts)
        rr["timestamp"] = ts_dt
        # Convertimos a objeto con atributos (para ser compatible con sink que usa r.timestamp)
        records.append(types.SimpleNamespace(**rr))
    return records

def _approx_events_in_file(path: Path) -> int:
    try:
        return len(_parse_jsonl(path))
    except Exception:
        return 0

# ───────────────────────────────────────────────────────────────
# Flujo: Sincronización de usuarios
# ───────────────────────────────────────────────────────────────

def _sync_users_from_pending_file(ip: str, port: int, password: int, cfg, log) -> dict:
    pending_file = _find_latest_pending_users_file(cfg)
    if not pending_file:
        log("[SIS2] No se encontró archivo de pendientes en out/sis2/pending (users-pending-*.jsonl).")
        return {"ok": True, "skipped": True, "reason": "no_pending_file"}

    log(f"[SIS2] Pendientes de usuarios: {pending_file}")
    try:
        rows = _parse_jsonl(pending_file)
        if len(rows) == 0:
            log(f"[SIS2] Pendientes vacío: {pending_file}. No se procesa ni se archiva.")
            return {"ok": True, "skipped": True, "reason": "empty_pending_file", "path": str(pending_file)}
    except Exception as e:
        log(f"[SIS2] ❌ Error leyendo pendientes: {e}")
        return {"ok": False, "error": f"parse_pending: {e}"}

    applied = 0
    failed = 0

    for r in rows:
        action = str(r.get("action", "A")).strip().upper()
        user_id = str(r.get("user_id", "")).strip()
        if not user_id:
            failed += 1
            continue

        try:
            if action == "C":
                ok = delete_user(ip, port, password, user_id=user_id)
            else:
                ok = upsert_user(
                    ip, port, password,
                    user_id=user_id,
                    name=str(r.get("name", "") or ""),
                    privilege=int(r.get("privilege", 0) or 0),
                    user_password=str(r.get("password", "") or ""),
                    card=r.get("card", 0),
                    enabled=bool(r.get("enabled", True)),
                )

            if ok:
                applied += 1
            else:
                failed += 1

        except Exception as e:
            failed += 1
            log(f"[SIS2] ⚠️ Usuario {user_id} acción {action} falló: {e}")

    archived_to = _archive_processed_file(pending_file)
    log(f"[SIS2] Pendientes procesados. applied={applied}, failed={failed}. Archivo archivado en: {archived_to}")
    return {"ok": failed == 0, "applied": applied, "failed": failed, "archived": str(archived_to)}


# ───────────────────────────────────────────────────────────────
# Flujo: Asistencia incremental + sink + clear + checkpoint
# ───────────────────────────────────────────────────────────────

def _attendance_incremental_pipeline(ip: str, port: int, password: int, cfg, log, *, runtime_connected: bool = True) -> dict:
    log(f"[SIS2] Conectando a {ip}:{port} ...")

    try:
        all_records = read_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ❌ Error al leer asistencia: {e}")
        return {"ok": False, "stage": "read_attendance", "error": str(e)}

    log(f"[SIS2] Se obtuvieron {len(all_records)} registros de asistencia (crudo).")

    output_dir = (BASE_DIR / cfg.output_dir).resolve()
    state_path = (output_dir / "sis2" / "state.json")
    state = load_state(state_path)

    if not state_path.exists():
        save_state(state_path, state)
        log(f"[SIS2] State creado: {state_path}")

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
        log("[SIS2] No hay registros nuevos. Nada que enviar ni limpiar.")
        return {"ok": True, "skipped": True, "reason": "no_new_records"}

    path_local = write_attendance_jsonl(records, output_dir)
    log(f"[SIS2] Archivo guardado en: {path_local}")

    # 1) Runtime (header): si está “desconectado”, NO enviar a SIS2 y NO limpiar reloj
    if not runtime_connected:
        log("[SIS2] Header=DESCONectado → DRY-RUN: se guarda archivo local, no se envía a DB y no se limpia el reloj.")
        return {"ok": True, "skipped": True, "reason": "header_disconnected", "local_path": str(path_local)}

    # 2) Ajustes (post-SIS2): también NO enviar y NO limpiar
    if cfg.sis2_disconnected:
        log("[SIS2] SIS2 desconectado (Ajustes / post-SIS2). No se envía a SIS2 y NO se limpia el dispositivo.")
        return {"ok": True, "skipped": True, "reason": "sis2_disconnected", "local_path": str(path_local)}

    # Política de password DB: ENV tiene prioridad, si no existe usa config.ini
    db_password = (os.getenv("SIS2_DB_PASSWORD") or "").strip() or str(getattr(cfg, "sis2_db_password", "") or "")
    src = "ENV(SIS2_DB_PASSWORD)" if (os.getenv("SIS2_DB_PASSWORD") or "").strip() else "config.ini [sis2_db]"

    sink_result = None
    try:
        sis2_cfg = Sis2Config(
            enabled=bool(cfg.sis2_enabled),
            mode=str(cfg.sis2_mode),
            drop_dir=(BASE_DIR / str(cfg.sis2_drop_dir)).resolve(),
            base_url=str(cfg.sis2_base_url),
            api_key=str(cfg.sis2_api_key),
            timeout_sec=int(cfg.sis2_timeout_sec),

            db_server=str(getattr(cfg, "sis2_db_server", "") or ""),
            db_database=str(getattr(cfg, "sis2_db_database", "admin_macasa_prod") or "admin_macasa_prod"),
            db_username=str(getattr(cfg, "sis2_db_username", "") or ""),
            db_password=db_password,
            db_driver=str(getattr(cfg, "sis2_db_driver", "ODBC Driver 18 for SQL Server") or "ODBC Driver 18 for SQL Server"),
            db_trust_server_certificate=bool(getattr(cfg, "sis2_db_trust_server_certificate", True)),
        )

        if not sis2_cfg.db_password:
            log("[SIS2] ❌ Falta contraseña DB. Define SIS2_DB_PASSWORD o [sis2_db] password en config.ini.")
            return {"ok": False, "stage": "sink", "error": "missing_db_password", "local_path": str(path_local)}

        log(f"[SIS2] Sink usando credenciales desde {src}.")
        sink_result = send_attendance_to_sis2(records, sis2_cfg, log=lambda m: log(f"[SIS2] {m}"))
    except Exception as e:
        log(f"[SIS2] SIS2 sink error: {e}")
        sink_result = {"ok": False, "error": str(e)}

    if not (sink_result and sink_result.get("ok") is True):
        log("[SIS2] No se limpia el dispositivo (sink no confirmado o falló).")
        return {
            "ok": False,
            "stage": "sink",
            "sink": sink_result,
            "local_path": str(path_local),
        }

    # Clear
    try:
        log("[SIS2] Sink OK. Limpiando registros de asistencia en el dispositivo...")
        ok_clear = clear_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ⚠️ Error limpiando dispositivo: {e}")
        return {"ok": False, "stage": "clear", "error": str(e), "sink": sink_result}

    if not ok_clear:
        log("[SIS2] ⚠️ Limpieza no confirmada (retorno False). No se actualiza checkpoint.")
        return {"ok": False, "stage": "clear", "error": "clear_attendance returned False", "sink": sink_result}

    log("[SIS2] ✅ Dispositivo limpiado correctamente.")

    max_ts = max((r.timestamp for r in records if isinstance(getattr(r, "timestamp", None), datetime)), default=None)
    if max_ts:
        state.last_ok_ts = max_ts
        save_state(state_path, state)
        log(f"[SIS2] Checkpoint actualizado: last_ok_ts={max_ts.isoformat()}")

    return {"ok": True, "count": len(records), "local_path": str(path_local), "sink": sink_result}


# ───────────────────────────────────────────────────────────────
# Runner (async)
# ───────────────────────────────────────────────────────────────

class _SIS2Runner:
    """
    Ejecuta acciones SIS2 en thread para no congelar Tkinter.
    Anti concurrencia: un solo job a la vez.
    """
    def __init__(self, *, tk_parent, get_conn, get_config, log,
                 ui_set_status, ui_set_last, ui_set_summary,
                 is_sis2_connected=None):
        self.tk_parent = tk_parent
        self.get_conn = get_conn
        self.get_config = get_config
        self.log = log

        self.ui_set_status = ui_set_status
        self.ui_set_last = ui_set_last
        self.ui_set_summary = ui_set_summary

        self.is_sis2_connected = is_sis2_connected

        self._lock = threading.Lock()
        self._running = False

    def run(self, action: str):
        if self._running:
            self.log("[SIS2] Pipeline ya está corriendo; ignorando solicitud duplicada.")
            return
        t = threading.Thread(target=self._run_guarded, args=(action,), daemon=True)
        t.start()

    def _ui(self, fn):
        self.tk_parent.after(0, fn)

    def _runtime_connected(self) -> bool:
        """
        Estado runtime real basado en el checkbox del header (y/o reglas que mande gui.py).
        Si no hay callback, asumimos True para no romper.
        """
        if callable(self.is_sis2_connected):
            try:
                return bool(self.is_sis2_connected())
            except Exception:
                return False
        return True

    def probe_db_for_header(self):
        """
        Probe para checkbox del header.
        Regresa (ok:bool, msg:str). No abre messagebox.
        """
        try:
            cfg = self.get_config()

            db_password = (os.getenv("SIS2_DB_PASSWORD") or "").strip() or str(getattr(cfg, "sis2_db_password", "") or "")
            src = "ENV(SIS2_DB_PASSWORD)" if (os.getenv("SIS2_DB_PASSWORD") or "").strip() else "config.ini [sis2_db]"

            sis2_cfg = Sis2Config(
                enabled=bool(cfg.sis2_enabled),
                mode=str(cfg.sis2_mode),
                drop_dir=(BASE_DIR / str(cfg.sis2_drop_dir)).resolve(),
                base_url=str(cfg.sis2_base_url),
                api_key=str(cfg.sis2_api_key),
                timeout_sec=int(cfg.sis2_timeout_sec),

                db_server=str(getattr(cfg, "sis2_db_server", "") or ""),
                db_database=str(getattr(cfg, "sis2_db_database", "admin_macasa_prod") or "admin_macasa_prod"),
                db_username=str(getattr(cfg, "sis2_db_username", "") or ""),
                db_password=db_password,
                db_driver=str(getattr(cfg, "sis2_db_driver", "ODBC Driver 18 for SQL Server") or "ODBC Driver 18 for SQL Server"),
                db_trust_server_certificate=bool(getattr(cfg, "sis2_db_trust_server_certificate", True)),
            )

            if not sis2_cfg.db_password:
                return False, "[SIS2] Falta contraseña DB. Define SIS2_DB_PASSWORD o [sis2_db] password en config.ini."

            self.log(f"[SIS2] Probe DB (header) usando credenciales desde {src}…")
            res = send_probe_to_sis2_db(sis2_cfg, log=lambda m: self.log(f"[SIS2] {m}"))
            ok = bool(res.get("ok"))

            if ok:
                return True, "[SIS2] Conectado (DB OK)."
            return False, "[SIS2] Desconectado (DB no accesible)."

        except Exception as e:
            return False, f"[SIS2] Desconectado (error): {e}"

    def _run_guarded(self, action: str):
        # 1) Lock para evitar doble ejecución
        if not self._lock.acquire(blocking=False):
            self.log("[SIS2] Lock ocupado; pipeline ya está en ejecución.")
            return

        self._running = True
        started_dt = datetime.now()

        self._ui(lambda: self.ui_set_status("Running"))
        self._ui(lambda: self.ui_set_summary("—"))

        self.log(f"[SIS2] START action={action} @ {started_dt:%Y-%m-%d %H:%M:%S}")

        try:
            # 2) Leer conexión al reloj
            try:
                ip, port, password = self.get_conn()
            except ValueError:
                self._ui(lambda: messagebox.showerror("Error", "Port y Password deben ser numéricos."))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            cfg = self.get_config()

            # 3) Ejecutar acción
            if action == "sync_users":
                res = _sync_users_from_pending_file(ip, port, password, cfg, self.log)
                ok = bool(res.get("ok"))
                applied = res.get("applied", 0)
                failed = res.get("failed", 0)

                if res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Empleados: {human}"
                    self._ui(lambda: messagebox.showinfo("Listo", human))
                    ok = True
                else:
                    summary = f"Empleados: {applied} actualizado(s), {failed} con error"
                    if ok:
                        self._ui(lambda: messagebox.showinfo(
                            "Listo",
                            f"Empleados actualizados.\nActualizados: {applied}\nErrores: {failed}"
                        ))
                    else:
                        self._ui(lambda: messagebox.showerror(
                            "Error",
                            f"No se pudieron actualizar algunos empleados.\n{summary}"
                        ))

            elif action == "attendance":
                res = _attendance_incremental_pipeline(
                    ip, port, password, cfg, self.log,
                    runtime_connected=self._runtime_connected()
                )

                if res.get("ok") and res.get("skipped"):
                    human = _human_reason(res.get("reason"))
                    summary = f"Checadas: {human}"
                    self._ui(lambda: messagebox.showinfo("Listo", human))
                    ok = True
                elif res.get("ok"):
                    count = res.get("count")
                    summary = f"Checadas: Enviadas {count} nueva(s)"
                    self._ui(lambda: messagebox.showinfo(
                        "Listo",
                        f"Checadas enviadas correctamente.\nNuevas: {count}"
                    ))
                    ok = True
                else:
                    summary = f"Checadas: ERROR ({res.get('stage')})"
                    self._ui(lambda: messagebox.showerror(
                        "Error",
                        f"No se pudieron enviar checadas.\nDetalle: {res.get('stage')}\n{res}"
                    ))
                    ok = False

            elif action == "read_users":
                self.log(f"[SIS2] Conectando a {ip}:{port} para leer usuarios...")
                try:
                    users = read_users(ip, port, password)
                except Exception as e:
                    self.log(f"[SIS2] ❌ Error al leer usuarios: {e}")
                    self._ui(lambda: messagebox.showerror("Error", f"No se pudo consultar empleados:\n{e}"))
                    self._ui(lambda: self.ui_set_status("Error"))
                    return

                def _clean(s: str) -> str:
                    s = (s or "")
                    return "".join(ch if (ch.isprintable() and ch not in "\r\n\t") else "?" for ch in s).strip()

                total = len(users)
                self.log(f"[SIS2] Usuarios leídos: {total}")
                self.log("[SIS2] Listado completo:")

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
                self._ui(lambda: messagebox.showinfo(
                    "Listo",
                    f"Empleados encontrados en el reloj: {total}\n(Consulta el Log para el detalle)"
                ))
                ok = True

            elif action == "recovery_preview":
                # DRY-RUN: solo analiza archivos asistencia-*.jsonl
                try:
                    if not getattr(self, "var_recovery", None) or not self.var_recovery.get():
                        summary = "Recovery: desactivado"
                        self._ui(lambda: messagebox.showinfo(
                            "Listo",
                            "Activa el checkbox 'Activar recovery' para usar esta función."
                        ))
                        ok = True
                    else:
                        d_from = _parse_ymd(self.ent_rec_from.get())
                        d_to = _parse_ymd(self.ent_rec_to.get())
                        if d_to < d_from:
                            raise ValueError("Rango inválido: 'Hasta' es menor que 'Desde'.")

                        files = _scan_asistencia_files(cfg)
                        selected = _filter_asistencia_by_range(files, d_from, d_to)
                        folder = _asistencia_dir(cfg)

                        if not selected:
                            self.log(f"[SIS2][RECOVERY] No hay archivos asistencia-* entre {d_from} y {d_to} en: {folder}")
                            summary = "Recovery: sin archivos"
                            if getattr(self, "lbl_recovery", None):
                                self._ui(lambda: self.lbl_recovery.config(text="Estado: sin archivos"))
                            self._ui(lambda: messagebox.showinfo(
                                "Listo",
                                "No encontré archivos para ese rango.\nRevisa fechas y carpeta out/sis2."
                            ))
                            ok = True
                        else:
                            self._recovery_selected = selected

                            self.log("=================================================")
                            self.log(f"[SIS2][RECOVERY] DRY-RUN en: {folder}")
                            self.log(f"[SIS2][RECOVERY] Rango: {d_from} → {d_to}")

                            agg: dict[date, dict] = {}
                            total_files = 0
                            total_events = 0
                            for d, p in selected:
                                total_files += 1
                                c = _approx_events_in_file(p)
                                total_events += c
                                if d not in agg:
                                    agg[d] = {"files": 0, "events": 0}
                                agg[d]["files"] += 1
                                agg[d]["events"] += c

                            for d in sorted(agg.keys()):
                                self.log(f"[SIS2][RECOVERY] {d.isoformat()} → archivos={agg[d]['files']} eventos≈{agg[d]['events']}")

                            self.log(f"[SIS2][RECOVERY] TOTAL → archivos={total_files} eventos≈{total_events}")
                            self.log("=================================================")

                            summary = f"Recovery: listo ({total_files} archivos)"
                            if getattr(self, "lbl_recovery", None):
                                self._ui(lambda: self.lbl_recovery.config(text=f"Estado: listo ({total_files} archivos)"))
                            self._ui(lambda: messagebox.showinfo(
                                "Listo",
                                f"Previsualización lista.\nArchivos: {total_files}\nEventos aprox.: {total_events}\nRevisa el Log."
                            ))
                            ok = True

                except Exception as e:
                    self.log(f"[SIS2][RECOVERY] ERROR preview → {e!r}")
                    summary = "Recovery: error"
                    if getattr(self, "lbl_recovery", None):
                        self._ui(lambda: self.lbl_recovery.config(text="Estado: error"))
                    self._ui(lambda: messagebox.showerror("Error", f"Falló la previsualización:\n{e!r}"))
                    ok = False

            elif action == "recovery_send":
                # ENVÍO: reinyecta archivos seleccionados usando el MISMO sink de DB
                try:
                    if not getattr(self, "var_recovery", None) or not self.var_recovery.get():
                        summary = "Recovery: desactivado"
                        self._ui(lambda: messagebox.showinfo(
                            "Listo",
                            "Activa el checkbox 'Activar recovery' para usar esta función."
                        ))
                        ok = True
                    else:
                        selected = getattr(self, "_recovery_selected", None)
                        if not selected:
                            summary = "Recovery: sin selección"
                            self._ui(lambda: messagebox.showinfo(
                                "Listo",
                                "Primero ejecuta 'Previsualizar (dry-run)' para preparar la selección."
                            ))
                            ok = True
                        else:
                            # Respeta la misma política de conexión que attendance:
                            if not self._runtime_connected():
                                self.log("[SIS2][RECOVERY] Header=DESCONectado → no se envía (solo dry-run permitido).")
                                summary = "Recovery: header desconectado"
                                self._ui(lambda: messagebox.showinfo(
                                    "Listo",
                                    "Header está DESCONectado.\nActívalo para permitir envío."
                                ))
                                ok = True
                            elif cfg.sis2_disconnected:
                                self.log("[SIS2][RECOVERY] SIS2 desconectado (Ajustes). No se envía.")
                                summary = "Recovery: sis2_disconnected"
                                self._ui(lambda: messagebox.showinfo(
                                    "Listo",
                                    "SIS2 está marcado como desconectado en Ajustes.\nDesactívalo para permitir envío."
                                ))
                                ok = True
                            else:
                                db_password = (os.getenv("SIS2_DB_PASSWORD") or "").strip() or str(getattr(cfg, "sis2_db_password", "") or "")
                                src = "ENV(SIS2_DB_PASSWORD)" if (os.getenv("SIS2_DB_PASSWORD") or "").strip() else "config.ini [sis2_db]"

                                sis2_cfg = Sis2Config(
                                    enabled=bool(cfg.sis2_enabled),
                                    mode=str(cfg.sis2_mode),
                                    drop_dir=(BASE_DIR / str(cfg.sis2_drop_dir)).resolve(),
                                    base_url=str(cfg.sis2_base_url),
                                    api_key=str(cfg.sis2_api_key),
                                    timeout_sec=int(cfg.sis2_timeout_sec),

                                    db_server=str(getattr(cfg, "sis2_db_server", "") or ""),
                                    db_database=str(getattr(cfg, "sis2_db_database", "admin_macasa_prod") or "admin_macasa_prod"),
                                    db_username=str(getattr(cfg, "sis2_db_username", "") or ""),
                                    db_password=db_password,
                                    db_driver=str(getattr(cfg, "sis2_db_driver", "ODBC Driver 18 for SQL Server") or "ODBC Driver 18 for SQL Server"),
                                    db_trust_server_certificate=bool(getattr(cfg, "sis2_db_trust_server_certificate", True)),
                                )

                                if not sis2_cfg.db_password:
                                    raise RuntimeError("Falta contraseña DB. Define SIS2_DB_PASSWORD o [sis2_db] password en config.ini.")

                                self.log(f"[SIS2][RECOVERY] Sink usando credenciales desde {src}.")
                                self.log("=================================================")
                                self.log("[SIS2][RECOVERY] INICIO envío (replay controlado desde archivos)")

                                sent_files = 0
                                sent_rows = 0
                                last_err = None

                                for d, p in selected:
                                    records = _load_attendance_records_from_jsonl(p)
                                    good = [r for r in records if isinstance(getattr(r, "timestamp", None), datetime)]
                                    if not good:
                                        self.log(f"[SIS2][RECOVERY] SKIP {p.name} (0 filas con timestamp válido)")
                                        continue

                                    res_sink = send_attendance_to_sis2(good, sis2_cfg, log=lambda m: self.log(f"[SIS2] {m}"))
                                    if not (res_sink and res_sink.get("ok") is True):
                                        last_err = res_sink
                                        self.log(f"[SIS2][RECOVERY] ❌ FAIL file={p.name} sink={res_sink}")
                                        break

                                    sent_files += 1
                                    sent_rows += len(good)
                                    self.log(f"[SIS2][RECOVERY] OK {d.isoformat()} file={p.name} filas={len(good)}")

                                self.log(f"[SIS2][RECOVERY] FIN envío → archivos_ok={sent_files} filas_enviadas={sent_rows}")
                                self.log("=================================================")

                                if last_err:
                                    summary = "Recovery: error en envío"
                                    if getattr(self, "lbl_recovery", None):
                                        self._ui(lambda: self.lbl_recovery.config(text="Estado: error"))
                                    self._ui(lambda: messagebox.showerror("Error", f"Falló el envío recovery.\nDetalle:\n{last_err}"))
                                    ok = False
                                else:
                                    summary = f"Recovery: enviado ({sent_files} archivos)"
                                    if getattr(self, "lbl_recovery", None):
                                        self._ui(lambda: self.lbl_recovery.config(text=f"Estado: enviado ({sent_files} archivos)"))
                                    self._ui(lambda: messagebox.showinfo(
                                        "Listo",
                                        f"Recovery enviado.\nArchivos OK: {sent_files}\nFilas enviadas: {sent_rows}\nRevisa el Log."
                                    ))
                                    ok = True

                except Exception as e:
                    self.log(f"[SIS2][RECOVERY] ERROR send → {e!r}")
                    summary = "Recovery: error"
                    if getattr(self, "lbl_recovery", None):
                        self._ui(lambda: self.lbl_recovery.config(text="Estado: error"))
                    self._ui(lambda: messagebox.showerror("Error", f"Falló el envío recovery:\n{e!r}"))
                    ok = False

            elif action == "full":
                started = time.time()
                self.log(f"[SIS2] Iniciando proceso completo en {ip}:{port} ...")

                users_result = _sync_users_from_pending_file(ip, port, password, cfg, self.log)
                attendance_result = _attendance_incremental_pipeline(
                    ip, port, password, cfg, self.log,
                    runtime_connected=self._runtime_connected()
                )
                elapsed = time.time() - started

                ok = bool(users_result.get("ok")) and bool(attendance_result.get("ok"))
                summary = f"Todo: listo en {elapsed:.1f}s"

                empleados_msg = (
                    _human_reason(users_result.get("reason")) if users_result.get("skipped")
                    else f"Empleados actualizados: {users_result.get('applied', 0)} | Errores: {users_result.get('failed', 0)}"
                )
                checadas_msg = (
                    _human_reason(attendance_result.get("reason")) if attendance_result.get("skipped")
                    else f"Checadas enviadas: {attendance_result.get('count', 0)}"
                )

                msg = "\n".join([
                    "Sincronización completa finalizada.",
                    f"Tiempo: {elapsed:.1f}s",
                    "",
                    empleados_msg,
                    checadas_msg,
                ])

                self.log("[SIS2] " + msg.replace("\n", " | "))

                if ok:
                    self._ui(lambda: messagebox.showinfo("Listo", msg))
                else:
                    self._ui(lambda: messagebox.showerror("Error", msg))

            else:
                self.log(f"[SIS2] Acción desconocida: {action}")
                self._ui(lambda: messagebox.showerror("Error", f"Acción desconocida: {action}"))
                self._ui(lambda: self.ui_set_status("Error"))
                return

            # 4) UI final (status/last/summary)
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
