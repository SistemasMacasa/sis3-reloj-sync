# sis3_reloj/gui_tab_sis2.py
import tkinter as tk
from tkinter import ttk, messagebox

from .sis2_sink import Sis2Config, send_attendance_to_sis2
from .zk_client import read_attendance, read_users
from .file_sink import write_attendance_jsonl, write_users_jsonl
from .config import BASE_DIR


def build_tab_sis2(parent, *, get_conn, get_config, log):
    """
    parent: frame del tab
    get_conn(): (ip:str, port:int, password:int)
    get_config(): AppConfig
    log(msg:str)
    """
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    # Botones
    btn_read = ttk.Button(
        frame,
        text="Leer asistencia y guardar archivo",
        command=lambda: _on_read_and_save(get_conn, get_config, log),
    )
    btn_read.pack(pady=(5, 10))

    btn_users = ttk.Button(
        frame,
        text="Leer usuarios y guardar archivo",
        command=lambda: _on_read_users_and_save(get_conn, get_config, log),
    )
    btn_users.pack(pady=(0, 10))

    return frame


def _on_read_and_save(get_conn, get_config, log):
    try:
        ip, port, password = get_conn()
    except ValueError:
        messagebox.showerror("Error", "Port y Password deben ser numéricos.")
        return

    cfg = get_config()
    log(f"[SIS2] Conectando a {ip}:{port} ...")

    try:
        records = read_attendance(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ❌ Error al leer asistencia: {e}")
        messagebox.showerror("Error", f"No se pudo leer asistencia:\n{e}")
        return

    log(f"[SIS2] Se obtuvieron {len(records)} registros de asistencia.")

    output_dir = BASE_DIR / cfg.output_dir
    path = write_attendance_jsonl(records, output_dir)
    log(f"[SIS2] Archivo guardado en: {path}")

    # Hook SIS2 sink (solo si NO está desconectado)
    if not cfg.sis2_disconnected:
        try:
            sis2_cfg = Sis2Config(
                enabled=bool(cfg.sis2_enabled),
                mode=str(cfg.sis2_mode),
                drop_dir=(BASE_DIR / str(cfg.sis2_drop_dir)).resolve(),
                base_url=str(cfg.sis2_base_url),
                api_key=str(cfg.sis2_api_key),
                timeout_sec=int(cfg.sis2_timeout_sec),
            )
            send_attendance_to_sis2(records, sis2_cfg, log=lambda m: log(f"[SIS2] {m}"))
        except Exception as e:
            log(f"[SIS2] SIS2 sink error: {e}")
    else:
        log("[SIS2] SIS2 desconectado (Ajustes). No se envía a SIS2.")

    messagebox.showinfo("OK", f"Se guardaron {len(records)} registros en:\n{path}")


def _on_read_users_and_save(get_conn, get_config, log):
    try:
        ip, port, password = get_conn()
    except ValueError:
        messagebox.showerror("Error", "Port y Password deben ser numéricos.")
        return

    cfg = get_config()
    log(f"[SIS2] Conectando a {ip}:{port} para leer usuarios...")

    try:
        users = read_users(ip, port, password)
    except Exception as e:
        log(f"[SIS2] ❌ Error al leer usuarios: {e}")
        messagebox.showerror("Error", f"No se pudo leer usuarios:\n{e}")
        return

    log(f"[SIS2] Se obtuvieron {len(users)} usuarios del dispositivo.")

    output_dir = BASE_DIR / cfg.output_dir
    path = write_users_jsonl(users, output_dir)
    log(f"[SIS2] Archivo de usuarios guardado en: {path}")

    messagebox.showinfo("OK", f"Se guardaron {len(users)} usuarios en:\n{path}")
