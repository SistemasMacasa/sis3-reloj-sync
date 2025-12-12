# sis3_reloj/gui_tab_sis3.py
import tkinter as tk
from tkinter import ttk


def build_tab_sis3(parent, *, get_conn, get_config, log):
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    ttk.Label(frame, text="SIS3: pendiente (lo implementa tu compañero).").pack(anchor="w", pady=(5, 10))

    btn = ttk.Button(
        frame,
        text="Probar conexión (stub)",
        command=lambda: _test(get_conn, log),
    )
    btn.pack(anchor="w")

    return frame


def _test(get_conn, log):
    try:
        ip, port, password = get_conn()
        log(f"[SIS3] Stub OK. Conexión actual: {ip}:{port} / password={password}")
    except Exception as e:
        log(f"[SIS3] Stub error: {e}")
