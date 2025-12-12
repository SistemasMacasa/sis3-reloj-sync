# sis3_reloj/gui_tab_ajustes.py
import tkinter as tk
from tkinter import ttk

from .config import save_mode_sis2_disconnected


def build_tab_ajustes(parent, *, get_config, set_config_field, log):
    """
    get_config(): AppConfig
    set_config_field(name:str, value:any) -> actualiza en memoria (no solo en config.ini)
    log(msg:str)
    """
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    cfg = get_config()

    sis2_disc_var = tk.BooleanVar(value=bool(cfg.sis2_disconnected))

    chk = ttk.Checkbutton(
        frame,
        text="Se ha desconectado SIS2 (modo post-SIS2)",
        variable=sis2_disc_var,
        command=lambda: _on_toggle(sis2_disc_var, set_config_field, log),
    )
    chk.pack(anchor="w", pady=(8, 8))

    hint = ttk.Label(
        frame,
        text="Nota: Este modo solo evita el envío a SIS2. No borra nada del reloj.",
    )
    hint.pack(anchor="w")

    return frame


def _on_toggle(var, set_config_field, log):
    val = bool(var.get())
    save_mode_sis2_disconnected(val)
    set_config_field("sis2_disconnected", val)

    mode = "POST-SIS2 (podrá limpiar reloj en el futuro)" if val else "COEXISTENCIA con SIS2"
    log(f"[AJUSTES] Modo cambiado: {mode}")
