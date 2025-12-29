# sis3_reloj/gui_tab_ajustes.py
import tkinter as tk
from tkinter import ttk

from .config import save_mode_sis2_disconnected


def build_tab_ajustes(parent, *, get_config, set_config_field, log, on_toggle_sis2_disconnected=None):
    """
    get_config(): AppConfig
    set_config_field(name:str, value:any) -> actualiza en memoria (no solo en config.ini)
    log(msg:str)
    on_toggle_sis2_disconnected(value: bool) -> callback para que gui.py oculte/active la pestaña SIS2
    """
    frame = ttk.Frame(parent, padding=10)
    frame.pack(fill=tk.BOTH, expand=True)

    cfg = get_config()
    sis2_disc_var = tk.BooleanVar(value=bool(getattr(cfg, "sis2_disconnected", False)))

    # Panel principal (look consistente)
    card = ttk.LabelFrame(frame, text="Ajustes", padding=(12, 10))
    card.pack(fill=tk.X, pady=(0, 10))

    # Título/explicación cortita y entendible
    title = ttk.Label(
        card,
        text="Cuando se active este ajuste, el reloj trabajará solo con SIS3.",
        wraplength=680,
        justify="left",
    )
    title.pack(anchor="w", pady=(0, 8))

    # Checkbox centrado (texto simple)
    row = ttk.Frame(card)
    row.pack(fill=tk.X)

    chk = ttk.Checkbutton(
        row,
        text="Activar modo solo SIS3",
        variable=sis2_disc_var,
        command=lambda: _on_toggle(sis2_disc_var, set_config_field, log, on_toggle_sis2_disconnected),
    )
    chk.pack(anchor="center", pady=(6, 10))

    # Explicación tipo “tarjeta de ayuda” (sin palabras raras)
    help_box = ttk.LabelFrame(card, text="¿Qué cambia?", padding=(10, 8))
    help_box.pack(fill=tk.X, pady=(0, 8))

    hint = ttk.Label(
        help_box,
        text=(
            "• Se desactiva SIS2: se oculta la pestaña SIS2 y se bloquea el switch “SIS2 Conectado”.\n"
            "• A partir de aquí, las checadas se envían a SIS3.\n"
            "• SIS3 ya podrá limpiar el reloj cuando termine correctamente.\n"
            "\n"
            "Tip: Actívalo solo cuando ya estés listo para dejar de usar SIS2."
        ),
        justify="left",
        wraplength=680,
    )
    hint.pack(anchor="w")
    return frame


def _on_toggle(var, set_config_field, log, on_toggle_sis2_disconnected=None):
    val = bool(var.get())

    # Persistencia en config.ini
    save_mode_sis2_disconnected(val)

    # Estado en memoria (para que GUI reaccione inmediatamente)
    set_config_field("sis2_disconnected", val)

    # Mensaje al log en español “normal”
    if val:
        log("[AJUSTES] Modo solo SIS3 ACTIVADO: SIS2 se deshabilita y SIS3 podrá limpiar el reloj al finalizar OK.")
    else:
        log("[AJUSTES] Modo solo SIS3 DESACTIVADO: SIS2 vuelve a estar disponible.")

    # Callback hacia gui.py para aplicar hide/show tab + bloquear header checkbox
    if callable(on_toggle_sis2_disconnected):
        on_toggle_sis2_disconnected(val)
