# sis3_reloj/gui.py
import tkinter as tk
import threading
from tkinter import ttk

from .config import load_config
from .gui_tab_sis2 import build_tab_sis2
from .gui_tab_sis3 import build_tab_sis3
from .gui_tab_ajustes import build_tab_ajustes


class SIS3RelojApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SIS3RelojChecador")
        self.geometry("740x520")

        self.config_obj = load_config()

        # Vars globales (header)
        self.ip_var = tk.StringVar(value=self.config_obj.ip)
        self.port_var = tk.StringVar(value=str(self.config_obj.port))
        self.pass_var = tk.StringVar(value=str(self.config_obj.password))

        # Estado de conexión SIS2 (checkbox header)
        self.sis2_conn_var = tk.BooleanVar(value=False)
        self.sis2_probe_fn = None  # se registra desde la pestaña SIS2

        # refs notebook/tabs
        self.nb = None
        self.tab_sis2 = None
        self.tab_sis3 = None
        self.tab_ajustes = None

        # widgets header / log
        self.lbl_sis2_state = None
        self.chk_sis2 = None
        self.txt_log = None

        self._build_ui()
        self.log("[APP] Aplicación iniciada.")

        # aplicar modo según config (si SIS2 está “post-SIS2”, ocultar tab)
        self.apply_sis2_mode_from_config()

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill=tk.BOTH, expand=True)

        # ✅ Estilos ttk para estado SIS2
        style = ttk.Style(self)
        style.configure("SIS2.Connected.TLabel", foreground="#15803d")     # verde
        style.configure("SIS2.Disconnected.TLabel", foreground="#b91c1c")  # rojo
        style.configure("SIS2.Connecting.TLabel", foreground="#b45309")    # ámbar

        # Header: conexión (global)
        header = ttk.Frame(root)
        header.pack(fill=tk.X)

        ttk.Label(header, text="IP:").grid(row=0, column=0, sticky="w")
        ttk.Entry(header, textvariable=self.ip_var, width=18).grid(row=0, column=1, sticky="w", padx=(5, 14))

        ttk.Label(header, text="Port:").grid(row=0, column=2, sticky="w")
        ttk.Entry(header, textvariable=self.port_var, width=8).grid(row=0, column=3, sticky="w", padx=(5, 14))

        ttk.Label(header, text="Password:").grid(row=0, column=4, sticky="w")
        ttk.Entry(header, textvariable=self.pass_var, width=8).grid(row=0, column=5, sticky="w", padx=(5, 0))

        # Estado + checkbox SIS2
        self.lbl_sis2_state = ttk.Label(
            header,
            text="Desconectado",
            style="SIS2.Disconnected.TLabel"
        )

        self.lbl_sis2_state.grid(row=0, column=6, sticky="w", padx=(12, 6))
        self.chk_sis2 = ttk.Checkbutton(
            header,
            text="SIS2 Conectado",
            variable=self.sis2_conn_var,
            command=self._on_toggle_sis2
        )


        self.chk_sis2.grid(row=0, column=7, sticky="w", padx=(6, 0))
        header.columnconfigure(8, weight=1)

        # Notebook: SIS2 / SIS3 / Ajustes
        self.nb = ttk.Notebook(root)
        self.nb.pack(fill=tk.BOTH, expand=False, pady=(10, 10))

        self.tab_sis2 = ttk.Frame(self.nb)
        self.tab_sis3 = ttk.Frame(self.nb)
        self.tab_ajustes = ttk.Frame(self.nb)

        self.nb.add(self.tab_sis2, text="SIS2")
        self.nb.add(self.tab_sis3, text="SIS3")
        self.nb.add(self.tab_ajustes, text="Ajustes")

        # Log global
        ttk.Label(root, text="Log:").pack(anchor="w")
        self.txt_log = tk.Text(root, height=14)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

        # Construir tabs (delegación)
        build_tab_sis2(
            self.tab_sis2,
            get_conn=self.get_connection,
            get_config=lambda: self.config_obj,
            log=self.log,
            register_probe=lambda fn: setattr(self, "sis2_probe_fn", fn),
            set_header_state=self.set_sis2_header_state,
            # runtime connected REAL: checkbox ON y NO post-SIS2
            is_sis2_connected=lambda: bool(self.sis2_conn_var.get()) and (not bool(self.config_obj.sis2_disconnected)),
        )

        build_tab_sis3(
            self.tab_sis3,
            get_conn=self.get_connection,
            get_config=lambda: self.config_obj,
            log=self.log,
        )

        build_tab_ajustes(
            self.tab_ajustes,
            get_config=lambda: self.config_obj,
            set_config_field=self.set_config_field,
            log=self.log,
            on_toggle_sis2_disconnected=self.on_toggle_sis2_disconnected,
        )

    # -------------------------
    # Helpers compartidos
    # -------------------------
    def get_connection(self):
        ip = self.ip_var.get().strip()
        port = int(self.port_var.get())
        pass_raw = (self.pass_var.get() or "").strip()
        password = int(pass_raw) if pass_raw != "" else 0
        return ip, port, password

    def set_config_field(self, name: str, value):
        setattr(self.config_obj, name, value)

    def log(self, msg: str):
        if self.txt_log is None:
            return
        self.txt_log.insert(tk.END, msg + "\n")
        self.txt_log.see(tk.END)

    # -------------------------
    # SIS2: UI state + hide/show
    # -------------------------
    def set_sis2_header_state(self, ok: bool, msg: str | None = None, phase: str | None = None):
        """
        Estado visible (label) = qué está pasando.
        Checkbox = permiso/acción (permitir o no el envío).
        """
        if phase == "connecting":
            self.lbl_sis2_state.config(text="Conectando…", style="SIS2.Connecting.TLabel")
        elif ok:
            self.lbl_sis2_state.config(text="Conectado", style="SIS2.Connected.TLabel")
        else:
            self.lbl_sis2_state.config(text="Desconectado", style="SIS2.Disconnected.TLabel")


        if msg:
            self.log(msg)


    def _on_toggle_sis2(self):
        # si SIS2 está “post-SIS2”, no permitir conectar
        if bool(getattr(self.config_obj, "sis2_disconnected", False)):
            self.sis2_conn_var.set(False)
            self.set_sis2_header_state(False, "[SIS2] Desactivado por modo post-SIS2 (Ajustes).")
            return

        # si lo apagan
        if not self.sis2_conn_var.get():
            self.set_sis2_header_state(False, "[SIS2] Estado → Desconectado (por usuario)")
            return

        # si lo prenden: UI inmediato
        self.set_sis2_header_state(True, "[SIS2] Estado → Conectando (probe DB)…", phase="connecting")

        # bloquear el toggle mientras prueba (evita clicks nerviosos)
        try:
            self.chk_sis2.state(["disabled"])
        except Exception:
            pass

        def worker():
            try:
                if not callable(self.sis2_probe_fn):
                    raise RuntimeError("SIS2 probe no disponible")

                ok, human = self.sis2_probe_fn()

                def apply_ok():
                    self.set_sis2_header_state(ok, human)
                    if not ok:
                        self.sis2_conn_var.set(False)
                    try:
                        self.chk_sis2.state(["!disabled"])
                    except Exception:
                        pass

                self.after(0, apply_ok)

            except Exception as e:
                def apply_err():
                    self.set_sis2_header_state(False, f"[SIS2] ERROR al conectar: {e}")
                    self.sis2_conn_var.set(False)
                    try:
                        self.chk_sis2.state(["!disabled"])
                    except Exception:
                        pass

                self.after(0, apply_err)

        threading.Thread(target=worker, daemon=True).start()

    def apply_sis2_mode_from_config(self):
        """
        Si sis2_disconnected=True:
          - Oculta pestaña SIS2
          - Desactiva checkbox header
          - Fuerza estado desconectado
        Si sis2_disconnected=False:
          - Muestra pestaña SIS2
          - Habilita checkbox header
        """
        post = bool(getattr(self.config_obj, "sis2_disconnected", False))

        if post:
            # Ocultar pestaña SIS2
            try:
                self.nb.hide(self.tab_sis2)
            except Exception:
                pass

            # Deshabilitar header toggle y forzar desconectado
            try:
                self.chk_sis2.state(["disabled"])
            except Exception:
                pass

            self.sis2_conn_var.set(False)
            self.set_sis2_header_state(False, "[SIS2] Modo post-SIS2 activo: pestaña SIS2 deshabilitada.")
        else:
            # Mostrar pestaña SIS2 sin duplicar
            try:
                tabs = self.nb.tabs()
                sis2_id = str(self.tab_sis2)
                if sis2_id not in tabs:
                    self.nb.insert(0, self.tab_sis2, text="SIS2")
            except Exception:
                pass

            try:
                self.chk_sis2.state(["!disabled"])
            except Exception:
                pass

    def on_toggle_sis2_disconnected(self, value: bool):
        """
        Callback desde Ajustes cuando el usuario cambia el modo post-SIS2.
        """
        self.set_config_field("sis2_disconnected", bool(value))

        # Si se activa post-SIS2, forzar OFF el checkbox en caliente
        if bool(value):
            self.sis2_conn_var.set(False)

        self.apply_sis2_mode_from_config()


def run_app():
    app = SIS3RelojApp()
    app.mainloop()


if __name__ == "__main__":
    run_app()
