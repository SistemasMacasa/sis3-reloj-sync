# sis3_reloj/gui.py
import tkinter as tk
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

        self._build_ui()
        self.log("[APP] Aplicación iniciada.")

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill=tk.BOTH, expand=True)

        # -------------------------
        # Header: conexión (global)
        # -------------------------
        header = ttk.Frame(root)
        header.pack(fill=tk.X)

        ttk.Label(header, text="IP:").grid(row=0, column=0, sticky="w")
        ttk.Entry(header, textvariable=self.ip_var, width=18).grid(row=0, column=1, sticky="w", padx=(5, 14))

        ttk.Label(header, text="Port:").grid(row=0, column=2, sticky="w")
        ttk.Entry(header, textvariable=self.port_var, width=8).grid(row=0, column=3, sticky="w", padx=(5, 14))

        ttk.Label(header, text="Password:").grid(row=0, column=4, sticky="w")
        ttk.Entry(header, textvariable=self.pass_var, width=8).grid(row=0, column=5, sticky="w", padx=(5, 0))

        # -------------------------
        # Notebook: SIS2 / SIS3 / Ajustes
        # -------------------------
        nb = ttk.Notebook(root)
        nb.pack(fill=tk.BOTH, expand=False, pady=(10, 10))

        tab_sis2 = ttk.Frame(nb)
        tab_sis3 = ttk.Frame(nb)
        tab_ajustes = ttk.Frame(nb)

        nb.add(tab_sis2, text="SIS2")
        nb.add(tab_sis3, text="SIS3")
        nb.add(tab_ajustes, text="Ajustes")

        # -------------------------
        # Log global
        # -------------------------
        ttk.Label(root, text="Log:").pack(anchor="w")
        self.txt_log = tk.Text(root, height=14)
        self.txt_log.pack(fill=tk.BOTH, expand=True)

        # Construir tabs (delegación)
        build_tab_sis2(
            tab_sis2,
            get_conn=self.get_connection,
            get_config=lambda: self.config_obj,
            log=self.log,
        )

        build_tab_sis3(
            tab_sis3,
            get_conn=self.get_connection,
            get_config=lambda: self.config_obj,
            log=self.log,
        )

        build_tab_ajustes(
            tab_ajustes,
            get_config=lambda: self.config_obj,
            set_config_field=self.set_config_field,
            log=self.log,
        )

    # -------------------------
    # Helpers compartidos
    # -------------------------
    def get_connection(self):
        ip = self.ip_var.get().strip()
        port = int(self.port_var.get())
        password = int(self.pass_var.get())
        return ip, port, password

    def set_config_field(self, name: str, value):
        setattr(self.config_obj, name, value)

    def log(self, msg: str):
        self.txt_log.insert(tk.END, msg + "\n")
        self.txt_log.see(tk.END)


def run_app():
    app = SIS3RelojApp()
    app.mainloop()
