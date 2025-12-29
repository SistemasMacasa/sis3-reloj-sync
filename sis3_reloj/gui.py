# sis3_reloj/gui.py
import tkinter as tk
import threading
import sys
from tkinter import ttk, messagebox
from pathlib import Path

from .config import load_config, BASE_DIR
from .odbc_bootstrap import ensure_odbc_driver
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

        # entries header (para habilitar/deshabilitar)
        self.ent_ip = None
        self.ent_port = None
        self.ent_pass = None

        self._build_ui()
        self.log("[APP] Aplicación iniciada.")

        # aplicar modo según config (si SIS2 está “post-SIS2”, ocultar tab)
        self.apply_sis2_mode_from_config()

    def _build_ui(self):
        root = ttk.Frame(self, padding=10)
        root.pack(fill=tk.BOTH, expand=True)

        # ✅ Estilos ttk para estado SIS2 (badge/pill)
        style = ttk.Style(self)

        # Badge base (padding y borde)
        try:
            style.configure("SIS2.Badge.TLabel", padding=(10, 4), relief="solid", borderwidth=1)
        except Exception:
            pass

        style.configure("SIS2.Badge.Disconnected.TLabel", foreground="#7f1d1d", background="#fee2e2")
        style.configure("SIS2.Badge.Connecting.TLabel", foreground="#7c2d12", background="#ffedd5")
        style.configure("SIS2.Badge.Connected.TLabel", foreground="#14532d", background="#dcfce7")

        # Header: panel centrado (global)
        header = ttk.LabelFrame(root, text="Conexión al reloj", padding=(10, 8))
        header.pack(fill=tk.X)

        ttk.Label(header, text="IP").grid(row=0, column=0, sticky="w")
        self.ent_ip = ttk.Entry(header, textvariable=self.ip_var, width=18)
        self.ent_ip.grid(row=0, column=1, sticky="w", padx=(6, 16))

        ttk.Label(header, text="Port").grid(row=0, column=2, sticky="w")
        self.ent_port = ttk.Entry(header, textvariable=self.port_var, width=8)
        self.ent_port.grid(row=0, column=3, sticky="w", padx=(6, 16))

        ttk.Label(header, text="Password").grid(row=0, column=4, sticky="w")
        self.ent_pass = ttk.Entry(header, textvariable=self.pass_var, width=8, show="•")
        self.ent_pass.grid(row=0, column=5, sticky="w", padx=(6, 0))

        # Spacer
        header.columnconfigure(6, weight=1)

        # Estado (badge/pill)
        self.lbl_sis2_state = ttk.Label(
            header,
            text="SIS2: Desconectado",
            style="SIS2.Badge.Disconnected.TLabel"
        )
        self.lbl_sis2_state.grid(row=0, column=7, sticky="e", padx=(8, 10))

        # Toggle SIS2
        self.chk_sis2 = ttk.Checkbutton(
            header,
            text="SIS2 Conectado",
            variable=self.sis2_conn_var,
            command=self._on_toggle_sis2
        )
        self.chk_sis2.grid(row=0, column=8, sticky="e")

        # Notebook: SIS2 / SIS3 / Ajustes
        self.nb = ttk.Notebook(root)
        self.nb.pack(fill=tk.BOTH, expand=True, pady=(10, 10))

        self.tab_sis2 = ttk.Frame(self.nb)
        self.tab_sis3 = ttk.Frame(self.nb)
        self.tab_ajustes = ttk.Frame(self.nb)

        self.nb.add(self.tab_sis2, text="SIS2")
        self.nb.add(self.tab_sis3, text="SIS3")
        self.nb.add(self.tab_ajustes, text="Ajustes")

        # Log global (con scroll + read-only)
        ttk.Label(root, text="Log").pack(anchor="w", pady=(0, 4))

        log_wrap = ttk.Frame(root)
        log_wrap.pack(fill=tk.BOTH, expand=True)

        self.txt_log = tk.Text(log_wrap, height=12, wrap="word")
        self.txt_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scroll = ttk.Scrollbar(log_wrap, orient="vertical", command=self.txt_log.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)

        self.txt_log.configure(yscrollcommand=scroll.set)

        # Fuente fija + tags (si el tema lo permite)
        try:
            self.txt_log.configure(font=("Consolas", 10))
        except Exception:
            pass

        try:
            self.txt_log.tag_configure("APP", foreground="#1f2937")
            self.txt_log.tag_configure("SIS2", foreground="#1d4ed8")
            self.txt_log.tag_configure("SIS3", foreground="#047857")
            self.txt_log.tag_configure("AJUSTES", foreground="#7c2d12")
            self.txt_log.tag_configure("ERR", foreground="#b91c1c")
        except Exception:
            pass

        try:
            self.txt_log.configure(state="disabled")
        except Exception:
            pass

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

    def set_header_inputs_enabled(self, enabled: bool):
        """
        Evita que cambien IP/Port/Password a mitad de un envío/probe.
        """
        state = "!disabled" if enabled else "disabled"
        for w in (self.ent_ip, self.ent_port, self.ent_pass):
            if w is None:
                continue
            try:
                w.state([state])
            except Exception:
                try:
                    w.configure(state=("normal" if enabled else "disabled"))
                except Exception:
                    pass

    def log(self, msg: str):
        if self.txt_log is None:
            return

        m = (msg or "").strip()
        tag = None

        if m.startswith("[SIS2]"):
            tag = "SIS2"
        elif m.startswith("[SIS3]"):
            tag = "SIS3"
        elif m.startswith("[AJUSTES]"):
            tag = "AJUSTES"
        elif m.startswith("[APP]"):
            tag = "APP"

        is_err = ("ERROR" in m) or ("❌" in m) or ("Fallo" in m)

        try:
            self.txt_log.configure(state="normal")
        except Exception:
            pass

        if tag:
            self.txt_log.insert(tk.END, m + "\n", (tag, "ERR") if is_err else (tag,))
        else:
            self.txt_log.insert(tk.END, m + "\n", ("ERR",) if is_err else ())

        self.txt_log.see(tk.END)

        try:
            self.txt_log.configure(state="disabled")
        except Exception:
            pass

    # -------------------------
    # SIS2: UI state + hide/show
    # -------------------------
    def set_sis2_header_state(self, ok: bool, msg: str | None = None, phase: str | None = None):
        """
        Estado visible (label) = qué está pasando.
        Checkbox = permiso/acción (permitir o no el envío).
        """
        if phase == "connecting":
            self.lbl_sis2_state.config(text="SIS2: Conectando…", style="SIS2.Badge.Connecting.TLabel")
        elif ok:
            self.lbl_sis2_state.config(text="SIS2: Conectado", style="SIS2.Badge.Connected.TLabel")
        else:
            self.lbl_sis2_state.config(text="SIS2: Desconectado", style="SIS2.Badge.Disconnected.TLabel")

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

        # bloquear el toggle e inputs mientras prueba (evita clicks nerviosos y cambios a mitad)
        try:
            self.chk_sis2.state(["disabled"])
        except Exception:
            pass
        self.set_header_inputs_enabled(False)

        def worker():
            try:
                # Si SIS2 está en modo DB, asegurar driver ODBC antes del probe
                try:
                    sis2_mode = str(getattr(self.config_obj, "sis2_mode", "") or "").strip().lower()
                except Exception:
                    sis2_mode = ""

                if sis2_mode == "db":
                    ok_odbc, msg_odbc = ensure_odbc_driver()
                    self.after(0, lambda: self.log(f"[APP] {msg_odbc}"))
                    if not ok_odbc:
                        def fail_odbc():
                            self.set_sis2_header_state(False, f"[SIS2] {msg_odbc}")
                            self.sis2_conn_var.set(False)
                            try:
                                self.chk_sis2.state(["!disabled"])
                            except Exception:
                                pass
                            self.set_header_inputs_enabled(True)
                            messagebox.showerror("Error", msg_odbc)
                        self.after(0, fail_odbc)
                        return

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
                    self.set_header_inputs_enabled(True)

                self.after(0, apply_ok)

            except Exception as e:
                def apply_err():
                    self.set_sis2_header_state(False, f"[SIS2] ERROR al conectar: {e}")
                    self.sis2_conn_var.set(False)
                    try:
                        self.chk_sis2.state(["!disabled"])
                    except Exception:
                        pass
                    self.set_header_inputs_enabled(True)

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
