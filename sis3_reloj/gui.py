# sis3_reloj/gui.py
import tkinter as tk
import threading
from tkinter import ttk
from pathlib import Path

from .config import load_config, BASE_DIR
from .gui_tab_sis2 import build_tab_sis2
from .gui_tab_sis3 import build_tab_sis3
from .gui_tab_ajustes import build_tab_ajustes


class SIS3RelojApp(tk.Tk):
    """
    GUI principal.

    Hooks expuestos a pestañas:
      - set_sis2_badge_state(ok/phase/msg, auto_reset_ms=...)
      - set_reloj_badge_state(ok/phase/msg, auto_reset_ms=...)
      - clear_log()
      - apply_sis2_mode_from_config()
    """

    LOG_CLEAR_TOKEN = "__CLEAR_LOG__"

    def __init__(self):
        super().__init__()
        self.title("SIS3RelojChecador")
        self.geometry("740x720")

        self.config_obj = load_config()

        # Vars globales (header)
        self.ip_var = tk.StringVar(value=self.config_obj.ip)
        self.port_var = tk.StringVar(value=str(self.config_obj.port))
        self.pass_var = tk.StringVar(value=str(self.config_obj.password))

        # refs notebook/tabs
        self.nb = None
        self.tab_sis2 = None
        self.tab_sis3 = None
        self.tab_ajustes = None

        # widgets log + badges (creados en UI y “enlazados” aquí)
        self.txt_log = None

        # Badge SIS2 (label dentro del tab SIS2)
        self.lbl_sis2_state = None
        self.chk_sis2 = None  # legacy (tolerado)

        # Badge Reloj/Checador (label dentro del header)
        self.lbl_reloj_state = None

        # after_ids para auto-reset
        self._sis2_badge_reset_after_id = None
        self._reloj_badge_reset_after_id = None

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

        # ✅ Estilos ttk para badge SIS2
        style = ttk.Style(self)
        try:
            style.configure("SIS2.Badge.TLabel", padding=(10, 4), relief="solid", borderwidth=1)
        except Exception:
            pass

        style.configure("SIS2.Badge.Disconnected.TLabel", foreground="#7f1d1d", background="#fee2e2")
        style.configure("SIS2.Badge.Connecting.TLabel", foreground="#7c2d12", background="#ffedd5")
        style.configure("SIS2.Badge.Connected.TLabel", foreground="#14532d", background="#dcfce7")

        # ✅ Estilos ttk para badge Reloj/Checador (global en header)
        try:
            style.configure("Reloj.Badge.TLabel", padding=(10, 4), relief="solid", borderwidth=1)
        except Exception:
            pass

        style.configure("Reloj.Badge.Disconnected.TLabel", foreground="#7f1d1d", background="#fee2e2")
        style.configure("Reloj.Badge.Connecting.TLabel", foreground="#7c2d12", background="#ffedd5")
        style.configure("Reloj.Badge.Connected.TLabel", foreground="#14532d", background="#dcfce7")

        # Header: Conexión al reloj
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
        self.ent_pass.grid(row=0, column=5, sticky="w", padx=(6, 12))

        # spacer
        header.columnconfigure(6, weight=1)

        # ✅ Badge global del checador (socket-like)
        self.lbl_reloj_state = ttk.Label(
            header,
            text="Reloj: Desconectado",
            style="Reloj.Badge.Disconnected.TLabel",
        )
        self.lbl_reloj_state.grid(row=0, column=7, sticky="e")

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

        # Construir tabs
        build_tab_sis2(
            self.tab_sis2,
            get_conn=self.get_connection,
            get_config=lambda: self.config_obj,
            log=self.log,
            # el tab “enlaza” widgets SIS2 al App (badge)
            bind_sis2_controls=lambda lbl, chk=None: (
                setattr(self, "lbl_sis2_state", lbl),
                setattr(self, "chk_sis2", chk),
            ),
            # hooks globales
            ui_set_sis2_badge=self.set_sis2_badge_state,
            ui_set_reloj_badge=self.set_reloj_badge_state,  # ✅ SOLO aquí (SIS2 usa checador)
            ui_clear_log=self.clear_log,
        )

        # ❗️IMPORTANTE: NO pasar ui_set_reloj_badge a SIS3 si gui_tab_sis3 no lo acepta
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
        Evita que cambien IP/Port/Password a mitad de un envío/proceso.
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

    # -------------------------
    # Log global
    # -------------------------
    def clear_log(self):
        if self.txt_log is None:
            return
        try:
            self.txt_log.configure(state="normal")
        except Exception:
            pass
        try:
            self.txt_log.delete("1.0", tk.END)
        except Exception:
            pass
        try:
            self.txt_log.configure(state="disabled")
        except Exception:
            pass

    def log(self, msg: str):
        """
        También acepta token especial para limpiar log sin acoplar la pestaña al widget:
          app.log("__CLEAR_LOG__")
        """
        if self.txt_log is None:
            return

        m = (msg or "").strip()

        if m == self.LOG_CLEAR_TOKEN:
            self.clear_log()
            return

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
    # SIS2 badge state (transitorio)
    # -------------------------
    def set_sis2_badge_state(
        self,
        ok: bool | None = None,
        *,
        phase: str | None = None,
        msg: str | None = None,
        auto_reset_ms: int | None = None,
    ):
        if self.lbl_sis2_state is None:
            if msg:
                self.log(msg)
            return

        # cancelar auto-reset previo
        try:
            if self._sis2_badge_reset_after_id is not None:
                self.after_cancel(self._sis2_badge_reset_after_id)
                self._sis2_badge_reset_after_id = None
        except Exception:
            self._sis2_badge_reset_after_id = None

        ph = (phase or "").strip().lower()

        if ph == "connecting":
            try:
                self.lbl_sis2_state.config(text="SIS2: Conectando…", style="SIS2.Badge.Connecting.TLabel")
            except Exception:
                pass

        elif ph == "connected" or ok is True:
            try:
                self.lbl_sis2_state.config(text="SIS2: Conectado", style="SIS2.Badge.Connected.TLabel")
            except Exception:
                pass

            if auto_reset_ms and int(auto_reset_ms) > 0:
                def _reset():
                    try:
                        self.lbl_sis2_state.config(
                            text="SIS2: Desconectado",
                            style="SIS2.Badge.Disconnected.TLabel",
                        )
                    except Exception:
                        pass
                    self._sis2_badge_reset_after_id = None

                try:
                    self._sis2_badge_reset_after_id = self.after(int(auto_reset_ms), _reset)
                except Exception:
                    self._sis2_badge_reset_after_id = None

        else:
            try:
                self.lbl_sis2_state.config(text="SIS2: Desconectado", style="SIS2.Badge.Disconnected.TLabel")
            except Exception:
                pass

        if msg:
            self.log(msg)

    # -------------------------
    # Reloj/Checador badge state (GLOBAL en header)
    # -------------------------
    def set_reloj_badge_state(
        self,
        ok: bool | None = None,
        *,
        phase: str | None = None,
        msg: str | None = None,
        auto_reset_ms: int | None = None,
    ):
        """
        Badge del checador (ZK) como estado transitorio (socket-like):
          - connecting: durante operación
          - connected: confirmación breve (si auto_reset_ms)
          - disconnected: estado por defecto (no hay socket abierto)
        """
        if self.lbl_reloj_state is None:
            if msg:
                self.log(msg)
            return

        # cancelar auto-reset previo
        try:
            if self._reloj_badge_reset_after_id is not None:
                self.after_cancel(self._reloj_badge_reset_after_id)
                self._reloj_badge_reset_after_id = None
        except Exception:
            self._reloj_badge_reset_after_id = None

        ph = (phase or "").strip().lower()

        if ph == "connecting":
            try:
                self.lbl_reloj_state.config(text="Reloj: Conectando…", style="Reloj.Badge.Connecting.TLabel")
            except Exception:
                pass

        elif ph == "connected" or ok is True:
            try:
                self.lbl_reloj_state.config(text="Reloj: Conectado", style="Reloj.Badge.Connected.TLabel")
            except Exception:
                pass

            if auto_reset_ms and int(auto_reset_ms) > 0:
                def _reset():
                    try:
                        self.lbl_reloj_state.config(
                            text="Reloj: Desconectado",
                            style="Reloj.Badge.Disconnected.TLabel",
                        )
                    except Exception:
                        pass
                    self._reloj_badge_reset_after_id = None

                try:
                    self._reloj_badge_reset_after_id = self.after(int(auto_reset_ms), _reset)
                except Exception:
                    self._reloj_badge_reset_after_id = None

        else:
            try:
                self.lbl_reloj_state.config(text="Reloj: Desconectado", style="Reloj.Badge.Disconnected.TLabel")
            except Exception:
                pass

        if msg:
            self.log(msg)

    # -------------------------
    # SIS2: hide/show por modo post-SIS2
    # -------------------------
    def apply_sis2_mode_from_config(self):
        """
        Si sis2_disconnected=True:
          - Oculta pestaña SIS2
        Si sis2_disconnected=False:
          - Muestra pestaña SIS2
        """
        post = bool(getattr(self.config_obj, "sis2_disconnected", False))

        if post:
            try:
                self.nb.hide(self.tab_sis2)
            except Exception:
                pass

            self.set_sis2_badge_state(
                False,
                phase="disconnected",
                msg="[SIS2] Modo post-SIS2 activo: pestaña SIS2 deshabilitada.",
            )
        else:
            try:
                tabs = self.nb.tabs()
                sis2_id = str(self.tab_sis2)
                if sis2_id not in tabs:
                    self.nb.insert(0, self.tab_sis2, text="SIS2")
            except Exception:
                pass

    def on_toggle_sis2_disconnected(self, value: bool):
        """
        Callback desde Ajustes cuando el usuario cambia el modo post-SIS2.
        """
        self.set_config_field("sis2_disconnected", bool(value))
        self.apply_sis2_mode_from_config()


def run_app():
    app = SIS3RelojApp()
    app.mainloop()


if __name__ == "__main__":
    run_app()
