# sis3_reloj/gui.py
import tkinter as tk
from tkinter import ttk, messagebox
from pathlib import Path

from .config import load_config, save_mode_sis2_disconnected, BASE_DIR
from .zk_client import read_attendance, read_users
from .file_sink import write_attendance_jsonl, write_users_jsonl


class SIS3RelojApp(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("SIS3RelojChecador")
        self.geometry("600x400")

        self.config_obj = load_config()
        self._build_ui()

    def _build_ui(self):
        frame = ttk.Frame(self, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        # --- Línea 1: configuración de reloj ---
        row = 0
        ttk.Label(frame, text="IP:").grid(row=row, column=0, sticky="w")
        self.ip_var = tk.StringVar(value=self.config_obj.ip)
        ttk.Entry(frame, textvariable=self.ip_var, width=18).grid(row=row, column=1, sticky="w", padx=5)

        ttk.Label(frame, text="Port:").grid(row=row, column=2, sticky="w")
        self.port_var = tk.StringVar(value=str(self.config_obj.port))
        ttk.Entry(frame, textvariable=self.port_var, width=8).grid(row=row, column=3, sticky="w", padx=5)

        ttk.Label(frame, text="Password:").grid(row=row, column=4, sticky="w")
        self.pass_var = tk.StringVar(value=str(self.config_obj.password))
        ttk.Entry(frame, textvariable=self.pass_var, width=8).grid(row=row, column=5, sticky="w", padx=5)

        row += 1

        # --- Modo SIS2 ---
        self.sis2_disc_var = tk.BooleanVar(value=self.config_obj.sis2_disconnected)
        chk = ttk.Checkbutton(
            frame,
            text="Se ha desconectado SIS2 (modo post-SIS2)",
            variable=self.sis2_disc_var,
            command=self.on_toggle_sis2_mode,
        )
        chk.grid(row=row, column=0, columnspan=6, sticky="w", pady=(8, 8))

        row += 1

        # --- Botón principal ---
        self.btn_read = ttk.Button(
            frame,
            text="Leer asistencia y guardar archivo",
            command=self.on_read_and_save,
        )
        self.btn_read.grid(row=row, column=0, columnspan=6, pady=(5, 10))

        row += 1

                # --- Botón usuarios ---
        row += 1
        self.btn_users = ttk.Button(
            frame,
            text="Leer usuarios y guardar archivo",
            command=self.on_read_users_and_save,
        )
        self.btn_users.grid(row=row, column=0, columnspan=6, pady=(0, 10))

        row += 1

        # --- Área de log ---
        ttk.Label(frame, text="Log:").grid(row=row, column=0, sticky="w")
        row += 1
        self.txt_log = tk.Text(frame, height=12)
        self.txt_log.grid(row=row, column=0, columnspan=6, sticky="nsew")

        frame.rowconfigure(row, weight=1)
        frame.columnconfigure(5, weight=1)

        self.log("Aplicación iniciada.")

    def on_toggle_sis2_mode(self):
        val = self.sis2_disc_var.get()
        save_mode_sis2_disconnected(val)
        mode = "POST-SIS2 (podrá limpiar reloj en el futuro)" if val else "COEXISTENCIA con SIS2"
        self.log(f"Modo cambiado: {mode}")

    def on_read_and_save(self):
        try:
            ip = self.ip_var.get().strip()
            port = int(self.port_var.get())
            password = int(self.pass_var.get())
        except ValueError:
            messagebox.showerror("Error", "Port y Password deben ser numéricos.")
            return

        self.log(f"Conectando a {ip}:{port} ...")

        try:
            records = read_attendance(ip, port, password)
        except Exception as e:
            self.log(f"❌ Error al leer asistencia: {e}")
            messagebox.showerror("Error", f"No se pudo leer asistencia:\n{e}")
            return

        self.log(f"Se obtuvieron {len(records)} registros de asistencia.")

        output_dir = BASE_DIR / self.config_obj.output_dir
        path = write_attendance_jsonl(records, output_dir)
        self.log(f"Archivo guardado en: {path}")

        messagebox.showinfo("OK", f"Se guardaron {len(records)} registros en:\n{path}")

        # Importante: aquí NO se borra nada del reloj,
        # aunque el checkbox de SIS2 diga que está desconectado.
        # Esa lógica la metemos después cuando tengamos SIS3 en producción.

    def log(self, msg: str):
        self.txt_log.insert(tk.END, msg + "\n")
        self.txt_log.see(tk.END)

    def on_read_users_and_save(self):
        try:
            ip = self.ip_var.get().strip()
            port = int(self.port_var.get())
            password = int(self.pass_var.get())
        except ValueError:
            messagebox.showerror("Error", "Port y Password deben ser numéricos.")
            return

        self.log(f"Conectando a {ip}:{port} para leer usuarios...")

        try:
            users = read_users(ip, port, password)
        except Exception as e:
            self.log(f"❌ Error al leer usuarios: {e}")
            messagebox.showerror("Error", f"No se pudo leer usuarios:\n{e}")
            return

        self.log(f"Se obtuvieron {len(users)} usuarios del dispositivo.")

        output_dir = BASE_DIR / self.config_obj.output_dir
        path = write_users_jsonl(users, output_dir)
        self.log(f"Archivo de usuarios guardado en: {path}")

        messagebox.showinfo("OK", f"Se guardaron {len(users)} usuarios en:\n{path}")


def run_app():
    app = SIS3RelojApp()
    app.mainloop()
