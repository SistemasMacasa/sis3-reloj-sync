# sis3_reloj/config.py
from configparser import ConfigParser
from pathlib import Path
import sys
import os

APP_NAME = "SIS3RelojChecador"

def _exe_dir() -> Path:
    # Carpeta física del .exe (solo útil para leer config.ini “portable”)
    return Path(sys.executable).resolve().parent

def _appdata_dir() -> Path:
    # Carpeta estable para datos del usuario (evita permisos)
    root = os.getenv("LOCALAPPDATA") or os.getenv("APPDATA") or str(Path.home())
    p = Path(root) / APP_NAME
    p.mkdir(parents=True, exist_ok=True)
    return p

def _app_base_dir() -> Path:
    """
    Dev: raíz del repo
    EXE: AppData (datos persistentes, sin bronca de permisos)
    """
    if getattr(sys, "frozen", False):
        return _appdata_dir()
    return Path(__file__).resolve().parent.parent

BASE_DIR = _app_base_dir()

# Config:
# - Dev: en raíz del repo
# - EXE: junto al .exe (portable)
if getattr(sys, "frozen", False):
    CONFIG_PATH = _exe_dir() / "config.ini"
else:
    CONFIG_PATH = BASE_DIR / "config.ini"
class AppConfig:
    def __init__(
        self,
        ip: str,
        port: int,
        password: int,
        sis2_disconnected: bool,
        output_dir: str,
        sis2_enabled: bool,
        sis2_mode: str,
        sis2_drop_dir: str,
        sis2_base_url: str,
        sis2_api_key: str,
        sis2_timeout_sec: int,
        sis2_db_server: str,
        sis2_db_database: str,
        sis2_db_username: str,
        sis2_db_password: str,
        sis2_db_driver: str,
        sis2_db_trust_server_certificate: bool,

        # ✅ SIS3 API
        sis3_base_url: str,
        sis3_api_key: str,
        sis3_timeout_sec: int,
    ):
        self.ip = ip
        self.port = port
        self.password = password

        self.sis2_disconnected = sis2_disconnected
        self.output_dir = output_dir

        # SIS2 sink (file/http/db)
        self.sis2_enabled = sis2_enabled
        self.sis2_mode = sis2_mode
        self.sis2_drop_dir = sis2_drop_dir
        self.sis2_base_url = sis2_base_url
        self.sis2_api_key = sis2_api_key
        self.sis2_timeout_sec = sis2_timeout_sec

        # SIS2 DB sink
        self.sis2_db_server = sis2_db_server
        self.sis2_db_database = sis2_db_database
        self.sis2_db_username = sis2_db_username
        self.sis2_db_password = sis2_db_password
        self.sis2_db_driver = sis2_db_driver
        self.sis2_db_trust_server_certificate = sis2_db_trust_server_certificate

        # ✅ SIS3 API
        self.sis3_base_url = sis3_base_url
        self.sis3_api_key = sis3_api_key
        self.sis3_timeout_sec = sis3_timeout_sec


def load_config() -> AppConfig:
    parser = ConfigParser()
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"No se encontró config.ini en {CONFIG_PATH}")

    parser.read(CONFIG_PATH, encoding="utf-8")

    ip = parser.get("reloj", "ip", fallback="192.168.1.145")
    port = parser.getint("reloj", "port", fallback=4370)
    password = parser.getint("reloj", "password", fallback=0)

    sis2_disc = parser.getboolean("modes", "sis2_disconnected", fallback=False)
    output_dir = parser.get("logging", "output_dir", fallback="out")

    # SIS2 sink
    sis2_enabled = parser.getboolean("sis2", "enabled", fallback=True)
    sis2_mode = parser.get("sis2", "mode", fallback="file")
    sis2_drop_dir = parser.get("sis2", "drop_dir", fallback="out/sis2")
    sis2_base_url = parser.get("sis2", "base_url", fallback="")
    sis2_api_key = parser.get("sis2", "api_key", fallback="")
    sis2_timeout_sec = parser.getint("sis2", "timeout_sec", fallback=10)

    # SIS2 DB sink
    sis2_db_server = parser.get("sis2_db", "server", fallback="")
    sis2_db_database = parser.get("sis2_db", "database", fallback="admin_macasa_prod")
    sis2_db_username = parser.get("sis2_db", "username", fallback="")
    sis2_db_password = parser.get("sis2_db", "password", fallback="")  # recomendado vacío + env
    sis2_db_driver = parser.get("sis2_db", "driver", fallback="ODBC Driver 18 for SQL Server")
    sis2_db_trust = parser.getboolean("sis2_db", "trust_server_certificate", fallback=True)

    # ✅ SIS3 API
    sis3_base_url = parser.get("sis3", "base_url", fallback="")
    sis3_api_key  = parser.get("sis3", "api_key", fallback="")
    sis3_timeout_sec = parser.getint("sis3", "timeout_sec", fallback=20)

    return AppConfig(
        ip, port, password,
        sis2_disc, output_dir,
        sis2_enabled, sis2_mode, sis2_drop_dir, sis2_base_url, sis2_api_key, sis2_timeout_sec,
        sis2_db_server, sis2_db_database, sis2_db_username, sis2_db_password, sis2_db_driver, sis2_db_trust,

        # ✅ SIS3
        sis3_base_url, sis3_api_key, sis3_timeout_sec,
    )


def save_mode_sis2_disconnected(value: bool):
    """
    Actualiza SOLO la llave [modes] sis2_disconnected sin reescribir todo el INI,
    para preservar comentarios/formato y evitar efectos colaterales.
    """
    new_val = "true" if value else "false"

    if not CONFIG_PATH.exists():
        # ini mínimo
        content = "[modes]\n" + f"sis2_disconnected = {new_val}\n"
        CONFIG_PATH.write_text(content, encoding="utf-8")
        return

    lines = CONFIG_PATH.read_text(encoding="utf-8").splitlines(True)

    in_modes = False
    modes_found = False
    key_written = False

    out = []
    for i, line in enumerate(lines):
        stripped = line.strip()

        # Detecta sección
        if stripped.startswith("[") and stripped.endswith("]"):
            # Si salimos de [modes] y no escribimos la key, la insertamos antes de cambiar sección
            if in_modes and not key_written:
                out.append(f"sis2_disconnected = {new_val}\n")
                key_written = True

            section = stripped[1:-1].strip().lower()
            in_modes = (section == "modes")
            if in_modes:
                modes_found = True

            out.append(line)
            continue

        # Si estamos dentro de [modes], buscamos la key
        if in_modes:
            # soporta formatos: key=, key :, key con espacios
            if stripped.lower().startswith("sis2_disconnected"):
                # Reemplaza la línea completa
                out.append(f"sis2_disconnected = {new_val}\n")
                key_written = True
                continue

        out.append(line)

    # Si el archivo no tenía [modes], lo agregamos al final
    if not modes_found:
        if out and not out[-1].endswith("\n"):
            out[-1] += "\n"
        if out and out[-1].strip() != "":
            out.append("\n")
        out.append("[modes]\n")
        out.append(f"sis2_disconnected = {new_val}\n")
        key_written = True

    # Si sí tenía [modes] pero no la key, pudo ya haberse insertado al salir de la sección;
    # si el archivo termina dentro de [modes], la insertamos aquí.
    if modes_found and not key_written:
        if out and not out[-1].endswith("\n"):
            out[-1] += "\n"
        out.append(f"sis2_disconnected = {new_val}\n")

    CONFIG_PATH.write_text("".join(out), encoding="utf-8")
