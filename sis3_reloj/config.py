# sis3_reloj/config.py
from configparser import ConfigParser
from pathlib import Path
import sys
import os

def _app_base_dir() -> Path:
    """
    Dev: carpeta del repo (junto a main.py)
    EXE (PyInstaller): carpeta donde está el .exe
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent  # carpeta del EXE
    return Path(__file__).resolve().parent.parent

BASE_DIR = _app_base_dir()
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

    return AppConfig(
        ip, port, password,
        sis2_disc, output_dir,
        sis2_enabled, sis2_mode, sis2_drop_dir, sis2_base_url, sis2_api_key, sis2_timeout_sec,
        sis2_db_server, sis2_db_database, sis2_db_username, sis2_db_password, sis2_db_driver, sis2_db_trust
    )


def save_mode_sis2_disconnected(value: bool):
    parser = ConfigParser()
    if CONFIG_PATH.exists():
        parser.read(CONFIG_PATH, encoding="utf-8")

    if "modes" not in parser:
        parser["modes"] = {}

    parser["modes"]["sis2_disconnected"] = "true" if value else "false"

    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        parser.write(f)
