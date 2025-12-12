# sis3_reloj/config.py
from configparser import ConfigParser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.ini"

class AppConfig:
    def __init__(self, ip, port, password, sis2_disconnected, output_dir):
        self.ip = ip
        self.port = port
        self.password = password
        self.sis2_disconnected = sis2_disconnected
        self.output_dir = output_dir

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

    return AppConfig(ip, port, password, sis2_disc, output_dir)

def save_mode_sis2_disconnected(value: bool):
    parser = ConfigParser()
    if CONFIG_PATH.exists():
        parser.read(CONFIG_PATH, encoding="utf-8")

    if "modes" not in parser:
        parser["modes"] = {}

    parser["modes"]["sis2_disconnected"] = "true" if value else "false"

    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        parser.write(f)
