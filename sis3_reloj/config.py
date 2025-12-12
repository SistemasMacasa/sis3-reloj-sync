# sis3_reloj/config.py
from configparser import ConfigParser
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = BASE_DIR / "config.ini"


class AppConfig:
    def __init__(
        self,
        ip,
        port,
        password,
        sis2_disconnected,
        output_dir,
        sis2_enabled,
        sis2_mode,
        sis2_drop_dir,
        sis2_base_url,
        sis2_api_key,
        sis2_timeout_sec,
    ):
        self.ip = ip
        self.port = port
        self.password = password
        self.sis2_disconnected = sis2_disconnected
        self.output_dir = output_dir

        self.sis2_enabled = sis2_enabled
        self.sis2_mode = sis2_mode
        self.sis2_drop_dir = sis2_drop_dir
        self.sis2_base_url = sis2_base_url
        self.sis2_api_key = sis2_api_key
        self.sis2_timeout_sec = sis2_timeout_sec


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

    # SIS2 sink (configurable)
    sis2_enabled = parser.getboolean("sis2", "enabled", fallback=True)
    sis2_mode = parser.get("sis2", "mode", fallback="file")
    sis2_drop_dir = parser.get("sis2", "drop_dir", fallback="out/sis2")
    sis2_base_url = parser.get("sis2", "base_url", fallback="")
    sis2_api_key = parser.get("sis2", "api_key", fallback="")
    sis2_timeout_sec = parser.getint("sis2", "timeout_sec", fallback=10)

    return AppConfig(
        ip,
        port,
        password,
        sis2_disc,
        output_dir,
        sis2_enabled,
        sis2_mode,
        sis2_drop_dir,
        sis2_base_url,
        sis2_api_key,
        sis2_timeout_sec,
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

