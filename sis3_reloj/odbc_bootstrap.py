# sis3_reloj/odbc_bootstrap.py
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ODBC_NAME = "ODBC Driver 18 for SQL Server"
MSI_NAME = "msodbcsql18.msi"


def _has_driver() -> bool:
    """
    Revisa si Windows ya tiene el driver ODBC 18.
    Si pyodbc no está disponible aún, regresamos False para intentar instalar.
    """
    try:
        import pyodbc
        return ODBC_NAME in (pyodbc.drivers() or [])
    except Exception:
        return False


def _runtime_base_dir() -> Path:
    """
    Dev: carpeta del proyecto (cwd / donde está el código).
    EXE onefile: carpeta temporal de PyInstaller (sys._MEIPASS).
    """
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    return Path.cwd()


def ensure_odbc_driver(base_dir: Path | None = None) -> tuple[bool, str]:
    """
    Asegura que exista el driver ODBC 18.
    - Si ya está: (True, "ya estaba instalado")
    - Si no está: intenta instalar msodbcsql18.msi en modo silencioso.
    """
    if _has_driver():
        return True, "Driver ODBC ya instalado."

    base = (base_dir if base_dir is not None else _runtime_base_dir())
    msi_path = (base / "assets" / "odbc" / MSI_NAME).resolve()

    if not msi_path.exists():
        return False, f"No se encontró {MSI_NAME}. Esperado en: {msi_path}"

    # Instalación silenciosa (sin ventanas)
    cmd = [
        "msiexec",
        "/i", str(msi_path),
        "/quiet",
        "/norestart",
        "IACCEPTMSODBCSQLLICENSETERMS=YES"
    ]

    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        return False, f"No se pudo instalar el driver ODBC (msiexec code={e.returncode})."

    # Re-checar
    if _has_driver():
        return True, "Driver ODBC instalado correctamente."
    return False, "Se intentó instalar ODBC, pero Windows no lo registró (reinicio o permisos)."
