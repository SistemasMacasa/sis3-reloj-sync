# sis3_reloj/file_sink.py
from pathlib import Path
from datetime import datetime
from typing import List
from .zk_client import AttendanceRecord, UserRecord
import json

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)

def write_attendance_jsonl(records: List[AttendanceRecord], base_dir: Path) -> Path:
    ensure_dir(base_dir)
    now = datetime.now()
    fname = f"asistencia-{now:%Y%m%d-%H%M%S}.jsonl"
    fpath = base_dir / fname

    with fpath.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")

    return fpath


def write_users_jsonl(users: list[UserRecord], base_dir: Path) -> Path:
    ensure_dir(base_dir)

    # Archivo fijo (snapshot): siempre reemplaza el anterior
    fpath = base_dir / "usuarios.jsonl"
    tmp = base_dir / "usuarios.jsonl.tmp"

    with tmp.open("w", encoding="utf-8") as f:
        for user in users:
            f.write(json.dumps(user.to_dict(), ensure_ascii=False) + "\n")

    # Reemplazo atómico (en Windows funciona como "overwrite")
    tmp.replace(fpath)
    return fpath

