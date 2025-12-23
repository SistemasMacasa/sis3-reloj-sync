from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Any
import os

# Records vienen de zk_client.AttendanceRecord (tienen .user_id, .timestamp, .punch)
# Aquí no importamos el tipo para no acoplar.
import pyodbc


@dataclass(frozen=True)
class Sis2DbConfig:
    enabled: bool
    server: str          # ej: "app.sismanagement.com.mx\\MSSQLSERVER2019,1434"
    database: str        # ej: "admin_macasa_prod"
    username: str        # ej: "dba_mchs"
    password: str        # NO hardcode; venir de env o config.ini si aceptan el riesgo
    driver: str = "ODBC Driver 18 for SQL Server"  # o 17
    trust_server_certificate: bool = True
    timeout_sec: int = 10


def _conn_str(cfg: Sis2DbConfig) -> str:
    tsc = "yes" if cfg.trust_server_certificate else "no"
    # Encrypt=yes es default en Driver 18; en 17 no siempre. Forzamos Encrypt=yes para consistencia.
    return (
        f"DRIVER={{{cfg.driver}}};"
        f"SERVER={cfg.server};"
        f"DATABASE={cfg.database};"
        f"UID={cfg.username};"
        f"PWD={cfg.password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate={tsc};"
        f"Connection Timeout={int(cfg.timeout_sec)};"
    )


def send_attendance_to_sis2_db(records: Iterable[Any], cfg: Sis2DbConfig, log=print) -> dict:
    """
    Inserta en dba_mchs.Tb_PersonalAsistencia:
      IdPersonal = user_id
      Asistencia = timestamp
      Tipo = punch
      CodigoVerificador = 0 (por compatibilidad con el viejo)
    Con idempotencia: no inserta si ya existe (IdPersonal + Asistencia + Tipo + CodigoVerificador).

    Retorna: {ok, inserted, skipped, error?}
    """
    if not cfg.enabled:
        log("SIS2(DB): disabled -> skip")
        return {"ok": True, "skipped": 0, "inserted": 0, "reason": "disabled"}

    rows = []
    for r in records:
        user_id = int(getattr(r, "user_id"))
        ts = getattr(r, "timestamp")
        punch = int(getattr(r, "punch", 0) or 0)
        if not isinstance(ts, datetime):
            # si viene string, intenta parse ISO (tu JSONL trae ISO)
            ts = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).replace(tzinfo=None)
        rows.append((user_id, ts, str(punch), 0))  # Tipo es varchar(10) en la tabla

    if not rows:
        return {"ok": True, "inserted": 0, "skipped": 0, "reason": "no_rows"}

    inserted = 0
    skipped = 0

    sql_exists = """
    SELECT 1
    FROM dba_mchs.Tb_PersonalAsistencia WITH (NOLOCK)
    WHERE IdPersonal = ?
      AND Asistencia = ?
      AND Tipo = ?
      AND CodigoVerificador = ?
    """

    sql_insert = """
    INSERT INTO dba_mchs.Tb_PersonalAsistencia (IdPersonal, Asistencia, Tipo, CodigoVerificador)
    VALUES (?, ?, ?, ?)
    """

    try:
        cn = pyodbc.connect(_conn_str(cfg), autocommit=False)
        cur = cn.cursor()

        for row in rows:
            cur.execute(sql_exists, row)
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(sql_insert, row)
            inserted += 1

        cn.commit()
        cn.close()

        log(f"SIS2(DB): inserted={inserted}, skipped={skipped}")
        return {"ok": True, "inserted": inserted, "skipped": skipped}

    except Exception as e:
        try:
            cn.rollback()
        except Exception:
            pass
        log(f"SIS2(DB) ERROR: {e}")
        return {"ok": False, "error": str(e), "inserted": inserted, "skipped": skipped}
