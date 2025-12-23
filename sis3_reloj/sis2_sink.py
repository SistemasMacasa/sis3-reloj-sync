# sis3_reloj/sis2_sink.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Iterable, Optional
import json
import time
import os

try:
    import requests  # opcional si mode=http
except Exception:
    requests = None


@dataclass(frozen=True)
class Sis2Config:
    enabled: bool
    mode: str  # "file" | "http" | "db"
    drop_dir: Path
    base_url: str
    api_key: str
    timeout_sec: int

    # DB mode
    db_server: str = ""
    db_database: str = "admin_macasa_prod"
    db_username: str = ""
    db_password: str = ""  # recomendado: vacío y usar env SIS2_DB_PASSWORD
    db_driver: str = "ODBC Driver 18 for SQL Server"
    db_trust_server_certificate: bool = True


def _now_tag() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def _to_jsonable(r: Any) -> Any:
    if isinstance(r, dict):
        return {k: _to_jsonable(v) for k, v in r.items()}

    if isinstance(r, (datetime, date)):
        return r.isoformat()

    d = getattr(r, "__dict__", None)
    if isinstance(d, dict):
        return {k: _to_jsonable(v) for k, v in d.items()}

    return r


def _write_jsonl(records: Iterable[Any], path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(_to_jsonable(r), ensure_ascii=False) + "\n")
            n += 1
    return n


def _send_db(records: list, cfg: Sis2Config, _log: callable) -> dict:
    try:
        import pyodbc
    except Exception:
        raise RuntimeError("pyodbc no está instalado. Ejecuta: pip install pyodbc")

    pwd = (cfg.db_password or os.environ.get("SIS2_DB_PASSWORD", "")).strip()
    if not cfg.db_server.strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.server en config.ini")
    if not cfg.db_username.strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.username en config.ini")
    if not pwd:
        raise RuntimeError("SIS2(DB): falta password. Usa env SIS2_DB_PASSWORD o sis2_db.password (no recomendado).")

    trust = "yes" if cfg.db_trust_server_certificate else "no"
    conn_str = (
        f"DRIVER={{{cfg.db_driver}}};"
        f"SERVER={cfg.db_server};"
        f"DATABASE={cfg.db_database};"
        f"UID={cfg.db_username};"
        f"PWD={pwd};"
        f"Encrypt=yes;"
        f"TrustServerCertificate={trust};"
        f"Connection Timeout={int(cfg.timeout_sec)};"
    )

    # Inserción idempotente: no duplicar (IdPersonal + Asistencia + Tipo + CodigoVerificador)
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

    rows = []
    for r in records:
        user_id = int(getattr(r, "user_id"))
        ts = getattr(r, "timestamp")
        status = int(getattr(r, "status", 1) or 1)

        # timestamp debe ser datetime
        if not isinstance(ts, datetime):
            ts = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).replace(tzinfo=None)

        # Tipo en la tabla es varchar(10); guardamos punch como string ("0"/"1")
        rows.append((user_id, ts, str(status), 0))

    inserted = 0
    skipped = 0

    _log(f"SIS2(DB): conectando a {cfg.db_server} / {cfg.db_database} ...")
    cn = pyodbc.connect(conn_str, autocommit=False)
    try:
        cur = cn.cursor()
        for row in rows:
            cur.execute(sql_exists, row)
            if cur.fetchone():
                skipped += 1
                continue
            cur.execute(sql_insert, row)
            inserted += 1
        cn.commit()
    except Exception:
        cn.rollback()
        raise
    finally:
        cn.close()

    _log(f"SIS2(DB): inserted={inserted}, skipped={skipped}")
    return {"ok": True, "mode": "db", "inserted": inserted, "skipped": skipped, "count": len(rows)}


def send_attendance_to_sis2(records: list, cfg: Sis2Config, log: Optional[callable] = None) -> dict:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if not cfg.enabled:
        _log("SIS2: disabled (cfg.enabled=false).")
        return {"ok": True, "skipped": True, "reason": "disabled"}

    mode = (cfg.mode or "file").strip().lower()

    if mode == "file":
        out_path = cfg.drop_dir / f"asis-sis2-{_now_tag()}.jsonl"
        n = _write_jsonl(records, out_path)
        _log(f"SIS2(FILE): wrote {n} records -> {out_path}")
        return {"ok": True, "mode": "file", "count": n, "path": str(out_path)}

    if mode == "http":
        if requests is None:
            raise RuntimeError("requests no está instalado. Ejecuta: pip install requests")
        if not cfg.base_url:
            raise RuntimeError("SIS2 HTTP: falta sis2.base_url en config.ini")

        url = cfg.base_url.rstrip("/") + "/api/reloj/asistencia"
        headers = {"Content-Type": "application/json"}
        if cfg.api_key:
            headers["Authorization"] = f"Bearer {cfg.api_key}"

        payload = {"records": [_to_jsonable(r) for r in records]}

        _log(f"SIS2(HTTP): POST {url} (records={len(records)}) ...")
        r = requests.post(url, json=payload, headers=headers, timeout=cfg.timeout_sec)
        if not (200 <= r.status_code < 300):
            raise RuntimeError(f"SIS2 HTTP error {r.status_code}: {r.text[:300]}")
        _log(f"SIS2(HTTP): ok {r.status_code}")
        return {"ok": True, "mode": "http", "count": len(records), "status": r.status_code}

    if mode == "db":
        return _send_db(records, cfg, _log)

    raise ValueError(f"SIS2 mode inválido: {cfg.mode!r}. Usa 'file', 'http' o 'db'.")
def send_probe_to_sis2_db(cfg: Sis2Config, log: Optional[callable] = None) -> dict:
    """
    Inserta un registro de prueba controlado en Tb_PersonalAsistencia.
    No depende del reloj, solo valida: credenciales + red + driver + INSERT real.
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    try:
        import pyodbc
    except Exception:
        raise RuntimeError("pyodbc no está instalado. Ejecuta: pip install pyodbc")

    pwd = (cfg.db_password or os.environ.get("SIS2_DB_PASSWORD", "")).strip()
    if not cfg.db_server.strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.server en config.ini")
    if not cfg.db_username.strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.username en config.ini")
    if not pwd:
        raise RuntimeError("SIS2(DB): falta password. Usa env SIS2_DB_PASSWORD o sis2_db.password (no recomendado).")

    trust = "yes" if cfg.db_trust_server_certificate else "no"
    conn_str = (
        f"DRIVER={{{cfg.db_driver}}};"
        f"SERVER={cfg.db_server};"
        f"DATABASE={cfg.db_database};"
        f"UID={cfg.db_username};"
        f"PWD={pwd};"
        f"Encrypt=yes;"
        f"TrustServerCertificate={trust};"
        f"Connection Timeout={int(cfg.timeout_sec)};"
    )

    # Registro “de laboratorio”
    # IdPersonal=0 (dummy), Tipo='99' (marca de prueba), CodigoVerificador=0 (como el viejo)
    sql_probe = """
    DECLARE @ts DATETIME = GETDATE();
    INSERT INTO dba_mchs.Tb_PersonalAsistencia (IdPersonal, Asistencia, Tipo, CodigoVerificador)
    OUTPUT INSERTED.IdAsistencia, @ts
    VALUES (?, @ts, ?, ?);
    """

    _log(f"SIS2(DB-PROBE): conectando a {cfg.db_server} / {cfg.db_database} ...")
    cn = pyodbc.connect(conn_str, autocommit=False)
    try:
        cur = cn.cursor()
        cur.execute(sql_probe, (0, "99", 0))
        row = cur.fetchone()
        cn.commit()

        probe_id = int(row[0])
        probe_ts = row[1]  # datetime
        _log(f"SIS2(DB-PROBE): OK IdAsistencia={probe_id} ts={probe_ts}")
        return {"ok": True, "mode": "db", "probe_id": probe_id, "probe_ts": probe_ts.isoformat(sep=" ")}

    except Exception:
        cn.rollback()
        raise
    finally:
        cn.close()
