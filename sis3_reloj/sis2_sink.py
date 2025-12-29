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
    db_driver: str = "ODBC Driver 18 for SQL Server"  # legacy (ya no se usa con pymssql)
    db_trust_server_certificate: bool = True         # legacy (ya no se usa con pymssql)


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


# ─────────────────────────────────────────────
# DB (sin ODBC): pymssql
# ─────────────────────────────────────────────

def _parse_server(server: str) -> tuple[str, int]:
    """
    Acepta:
      app.sismanagement.com.mx\\MSSQLSERVER2019,1434
      app.sismanagement.com.mx,1434
      app.sismanagement.com.mx\\INSTANCIA
      app.sismanagement.com.mx

    pymssql usa host + port (NO instancia). Si viene instancia, se ignora.
    """
    s = (server or "").strip()
    if not s:
        return "", 1433

    host = s
    port = 1433

    # puerto por coma
    if "," in s:
        left, right = s.rsplit(",", 1)
        host = left.strip()
        try:
            port = int(right.strip())
        except Exception:
            port = 1433

    # quitar \instancia
    if "\\" in host:
        host = host.split("\\", 1)[0].strip()

    return host, port


def _db_password(cfg: Sis2Config) -> str:
    # prioridad: cfg.db_password (config.ini) o ENV
    return (cfg.db_password or os.environ.get("SIS2_DB_PASSWORD", "") or "").strip()


def _require_db_cfg(cfg: Sis2Config):
    pwd = _db_password(cfg)

    if not (cfg.db_server or "").strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.server en config.ini")
    if not (cfg.db_username or "").strip():
        raise RuntimeError("SIS2(DB): falta sis2_db.username en config.ini")
    if not pwd:
        raise RuntimeError("SIS2(DB): falta password. Usa env SIS2_DB_PASSWORD o sis2_db.password (no recomendado).")

    return pwd


def _db_connect(cfg: Sis2Config):
    """
    Conecta con pymssql (sin drivers ODBC instalados).
    """
    try:
        import pymssql
    except Exception:
        raise RuntimeError("pymssql no está instalado. Ejecuta: pip install pymssql")

    pwd = _require_db_cfg(cfg)
    host, port = _parse_server(cfg.db_server)

    # Nota: pymssql maneja timeout en connect/login_timeout.
    # timeout_sec aplica a login y operaciones.
    t = int(cfg.timeout_sec or 10)

    return pymssql.connect(
        server=host,
        port=port,
        user=str(cfg.db_username),
        password=pwd,
        database=str(cfg.db_database),
        login_timeout=t,
        timeout=t,
        charset="UTF-8",
    )


def _send_db(records: list, cfg: Sis2Config, _log: callable) -> dict:
    """
    Inserción idempotente: no duplicar (IdPersonal + Asistencia + Tipo + CodigoVerificador)
    Tablas canónicas: dba_mchs.Tb_PersonalAsistencia
    """
    sql_exists = """
    SELECT 1
    FROM dba_mchs.Tb_PersonalAsistencia WITH (NOLOCK)
    WHERE IdPersonal = %s
      AND Asistencia = %s
      AND Tipo = %s
      AND CodigoVerificador = %s
    """

    sql_insert = """
    INSERT INTO dba_mchs.Tb_PersonalAsistencia (IdPersonal, Asistencia, Tipo, CodigoVerificador)
    VALUES (%s, %s, %s, %s)
    """

    rows = []
    for r in records:
        user_id = int(getattr(r, "user_id"))
        ts = getattr(r, "timestamp")
        status = int(getattr(r, "status", 1) or 1)

        if not isinstance(ts, datetime):
            # soporta iso / Z
            ts = datetime.fromisoformat(str(ts).replace("Z", "+00:00")).replace(tzinfo=None)

        # Tipo en tabla es varchar(10)
        rows.append((user_id, ts, str(status), 0))

    inserted = 0
    skipped = 0

    _log(f"SIS2(DB): conectando a {cfg.db_server} / {cfg.db_database} ...")
    cn = _db_connect(cfg)
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
        try:
            cn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cn.close()
        except Exception:
            pass

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
    No depende del reloj, solo valida: credenciales + red + INSERT real.
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    # Registro “de laboratorio”
    # IdPersonal=0 (dummy), Tipo='99' (marca de prueba), CodigoVerificador=0
    # Nota: con pymssql no usamos DECLARE/OUTPUT (para evitar temas de compatibilidad),
    #       hacemos INSERT y luego consultamos el último IdAsistencia para IdPersonal=0 Tipo='99'.
    sql_insert = """
    INSERT INTO dba_mchs.Tb_PersonalAsistencia (IdPersonal, Asistencia, Tipo, CodigoVerificador)
    VALUES (%s, GETDATE(), %s, %s)
    """

    sql_last = """
    SELECT TOP 1 IdAsistencia, Asistencia
    FROM dba_mchs.Tb_PersonalAsistencia WITH (NOLOCK)
    WHERE IdPersonal = %s AND Tipo = %s AND CodigoVerificador = %s
    ORDER BY IdAsistencia DESC
    """

    _log(f"SIS2(DB-PROBE): conectando a {cfg.db_server} / {cfg.db_database} ...")
    cn = _db_connect(cfg)
    try:
        cur = cn.cursor()
        cur.execute(sql_insert, (0, "99", 0))
        cn.commit()

        cur.execute(sql_last, (0, "99", 0))
        row = cur.fetchone()

        if row:
            probe_id = int(row[0])
            probe_ts = row[1]
            # probe_ts puede venir como datetime
            ts_str = probe_ts.isoformat(sep=" ") if hasattr(probe_ts, "isoformat") else str(probe_ts)
            _log(f"SIS2(DB-PROBE): OK IdAsistencia={probe_id} ts={ts_str}")
            return {"ok": True, "mode": "db", "probe_id": probe_id, "probe_ts": ts_str}

        _log("SIS2(DB-PROBE): OK (insert) pero no pude leer IdAsistencia (consulta vacía).")
        return {"ok": True, "mode": "db", "probe_id": None}

    except Exception:
        try:
            cn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            cn.close()
        except Exception:
            pass
