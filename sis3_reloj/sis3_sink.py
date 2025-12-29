# sis3_reloj/sis3_sink.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
import time
from datetime import datetime

try:
    import requests
except Exception:
    requests = None


@dataclass(frozen=True)
class Sis3Config:
    base_url: str
    api_key: str
    timeout_sec: int = 20


def send_attendance_to_sis3(
    records: list,
    cfg: Sis3Config,
    *,
    device_ip: str,
    device_port: int,
    file_tag: str,
    mode: str = "incremental",
    log: Optional[callable] = None
) -> dict:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if requests is None:
        raise RuntimeError("requests no está instalado. Ejecuta: pip install requests")

    if not cfg.base_url.strip():
        raise RuntimeError("SIS3: falta base_url")
    if not cfg.api_key.strip():
        raise RuntimeError("SIS3: falta api_key")

    url = cfg.base_url.rstrip("/") + "/api/checador/asistencias"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {cfg.api_key}",
    }

    payload = {
        "device": {"ip": device_ip, "port": int(device_port)},
        "batch": {
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "sis3_reloj",
            "mode": mode,
            "file_tag": file_tag,
        },
        "records": [
            {
                "user_id": str(getattr(r, "user_id", "")),
                "timestamp": (
                    getattr(r, "timestamp", None).isoformat()
                    if hasattr(getattr(r, "timestamp", None), "isoformat")
                    else str(getattr(r, "timestamp", "") or "")
                ),
                "status": getattr(r, "status", None),
                "punch": getattr(r, "punch", None),
            }
            for r in records
        ],
    }

    _log(f"SIS3(HTTP): POST {url} (records={len(records)}) ...")
    res = requests.post(url, json=payload, headers=headers, timeout=int(cfg.timeout_sec))

    if not (200 <= res.status_code < 300):
        raise RuntimeError(f"SIS3 HTTP error {res.status_code}: {res.text[:300]}")

    j = res.json()
    if not j.get("ok"):
        raise RuntimeError(f"SIS3 respondió ok=false: {j}")

    inserted = int(j.get("inserted") or 0)
    skipped  = int(j.get("skipped") or 0)

    # Log técnico (para ti)
    _log(f"SIS3(HTTP): ok inserted={inserted} skipped={skipped}")

    # Log humano (para operación)
    if inserted == 0 and skipped > 0:
        _log("SIS3(HTTP): Nota → SIS3 ya tenía estas checadas; no se duplicó nada.")
    elif inserted > 0 and skipped > 0:
        _log("SIS3(HTTP): Nota → Se insertaron nuevas checadas y otras ya existían (deduplicación).")
    elif inserted > 0 and skipped == 0:
        _log("SIS3(HTTP): OK → Checadas nuevas registradas en SIS3.")
    else:
        _log("SIS3(HTTP): OK → Sin cambios (0 nuevas, 0 repetidas).")

    # También regresamos un resumen por si quieres usarlo en UI después
    j["inserted"] = inserted
    j["skipped"] = skipped
    j["human"] = (
        "SIS3 ya tenía estas checadas; no se duplicó nada."
        if inserted == 0 and skipped > 0
        else
        "Se registraron checadas nuevas en SIS3."
        if inserted > 0
        else
        "Sin cambios."
    )

    return j



def probe_sis3(cfg: Sis3Config, *, log: Optional[callable] = None) -> dict:
    """
    Probe real contra SIS3:
    - pega al endpoint /api/checador/asistencias
    - manda 0 records (no inserta nada)
    - valida auth + reachability + JSON ok
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    if requests is None:
        raise RuntimeError("requests no está instalado. Ejecuta: pip install requests")

    if not cfg.base_url.strip():
        raise RuntimeError("SIS3: falta base_url")
    if not cfg.api_key.strip():
        raise RuntimeError("SIS3: falta api_key")

    url = cfg.base_url.rstrip("/") + "/api/checador/asistencias"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {cfg.api_key}",
    }
    # En Laravel te está marcando 422 porque "records" es obligatorio.
    # Mandamos 1 record dummy válido para pasar validación.
    payload = {
        "device": {"ip": "0.0.0.0", "port": 0},
        "batch": {
            "created_at": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "source": "sis3_reloj_probe",
            "mode": "probe",
            "file_tag": "probe",
        },
        "records": [
            {
                "user_id": "__probe__",          # string permitido
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "status": 0,                     # int válido
                "punch": 0,                      # int válido
            }
        ],
    }

    _log(f"SIS3(PROBE): POST {url} (records=1) ...")
    res = requests.post(url, json=payload, headers=headers, timeout=int(cfg.timeout_sec))

    if not (200 <= res.status_code < 300):
        raise RuntimeError(f"SIS3 PROBE HTTP error {res.status_code}: {res.text[:300]}")

    j = res.json()
    if not j.get("ok"):
        raise RuntimeError(f"SIS3 PROBE respondió ok=false: {j}")

    _log("SIS3(PROBE): OK (con record dummy para pasar validación)")
    return j
