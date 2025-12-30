# sis3_reloj/sis3_sink.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Callable, Any
from datetime import datetime

try:
    import requests
    from requests import Response
except Exception:
    requests = None
    Response = Any


@dataclass(frozen=True)
class Sis3Config:
    base_url: str
    api_key: str
    timeout_sec: int = 20


def _ensure_requests() -> None:
    if requests is None:
        raise RuntimeError("requests no está instalado. Ejecuta: pip install requests")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _headers(cfg: Sis3Config) -> dict:
    """
    Tu middleware acepta:
      - Authorization: Bearer <TOKEN>
      - o X-API-Key: <TOKEN>

    Enviamos ambos por robustez (no rompe nada).
    """
    token = (cfg.api_key or "").strip()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
        "X-API-Key": token,
        "X-Client": "sis3_reloj",
    }


def _safe_json(res: Response) -> dict:
    try:
        return res.json()
    except Exception:
        txt = (getattr(res, "text", "") or "")[:800]
        raise RuntimeError(f"Respuesta no-JSON (HTTP {res.status_code}): {txt}")


def _post_json(
    url: str,
    *,
    headers: dict,
    payload: dict,
    timeout_sec: int,
    log: Optional[Callable[[str], None]] = None,
) -> Response:
    if log:
        log(f"SIS3(HTTP): POST {url}")

    try:
        return requests.post(url, json=payload, headers=headers, timeout=int(timeout_sec))
    except requests.exceptions.Timeout:
        raise RuntimeError(f"SIS3 timeout ({timeout_sec}s) al llamar {url}")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"SIS3 connection error al llamar {url}: {e}")
    except Exception as e:
        raise RuntimeError(f"SIS3 error HTTP al llamar {url}: {e}")


def send_attendance_to_sis3(
    records: list,
    cfg: Sis3Config,
    *,
    device_ip: str,
    device_port: int,
    file_tag: str,
    mode: str = "incremental",
    log: Optional[Callable[[str], None]] = None,
) -> dict:
    def _log(msg: str) -> None:
        if log:
            log(msg)

    _ensure_requests()

    if not (cfg.base_url or "").strip():
        raise RuntimeError("SIS3: falta base_url")
    if not (cfg.api_key or "").strip():
        raise RuntimeError("SIS3: falta api_key")

    url = cfg.base_url.rstrip("/") + "/api/checador/asistencias"
    headers = _headers(cfg)

    # Tu API solo valida batch.file_tag y records[*], pero no le molesta recibir campos extra.
    payload = {
        "device": {"ip": device_ip, "port": int(device_port)},
        "batch": {
            "created_at": _now_iso(),
            "source": "sis3_reloj",
            "mode": mode,
            "file_tag": file_tag,
        },
        "records": [
            {
                "user_id": str(getattr(r, "user_id", "")),
                "timestamp": (
                    getattr(r, "timestamp", None).isoformat(timespec="seconds")
                    if hasattr(getattr(r, "timestamp", None), "isoformat")
                    else str(getattr(r, "timestamp", "") or "")
                ),
                "status": getattr(r, "status", None),
                "punch": getattr(r, "punch", None),
            }
            for r in records
        ],
    }

    _log(f"SIS3(HTTP): Enviando asistencias (records={len(records)}) ...")
    res = _post_json(url, headers=headers, payload=payload, timeout_sec=cfg.timeout_sec, log=log)

    if not (200 <= res.status_code < 300):
        # intenta JSON, si no, texto
        try:
            j_err = _safe_json(res)
            raise RuntimeError(f"SIS3 HTTP {res.status_code}: {str(j_err)[:800]}")
        except Exception:
            txt = (res.text or "")[:800]
            raise RuntimeError(f"SIS3 HTTP {res.status_code}: {txt}")

    j = _safe_json(res)

    if not j.get("ok"):
        raise RuntimeError(f"SIS3 respondió ok=false: {str(j)[:800]}")

    inserted = int(j.get("inserted") or 0)
    skipped = int(j.get("skipped") or 0)

    _log(f"SIS3(HTTP): ok received={j.get('received')} inserted={inserted} skipped={skipped}")

    # Log humano (operación)
    if inserted == 0 and skipped > 0:
        human = "SIS3 ya tenía estas checadas; no se duplicó nada."
        _log("SIS3(HTTP): Nota → SIS3 ya tenía estas checadas; no se duplicó nada.")
    elif inserted > 0 and skipped > 0:
        human = "Se insertaron checadas nuevas y otras ya existían (deduplicación)."
        _log("SIS3(HTTP): Nota → Se insertaron nuevas checadas y otras ya existían (deduplicación).")
    elif inserted > 0 and skipped == 0:
        human = "Checadas nuevas registradas en SIS3."
        _log("SIS3(HTTP): OK → Checadas nuevas registradas en SIS3.")
    else:
        human = "Sin cambios."
        _log("SIS3(HTTP): OK → Sin cambios (0 nuevas, 0 repetidas).")

    j["inserted"] = inserted
    j["skipped"] = skipped
    j["human"] = human
    return j


def probe_sis3(cfg: Sis3Config, *, log: Optional[Callable[[str], None]] = None) -> dict:
    """
    Probe SIN ENSUCIAR BD, compatible con tu API actual (solo POST y records min:1).

    Estrategia:
      - Hacemos POST al endpoint real con payload *intencionalmente inválido*:
            records: []
        Esto:
          - pasa el middleware (token OK),
          - entra al controller,
          - falla en validate() con 422,
          - NO inserta nada.

    Criterios:
      - 422 => PROBE OK (reachable + auth OK)
      - 401 => token inválido
      - 500 => API key no configurada u error interno
      - otros => error
    """
    def _log(msg: str) -> None:
        if log:
            log(msg)

    _ensure_requests()

    if not (cfg.base_url or "").strip():
        raise RuntimeError("SIS3: falta base_url")
    if not (cfg.api_key or "").strip():
        raise RuntimeError("SIS3: falta api_key")

    url = cfg.base_url.rstrip("/") + "/api/checador/asistencias"
    headers = _headers(cfg)

    # Payload que dispara 422 (records min:1) sin insertar nada.
    payload = {
        "batch": {"file_tag": "probe"},
        "records": [],
    }

    _log(f"SIS3(PROBE): POST {url} (payload inválido para forzar 422, sin insertar) ...")
    res = _post_json(url, headers=headers, payload=payload, timeout_sec=cfg.timeout_sec, log=log)

    if res.status_code == 422:
        # Esto confirma: auth OK + controller alcanzable.
        try:
            j = _safe_json(res)
        except Exception:
            j = {"ok": True, "probe": "validation_422", "status_code": 422}

        _log("SIS3(PROBE): OK (422 esperado: validación falló, pero auth + reachability OK).")
        return {
            "ok": True,
            "probe": "validation_422",
            "status_code": 422,
            "details": j,
        }

    if res.status_code == 401:
        # middleware rechazó
        try:
            j = _safe_json(res)
            msg = j.get("message") or "No autorizado"
        except Exception:
            msg = "No autorizado"
        raise RuntimeError(f"SIS3 PROBE 401: {msg}")

    if res.status_code == 500:
        # tu middleware puede regresar 500 si falta API key configurada
        try:
            j = _safe_json(res)
            msg = j.get("message") or "Error interno"
        except Exception:
            msg = (res.text or "")[:300] or "Error interno"
        raise RuntimeError(f"SIS3 PROBE 500: {msg}")

    # Si responde 2xx aquí, significaría que cambió la validación o endpoint (raro con records=[]).
    if 200 <= res.status_code < 300:
        j = _safe_json(res)
        # OJO: si esto llegara a insertar, sería un bug del backend porque records=[] no debería pasar validate.
        if j.get("ok") is True:
            _log("SIS3(PROBE): OK (2xx). Nota: el backend aceptó el probe; revisa validación si no era esperado.")
            return j
        raise RuntimeError(f"SIS3 PROBE 2xx pero ok!=true: {str(j)[:800]}")

    # Otros códigos
    try:
        j = _safe_json(res)
        raise RuntimeError(f"SIS3 PROBE HTTP {res.status_code}: {str(j)[:800]}")
    except Exception:
        txt = (res.text or "")[:800]
        raise RuntimeError(f"SIS3 PROBE HTTP {res.status_code}: {txt}")
