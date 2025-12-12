# sis3_reloj/sis2_sink.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, date
from pathlib import Path
from typing import Any, Iterable, Optional
import json
import time

try:
    import requests  # opcional si mode=http
except Exception:
    requests = None


@dataclass(frozen=True)
class Sis2Config:
    enabled: bool
    mode: str  # "file" | "http"
    drop_dir: Path
    base_url: str
    api_key: str
    timeout_sec: int


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

    raise ValueError(f"SIS2 mode inválido: {cfg.mode!r}. Usa 'file' o 'http'.")

