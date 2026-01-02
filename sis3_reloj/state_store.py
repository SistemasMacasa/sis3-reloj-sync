# sis3_reloj/state_store.py
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Any, Dict


APP_NAME = "sis3-reloj"
STATE_SCHEMA_VERSION = 1


# ───────────────────────────────────────────────────────────────
# Modelo (view por kind)
# ───────────────────────────────────────────────────────────────
@dataclass
class State:
    """
    State VIEW por target (sis2/sis3):
      - last_ok_ts: último timestamp confirmado como entregado al destino.
      - kind: 'sis2' o 'sis3' (se infiere desde el path)
    """
    kind: str
    last_ok_ts: Optional[datetime] = None


# ───────────────────────────────────────────────────────────────
# Helpers datetime
# ───────────────────────────────────────────────────────────────
def _parse_dt(value: str | None) -> Optional[datetime]:
    v = (value or "").strip()
    if not v:
        return None
    try:
        # soporta "YYYY-MM-DDTHH:MM:SS"
        return datetime.fromisoformat(v)
    except Exception:
        return None


def _dt_to_str(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    try:
        return dt.isoformat(timespec="seconds")
    except Exception:
        return dt.isoformat()


# ───────────────────────────────────────────────────────────────
# Paths
# ───────────────────────────────────────────────────────────────
def get_app_state_dir() -> Path:
    """
    Directorio persistente (onefile-friendly) para guardar estado/checkpoints.
    """
    # Windows
    if os.name == "nt":
        base = (
            os.getenv("LOCALAPPDATA")
            or os.getenv("APPDATA")
            or str(Path.home() / "AppData" / "Local")
        )
        return (Path(base) / APP_NAME / "state").resolve()

    # macOS
    if sys.platform == "darwin":
        return (Path.home() / "Library" / "Application Support" / APP_NAME / "state").resolve()

    # Linux/Unix
    xdg_state = os.getenv("XDG_STATE_HOME")
    if xdg_state:
        return (Path(xdg_state) / APP_NAME / "state").resolve()

    return (Path.home() / ".local" / "state" / APP_NAME / "state").resolve()


def get_state_path(kind: str) -> Path:
    """
    Path “VIEW” por target (se conserva para compatibilidad y logs).
    kind: 'sis2' o 'sis3'
    """
    k = (kind or "").strip().lower()
    if k not in ("sis2", "sis3"):
        raise ValueError(f"kind inválido para state: {kind!r}. Usa 'sis2' o 'sis3'.")
    d = get_app_state_dir() / k
    d.mkdir(parents=True, exist_ok=True)
    return d / "state.json"


def get_unified_state_path() -> Path:
    """
    Único archivo canónico de estado (checkpoint unificado).
    """
    d = get_app_state_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / "state.json"


def _infer_kind_from_path(path: Path) -> str:
    """
    Inferimos el kind desde el parent: .../state/sis2/state.json => 'sis2'
    """
    try:
        k = (path.parent.name or "").strip().lower()
        if k in ("sis2", "sis3"):
            return k
    except Exception:
        pass
    return "sis3"  # fallback razonable


# ───────────────────────────────────────────────────────────────
# IO JSON atómico
# ───────────────────────────────────────────────────────────────
def _read_json(path: Path) -> Dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    j = json.loads(raw or "{}")
    return j if isinstance(j, dict) else {}


def _atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _default_unified() -> Dict[str, Any]:
    return {
        "version": STATE_SCHEMA_VERSION,
        "targets": {
            "sis2": {"last_ok_ts": None},
            "sis3": {"last_ok_ts": None},
        },
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }


def _ensure_unified_shape(j: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(j, dict):
        j = {}
    if "targets" not in j or not isinstance(j.get("targets"), dict):
        j["targets"] = {}
    for k in ("sis2", "sis3"):
        if k not in j["targets"] or not isinstance(j["targets"].get(k), dict):
            j["targets"][k] = {}
        j["targets"][k].setdefault("last_ok_ts", None)
    j.setdefault("version", STATE_SCHEMA_VERSION)
    j["saved_at"] = datetime.now().isoformat(timespec="seconds")
    return j


# ───────────────────────────────────────────────────────────────
# API compatible: load_state(path) / save_state(path, state)
# ───────────────────────────────────────────────────────────────
def load_state(path: Path) -> State:
    """
    Lee estado del UNIFICADO y devuelve la vista del target inferido desde `path`.
    Si no existe el unificado, intenta migrar desde el viejo per-kind.
    """
    path = Path(path)
    kind = _infer_kind_from_path(path)
    unified_path = get_unified_state_path()

    # 1) Si existe unificado, se usa como fuente de verdad
    if unified_path.exists():
        try:
            uj = _ensure_unified_shape(_read_json(unified_path))
            last_ok = _parse_dt(uj.get("targets", {}).get(kind, {}).get("last_ok_ts"))
            return State(kind=kind, last_ok_ts=last_ok)
        except Exception:
            # unificado corrupto -> no truena, cae a vacío
            return State(kind=kind, last_ok_ts=None)

    # 2) No existe unificado -> migración desde per-kind (legacy)
    #    - Si hay un state.json viejo con last_ok_ts, lo migramos a unificado.
    if path.exists():
        try:
            lj = _read_json(path)
            last_ok = _parse_dt(lj.get("last_ok_ts"))
            uj = _default_unified()
            uj = _ensure_unified_shape(uj)
            uj["targets"][kind]["last_ok_ts"] = _dt_to_str(last_ok)
            _atomic_write_json(unified_path, uj)
            # re-escribe view (puntero)
            _write_view_file(path, kind, last_ok, unified_path)
            return State(kind=kind, last_ok_ts=last_ok)
        except Exception:
            return State(kind=kind, last_ok_ts=None)

    # 3) Nada existe
    return State(kind=kind, last_ok_ts=None)


def save_state(path: Path, state: State) -> None:
    """
    Guarda en unificado (canónico) y re-escribe el view file en `path`.
    """
    path = Path(path)
    kind = (getattr(state, "kind", None) or _infer_kind_from_path(path) or "sis3").strip().lower()
    if kind not in ("sis2", "sis3"):
        kind = "sis3"

    unified_path = get_unified_state_path()

    # Lee unificado existente si está, si no crea default
    try:
        uj = _read_json(unified_path) if unified_path.exists() else _default_unified()
    except Exception:
        uj = _default_unified()

    uj = _ensure_unified_shape(uj)
    uj["targets"][kind]["last_ok_ts"] = _dt_to_str(state.last_ok_ts)
    uj["saved_at"] = datetime.now().isoformat(timespec="seconds")
    _atomic_write_json(unified_path, uj)

    # Escribe view/puntero para compatibilidad y logs
    _write_view_file(path, kind, state.last_ok_ts, unified_path)


def _write_view_file(view_path: Path, kind: str, last_ok_ts: Optional[datetime], unified_path: Path) -> None:
    """
    Archivo view (por-kind) para:
      - que exista el path esperado por la UI/pipeline
      - que el log tenga una ruta estable
      - pero dejando claro que el canónico es el unificado
    """
    payload = {
        "_note": "VIEW FILE (no es la fuente de verdad). El canónico es el state unificado.",
        "kind": kind,
        "last_ok_ts": _dt_to_str(last_ok_ts),
        "unified_path": str(unified_path),
        "saved_at": datetime.now().isoformat(timespec="seconds"),
    }
    _atomic_write_json(view_path, payload)
