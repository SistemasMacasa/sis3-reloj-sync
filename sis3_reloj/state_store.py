from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json


@dataclass
class Sis2State:
    last_ok_ts: datetime | None = None


def load_state(path: Path) -> Sis2State:
    if not path.exists():
        return Sis2State()

    try:
        data = json.loads(path.read_text(encoding="utf-8") or "{}")
        ts = data.get("last_ok_ts")
        if ts:
            return Sis2State(last_ok_ts=datetime.fromisoformat(ts))
        return Sis2State()
    except Exception:
        # Si el state se corrompe, no bloqueamos: arrancamos “sin state”
        return Sis2State()


def save_state(path: Path, state: Sis2State) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_ok_ts": state.last_ok_ts.isoformat() if state.last_ok_ts else None,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
