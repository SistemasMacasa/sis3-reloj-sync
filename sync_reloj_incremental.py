from zk import ZK
from datetime import datetime
import json
from pathlib import Path

RELOJ_IP = "192.168.1.145"
RELOJ_PORT = 4370
RELOJ_PASSWORD = 0  # lo cambias si te dan un Comm Key distinto de 0

BASE_DIR = Path(__file__).resolve().parent
STATE_FILE = BASE_DIR / "last_sync.json"

def load_last_timestamp():
    if not STATE_FILE.exists():
        return None

    try:
        data = json.loads(STATE_FILE.read_text(encoding="utf-8"))
        ts_str = data.get("last_timestamp")
        if not ts_str:
            return None
        # 2025-12-11T09:51:06
        return datetime.fromisoformat(ts_str)
    except Exception:
        return None

def save_last_timestamp(ts: datetime):
    data = {"last_timestamp": ts.isoformat()}
    STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")

def main():
    zk = ZK(
        RELOJ_IP,
        port=RELOJ_PORT,
        timeout=5,
        password=RELOJ_PASSWORD,
        force_udp=False,
    )

    last_ts = load_last_timestamp()
    print("Último timestamp sincronizado:", last_ts)

    conn = None
    try:
        print(f"Conectando a {RELOJ_IP}:{RELOJ_PORT} ...")
        conn = zk.connect()
        print("✅ Conectado al reloj")

        conn.disable_device()
        attendances = conn.get_attendance() or []
        print(f"Total registros en el reloj: {len(attendances)}")

        # Filtrar solo los nuevos
        nuevos = []
        max_ts = last_ts

        for att in attendances:
            ts = att.timestamp
            if not isinstance(ts, datetime):
                continue

            if (last_ts is None) or (ts > last_ts):
                nuevos.append(att)
                if (max_ts is None) or (ts > max_ts):
                    max_ts = ts

        print(f"Registros nuevos detectados: {len(nuevos)}")
        print("-" * 60)

        for i, att in enumerate(sorted(nuevos, key=lambda a: a.timestamp)[:50], start=1):
            print(
                f"{i:03d} | user_id={att.user_id} | status={att.status} | "
                f"punch={att.punch} | fecha={att.timestamp}"
            )

        # NO borramos nada del reloj.
        conn.enable_device()
        conn.disconnect()
        print("🔌 Desconectado del reloj")

        # Si hubo nuevos, actualizamos el puntero
        if max_ts and (last_ts is None or max_ts > last_ts):
            save_last_timestamp(max_ts)
            print("📌 Actualizado last_sync.json con:", max_ts)

    except Exception as e:
        print("❌ Error en la sincronización:", e)
        if conn:
            conn.disconnect()

if __name__ == "__main__":
    main()
