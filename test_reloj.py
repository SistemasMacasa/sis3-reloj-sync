from zk import ZK, const
from datetime import datetime

# === CONFIGURACIÓN DEL RELOJ ===
RELOJ_IP = "192.168.1.145"
RELOJ_PORT = 4370
RELOJ_PASSWORD = 0  # Cambia esto si en el equipo configuraron Comm Key ≠ 0

def main():
    zk = ZK(
        RELOJ_IP,
        port=RELOJ_PORT,
        timeout=5,
        password=RELOJ_PASSWORD,
        force_udp=False  # Si da timeout, luego probamos con True
    )

    conn = None
    try:
        print(f"Conectando a {RELOJ_IP}:{RELOJ_PORT} ...")
        conn = zk.connect()
        print("✅ Conectado al reloj")

        # Opcional: deshabilita el equipo mientras lees (evita bloqueos)
        conn.disable_device()

        # Leer todos los registros de asistencia
        print("Leyendo registros de asistencia...")
        attendances = conn.get_attendance() or []

        print(f"Total registros encontrados: {len(attendances)}")
        print("-" * 60)

        # Imprime solo los primeros 20 para prueba
        for i, att in enumerate(attendances[:20], start=1):
            # att.timestamp ya viene como datetime
            ts = att.timestamp if isinstance(att.timestamp, datetime) else att.timestamp
            print(
                f"{i:03d} | user_id={att.user_id} | estado={att.status} | "
                f"punch={att.punch} | fecha={ts}"
            )

        # Vuelve a habilitar el equipo
        conn.enable_device()
        conn.disconnect()
        print("🔌 Desconectado del reloj")

    except Exception as e:
        print("❌ Error al conectar o leer del reloj:", e)
        if conn:
            conn.disconnect()

if __name__ == "__main__":
    main()
