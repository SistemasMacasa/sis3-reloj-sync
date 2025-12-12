# sis3_reloj/zk_client.py
from zk import ZK
from typing import List
from datetime import datetime

class UserRecord:
    def __init__(self, user_id, name, privilege, card, password, enabled):
        self.user_id = str(user_id)
        self.name = name or ""
        self.privilege = int(privilege) if privilege is not None else 0
        self.card = str(card) if card not in (None, "") else ""
        self.password = str(password) if password not in (None, "") else ""
        self.enabled = bool(enabled)

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "name": self.name,
            "privilege": self.privilege,
            "card": self.card,
            "password": self.password,
            "enabled": self.enabled,
        }


class AttendanceRecord:
    def __init__(self, user_id, status, punch, timestamp):
        self.user_id = str(user_id)
        self.status = int(status) if status is not None else None
        self.punch = int(punch) if punch is not None else None
        self.timestamp = timestamp  # datetime

    def to_dict(self):
        return {
            "user_id": self.user_id,
            "status": self.status,
            "punch": self.punch,
            "timestamp": self.timestamp.isoformat(),
        }

def read_attendance(ip: str, port: int, password: int) -> List[AttendanceRecord]:
    zk = ZK(
        ip,
        port=port,
        timeout=5,
        password=password,
        force_udp=False,
    )
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()

        attendances = conn.get_attendance() or []
        result = []
        for att in attendances:
            ts = att.timestamp
            if not isinstance(ts, datetime):
                continue
            result.append(
                AttendanceRecord(
                    user_id=att.user_id,
                    status=att.status,
                    punch=att.punch,
                    timestamp=ts,
                )
            )

        conn.enable_device()
        conn.disconnect()
        return result

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass

def read_users(ip: str, port: int, password: int) -> list[UserRecord]:
    """
    Lee usuarios del checador (equivalente a Tb_Personal).
    """
    zk = ZK(
        ip,
        port=port,
        timeout=5,
        password=password,
        force_udp=False,
    )
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()

        raw_users = conn.get_users() or []
        users: list[UserRecord] = []

        # DEBUG temporal: volcar el objeto crudo a un archivo
        from pathlib import Path
        import json
        debug_path = Path(__file__).resolve().parent.parent / "out" / "usuarios_raw.txt"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w", encoding="utf-8") as fdbg:
            for u in raw_users:
                fdbg.write(repr(u.__dict__) + "\n")


        for u in raw_users:
            # pyzk típicamente expone: user_id, name, privilege, card, password, enabled
            users.append(
                UserRecord(
                    user_id=u.user_id,
                    name=getattr(u, "name", ""),
                    privilege=getattr(u, "privilege", 0),
                    card=getattr(u, "card", ""),
                    password=getattr(u, "password", ""),
                    enabled=getattr(u, "enabled", True),
                )
            )

        conn.enable_device()
        conn.disconnect()
        return users

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass


def update_user_name(ip: str, port: int, password: int, target_user_id: str, new_name: str) -> bool:
    """
    Actualiza solo el 'name' de un usuario en el checador, manteniendo user_id, password, etc.
    Devuelve True si encontró y actualizó, False si no encontró el user_id.
    """
    zk = ZK(
        ip,
        port=port,
        timeout=5,
        password=password,
        force_udp=False,
    )
    conn = None
    try:
        conn = zk.connect()
        conn.disable_device()

        raw_users = conn.get_users() or []
        target = None
        for u in raw_users:
            if str(getattr(u, "user_id", "")) == str(target_user_id):
                target = u
                break

        if not target:
            return False

        # Usamos mismos valores que ya tiene el usuario, salvo el 'name'
        uid = getattr(target, "uid", None)
        privilege = getattr(target, "privilege", 0)
        pwd = getattr(target, "password", "")
        group_id = getattr(target, "group_id", "")
        card = getattr(target, "card", 0)
        user_id = getattr(target, "user_id", "")

        # Llamada al SDK pyzk – firma típica:
        # set_user(uid, name, privilege, password, group_id, user_id, card)
        conn.set_user(
            uid=uid,
            name=new_name,
            privilege=privilege,
            password=pwd,
            group_id=group_id,
            user_id=user_id,
            card=card,
        )

        conn.enable_device()
        conn.disconnect()
        return True

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass
