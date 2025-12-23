# sis3_reloj/zk_client.py
from zk import ZK
from typing import List, Optional
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


def _connect(ip: str, port: int, password: int):
    zk = ZK(
        ip,
        port=port,
        timeout=5,
        password=password,
        force_udp=False,
    )
    conn = zk.connect()
    return conn


def read_attendance(ip: str, port: int, password: int) -> List[AttendanceRecord]:
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        attendances = conn.get_attendance() or []
        result: List[AttendanceRecord] = []
        for att in attendances:
            ts = getattr(att, "timestamp", None)
            if not isinstance(ts, datetime):
                continue
            result.append(
                AttendanceRecord(
                    user_id=getattr(att, "user_id", ""),
                    status=getattr(att, "status", None),
                    punch=getattr(att, "punch", None),
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
    Lee usuarios del checador.
    """
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        raw_users = conn.get_users() or []
        users: list[UserRecord] = []

        # DEBUG temporal: volcar el objeto crudo a un archivo
        from pathlib import Path
        debug_path = Path(__file__).resolve().parent.parent / "out" / "usuarios_raw.txt"
        debug_path.parent.mkdir(parents=True, exist_ok=True)
        with debug_path.open("w", encoding="utf-8") as fdbg:
            for u in raw_users:
                fdbg.write(repr(getattr(u, "__dict__", {})) + "\n")

        for u in raw_users:
            users.append(
                UserRecord(
                    user_id=getattr(u, "user_id", ""),
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
    Actualiza solo el 'name' de un usuario.
    """
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        raw_users = conn.get_users() or []
        target = None
        for u in raw_users:
            if str(getattr(u, "user_id", "")) == str(target_user_id):
                target = u
                break

        if not target:
            return False

        uid = getattr(target, "uid", None)
        privilege = getattr(target, "privilege", 0)
        pwd = getattr(target, "password", "")
        group_id = getattr(target, "group_id", "")
        card = getattr(target, "card", 0)
        user_id = getattr(target, "user_id", "")

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


def clear_attendance(ip: str, port: int, password: int) -> bool:
    """
    Borra logs de asistencia del dispositivo (equivalente a ClearGLog).
    """
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        # Método típico en librería zk
        conn.clear_attendance()

        conn.enable_device()
        conn.disconnect()
        return True

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass


def upsert_user(
    ip: str,
    port: int,
    password: int,
    *,
    user_id: str,
    name: str,
    privilege: int = 0,
    user_password: str = "",
    card: int | str = 0,
    enabled: bool = True,
) -> bool:
    """
    Crea o actualiza un usuario en el dispositivo.
    Se basa en set_user(...) buscando si ya existe para reutilizar uid/group_id.
    """
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        raw_users = conn.get_users() or []
        target = None
        for u in raw_users:
            if str(getattr(u, "user_id", "")) == str(user_id):
                target = u
                break

        uid: Optional[int] = getattr(target, "uid", None) if target else None
        group_id = getattr(target, "group_id", "") if target else ""

        # Normalizaciones
        try:
            privilege = int(privilege)
        except Exception:
            privilege = 0

        if card in (None, ""):
            card_val = 0
        else:
            try:
                card_val = int(card)
            except Exception:
                # Si llega como string no-numérico, lo dejamos en 0 para no romper
                card_val = 0

        conn.set_user(
            uid=uid,
            name=str(name or ""),
            privilege=privilege,
            password=str(user_password or ""),
            group_id=group_id,
            user_id=str(user_id),
            card=card_val,
        )

        # Nota: enabled no siempre se aplica por set_user en todas las variantes.
        # Si tu SDK soporta enable/disable por otro método, lo añadimos después.

        conn.enable_device()
        conn.disconnect()
        return True

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass


def delete_user(ip: str, port: int, password: int, *, user_id: str) -> bool:
    """
    Elimina un usuario en el dispositivo.
    Intentamos por uid (más común). Si no se puede, intentamos por user_id.
    """
    conn = None
    try:
        conn = _connect(ip, port, password)
        conn.disable_device()

        raw_users = conn.get_users() or []
        target = None
        for u in raw_users:
            if str(getattr(u, "user_id", "")) == str(user_id):
                target = u
                break

        if not target:
            # Ya no existe; lo consideramos OK (idempotente)
            conn.enable_device()
            conn.disconnect()
            return True

        uid = getattr(target, "uid", None)

        # Algunas versiones:
        # - delete_user(uid=...)
        # - delete_user(user_id=...)
        deleted = False
        try:
            if uid is not None:
                conn.delete_user(uid=uid)
                deleted = True
        except Exception:
            deleted = False

        if not deleted:
            try:
                conn.delete_user(user_id=str(user_id))
                deleted = True
            except Exception:
                deleted = False

        conn.enable_device()
        conn.disconnect()
        return deleted

    finally:
        if conn:
            try:
                conn.disconnect()
            except Exception:
                pass
