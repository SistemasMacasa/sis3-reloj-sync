# change_user_name.py
from sis3_reloj.config import load_config
from sis3_reloj.zk_client import update_user_name

def main():
    config = load_config()

    # ⚠️ Ajusta estos valores para la prueba
    target_user_id = "111"
    new_name = "ELIEZER ALBERTO GOMEZ CERVANTES"

    print(f"Conectando a {config.ip}:{config.port} para actualizar user_id={target_user_id}...")
    ok = update_user_name(
        ip=config.ip,
        port=config.port,
        password=config.password,
        target_user_id=target_user_id,
        new_name=new_name,
    )

    if ok:
        print(f"✅ Nombre de usuario {target_user_id} actualizado a: {new_name}")
    else:
        print(f"⚠️ No se encontró user_id={target_user_id} en el dispositivo.")

if __name__ == "__main__":
    main()
