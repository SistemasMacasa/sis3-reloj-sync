# SIS3RelojChecador

Cliente de escritorio en Python para integrar el reloj checador ZKTeco MB160 con SIS3.

## Funciones actuales

- Leer registros de **asistencia** desde el reloj y guardarlos en `out/asistencia-YYYYMMDD-HHMMSS.jsonl`.
- Leer **usuarios** del reloj y guardarlos en `out/usuarios-YYYYMMDD-HHMMSS.jsonl`.
- Interfaz gráfica simple (Tkinter) para:
  - Configurar IP, puerto y password del reloj.
  - Cambiar entre modo "SIS2 activo" y "SIS2 desconectado" (solo flag por ahora).
  - Ejecutar lectura de usuarios y asistencias.

## Estructura

- `main.py` → punto de entrada, lanza la GUI.
- `sis3_reloj/`
  - `config.py` → lectura/escritura de `config.ini`.
  - `zk_client.py` → acceso al reloj (pyzk).
  - `file_sink.py` → escritura de archivos JSONL en `out/`.
  - `gui.py` → interfaz Tkinter.

## Configuración

1. Crear entorno virtual (opcional pero recomendado):

   ```bash
   python -m venv .venv
