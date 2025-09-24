# Laboratorio 2 – ETL con Prefect (SIN Docker)

Este proyecto implementa el Lab 2 con **Prefect 2** (más simple que Airflow para curso). 
Incluye: pipeline ETL basado en tu Lab1, **logging**, **monitoreo en SQLite** (`etl_monitor`) y salida `df_final.parquet`.

## Estructura
```
prefect_lab2/
├─ flows/
│  └─ etl_lab2_prefect.py   # define @flow y @task
├─ data/
│  ├─ alumnos.csv           # pon aquí tus insumos reales
│  ├─ calificaciones.json
│  └─ matriculas.xml
└─ requirements.txt
```

## Instalación rápida
```bash
# 1) Crear y activar entorno (opcional pero recomendado)
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux:
source .venv/bin/activate

# 2) Instalar dependencias
pip install -r requirements.txt

# 3) (Opcional) Iniciar la UI de Prefect
prefect server start
# Abre http://localhost:4200

# 4) Ejecutar el flujo
python flows/etl_lab2_prefect.py
```

API Key: pnu_auTN1sBsXGCzbvuZwq3Xa2S3VzUpgf2USsLZ

## ¿Qué hace?
- **extract**: lee `alumnos.csv`, `calificaciones.json`, `matriculas.xml`
- **transform**: normaliza y junta por `id_alumno` (ajusta a tus columnas reales)
- **load**: guarda `df_final.parquet` y carga a `data/etl.db` (tabla `hechos`)
- **log_run**: registra una fila en `etl_monitor` con métricas de la corrida
- Manejo de errores: si algo falla, se registra `estado='FAIL'` y el error

## Programar corridas
- Puedes usar **crontab**, **Task Scheduler** o un simple `while sleep` para lanzarlo diario.
- También puedes orquestarlo con **Prefect deployments** si lo deseas (no requerido para este lab).

## Personaliza a tu Lab1
Edita `flows/etl_lab2_prefect.py` en la sección **TRANSFORMACIÓN**: llaves, merges, reglas y columnas.
