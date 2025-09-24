from __future__ import annotations
from prefect import flow, task, get_run_logger
from prefect.states import Failed
from typing import Dict, Any
from pathlib import Path
from datetime import datetime
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import json

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = DATA_DIR / "etl.db"
FINAL_PARQUET = DATA_DIR / "df_final.parquet"
LOG_PATH = DATA_DIR / "etl.log"  # (logs simples por print; prefect también guarda logs)

@task
def extract() -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()

    alumnos_csv = DATA_DIR / "alumnos.csv"
    calif_json = DATA_DIR / "calificaciones.json"
    matric_xml = DATA_DIR / "matriculas.xml"

    df_al = pd.read_csv(alumnos_csv)
    df_ca = pd.read_json(calif_json, orient="records", lines=False)

    # XML -> DataFrame
    tree = ET.parse(matric_xml)
    root = tree.getroot()
    rows = []
    for r in root.findall(".//matricula"):
        rows.append({child.tag: child.text for child in r})
    df_ma = pd.DataFrame(rows)

    # Copias raw (opcionales)
    df_al.to_csv(DATA_DIR / "raw_alumnos.csv", index=False)
    df_ca.to_json(DATA_DIR / "raw_calificaciones.json", orient="records", indent=2)
    df_ma.to_csv(DATA_DIR / "raw_matriculas.csv", index=False)

    metrics = {
        "ts_extract": start.isoformat(),
        "alumnos_rows": int(len(df_al)),
        "calificaciones_rows": int(len(df_ca)),
        "matriculas_rows": int(len(df_ma)),
    }
    logger.info(f"EXTRACT OK: {metrics}")
    return {"al": df_al, "ca": df_ca, "ma": df_ma, "metrics": metrics}

@task
def transform(dfs: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()

    df_al = dfs["al"].copy()
    df_ca = dfs["ca"].copy()
    df_ma = dfs["ma"].copy()

    # === TRANSFORMACIÓN (ajusta a tu Lab1) ===
    # Normaliza tipos en df_ma
    for col in ["id_alumno", "id_matricula"]:
        if col in df_ma.columns:
            df_ma[col] = pd.to_numeric(df_ma[col], errors="coerce")

    # Identifica la llave primaria
    key = "id_alumno" if "id_alumno" in df_al.columns else df_al.columns[0]

    # Asegura el nombre de llave en ca y ma
    if key not in df_ca.columns:
        for c in ["idAlumno", "idalumno", "alumno_id"]:
            if c in df_ca.columns:
                df_ca = df_ca.rename(columns={c: key})
                break
    if key not in df_ma.columns:
        for c in ["idAlumno", "idalumno", "alumno_id"]:
            if c in df_ma.columns:
                df_ma = df_ma.rename(columns={c: key})
                break

    valid_left = df_al[df_al[key].notna()].copy()
    valid_ca = df_ca[df_ca[key].notna()].copy()
    valid_ma = df_ma[df_ma[key].notna()].copy()

    discarded = (len(df_al) - len(valid_left)) + (len(df_ca) - len(valid_ca)) + (len(df_ma) - len(valid_ma))

    # Merge incremental
    df_final = valid_left.merge(valid_ca, on=key, how="left").merge(valid_ma, on=key, how="left")

    # Guarda salida intermedia
    df_final.to_parquet(FINAL_PARQUET, index=False)

    metrics = {
        "ts_transform": start.isoformat(),
        "valid_rows": int(len(df_final)),
        "discarded_rows": int(discarded),
    }
    logger.info(f"TRANSFORM OK: {metrics}")
    return {"df_final_path": str(FINAL_PARQUET), "metrics": metrics}

@task
def load(transform_out: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()

    df_final = pd.read_parquet(transform_out["df_final_path"])

    # Carga a SQLite
    conn = sqlite3.connect(DB_PATH)
    df_final.to_sql("hechos", conn, if_exists="replace", index=False)
    conn.commit()
    conn.close()

    metrics = {
        "ts_load": start.isoformat(),
        "loaded_rows": int(len(df_final))
    }
    logger.info(f"LOAD OK: {metrics}")
    return {"metrics": metrics}

@task
def log_run(extract_m: Dict[str, Any], transform_m: Dict[str, Any], load_m: Dict[str, Any], estado: str = "OK", mensaje: str = "") -> None:
    logger = get_run_logger()
    registros_leidos = (extract_m.get("alumnos_rows", 0) +
                        extract_m.get("calificaciones_rows", 0) +
                        extract_m.get("matriculas_rows", 0))
    registros_validos = transform_m.get("valid_rows", 0)
    registros_descartados = transform_m.get("discarded_rows", 0)

    # Para una duración aproximada, medimos en la propia tarea (simple)
    ts_now = datetime.utcnow().isoformat(timespec="seconds")

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_monitor (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_ts TEXT,
            registros_leidos INTEGER,
            registros_validos INTEGER,
            registros_descartados INTEGER,
            duracion_s TEXT,
            estado TEXT,
            mensaje TEXT
        );
    """)
    conn.commit()
    cur.execute("""
        INSERT INTO etl_monitor(run_ts, registros_leidos, registros_validos, registros_descartados, duracion_s, estado, mensaje)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (ts_now, int(registros_leidos), int(registros_validos), int(registros_descartados), "-", estado, mensaje[:500]))
    conn.commit()
    conn.close()

    resumen = {
        "registros_leidos": registros_leidos,
        "registros_validos": registros_validos,
        "registros_descartados": registros_descartados,
        "estado": estado,
        "mensaje": mensaje[:200]
    }
    logger.info("RESUMEN LAB2 ETL PREFECT: %s", json.dumps(resumen, indent=2))

@flow(name="lab2_etl_prefect")
def etl_flow():
    try:
        ext = extract()
        trn = transform(ext)
        lod = load(trn)
        # Extraer métricas individuales para el monitor
        extract_metrics = ext["metrics"]
        transform_metrics = trn["metrics"]
        load_metrics = lod["metrics"]

        log_run(extract_metrics, transform_metrics, load_metrics, estado="OK", mensaje="")
    except Exception as e:
        # Si algo falla, registramos FAIL con métricas mínimas
        try:
            # Métricas vacías si no alcanzó a correr
            log_run.submit({}, {}, {}, estado="FAIL", mensaje=str(e))
        finally:
            raise

if __name__ == "__main__":
    etl_flow()
