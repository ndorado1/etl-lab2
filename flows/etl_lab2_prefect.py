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
import logging

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = DATA_DIR / "etl.db"
FINAL_PARQUET = DATA_DIR / "df_final.parquet"
LOG_PATH = DATA_DIR / "etl.log"

def setup_logging():
    """Configura el sistema de logging para el ETL"""
    # Crear el directorio de datos si no existe
    DATA_DIR.mkdir(exist_ok=True)
    
    # Configurar el logger
    logger = logging.getLogger('etl_lab2')
    logger.setLevel(logging.INFO)
    
    # Evitar duplicar handlers si ya existen
    if not logger.handlers:
        # Handler para archivo
        file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        # Handler para consola
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        
        # Formato de logs
        formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(funcName)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
    
    return logger

# Configurar logging al inicio
etl_logger = setup_logging()

@task
def extract() -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()
    
    # Log de inicio
    etl_logger.info("=== INICIANDO FASE DE EXTRACCIÓN ===")
    etl_logger.info(f"Timestamp de inicio: {start.isoformat()}")

    try:
        alumnos_csv = DATA_DIR / "alumnos.csv"
        calif_json = DATA_DIR / "calificaciones.json"
        matric_xml = DATA_DIR / "matriculas.xml"
        
        etl_logger.info(f"Leyendo archivo de alumnos: {alumnos_csv}")
        df_al = pd.read_csv(alumnos_csv)
        etl_logger.info(f"Alumnos leídos: {len(df_al)} registros")
        
        etl_logger.info(f"Leyendo archivo de calificaciones: {calif_json}")
        df_ca = pd.read_json(calif_json, orient="records", lines=False)
        etl_logger.info(f"Calificaciones leídas: {len(df_ca)} registros")

        etl_logger.info(f"Leyendo archivo de matrículas: {matric_xml}")
        # XML -> DataFrame
        tree = ET.parse(matric_xml)
        root = tree.getroot()
        rows = []
        for r in root.findall(".//matricula"):
            rows.append({child.tag: child.text for child in r})
        df_ma = pd.DataFrame(rows)
        etl_logger.info(f"Matrículas leídas: {len(df_ma)} registros")

        # Copias raw (opcionales)
        etl_logger.info("Generando copias raw de los datos originales")
        df_al.to_csv(DATA_DIR / "raw_alumnos.csv", index=False)
        df_ca.to_json(DATA_DIR / "raw_calificaciones.json", orient="records", indent=2)
        df_ma.to_csv(DATA_DIR / "raw_matriculas.csv", index=False)
        etl_logger.info("Copias raw generadas exitosamente")

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        
        metrics = {
            "ts_extract": start.isoformat(),
            "alumnos_rows": int(len(df_al)),
            "calificaciones_rows": int(len(df_ca)),
            "matriculas_rows": int(len(df_ma)),
        }
        
        etl_logger.info(f"=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
        etl_logger.info(f"Duración: {duration:.2f} segundos")
        total_registros = metrics["alumnos_rows"] + metrics["calificaciones_rows"] + metrics["matriculas_rows"]
        etl_logger.info(f"Total registros extraídos: {total_registros}")
        
        logger.info(f"EXTRACT OK: {metrics}")
        return {"al": df_al, "ca": df_ca, "ma": df_ma, "metrics": metrics}
        
    except Exception as e:
        etl_logger.error(f"ERROR EN EXTRACCIÓN: {str(e)}")
        etl_logger.error(f"Tipo de error: {type(e).__name__}")
        raise

@task
def transform(dfs: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()
    
    # Log de inicio
    etl_logger.info("=== INICIANDO FASE DE TRANSFORMACIÓN ===")
    etl_logger.info(f"Timestamp de inicio: {start.isoformat()}")

    try:
        df_al = dfs["al"].copy()
        df_ca = dfs["ca"].copy()
        df_ma = dfs["ma"].copy()
        
        etl_logger.info(f"Datos recibidos - Alumnos: {len(df_al)}, Calificaciones: {len(df_ca)}, Matrículas: {len(df_ma)}")

        # === LIMPIEZA DE DATOS ===
        etl_logger.info("Iniciando limpieza de datos")
        
        # 1. Eliminar duplicados en alumnos
        registros_antes = len(df_al)
        df_al = df_al.drop_duplicates(subset=['id_alumno'], keep='first')
        duplicados_eliminados = registros_antes - len(df_al)
        etl_logger.info(f"Duplicados eliminados en alumnos: {duplicados_eliminados}")
        logger.info(f"Alumnos después de eliminar duplicados: {len(df_al)}")
        
        # 2. Generar correos faltantes
        etl_logger.info("Generando correos electrónicos faltantes")
        def generar_correo(row):
            """Genera correo si está faltante usando formato nombre+apellido@colegio.edu"""
            if pd.isna(row['correo']) or row['correo'] == '':
                nombre = str(row['nombre']).lower().strip()
                apellido = str(row['apellido']).lower().strip()
                # Remover espacios y caracteres especiales
                nombre_limpio = nombre.replace(' ', '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
                apellido_limpio = apellido.replace(' ', '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
                return f"{nombre_limpio}.{apellido_limpio}@colegio.edu"
            return row['correo']
        
        # Contar correos faltantes antes
        correos_faltantes_antes = df_al['correo'].isna().sum()
        etl_logger.info(f"Correos faltantes encontrados: {correos_faltantes_antes}")
        
        # Aplicar generación de correos
        df_al['correo'] = df_al.apply(generar_correo, axis=1)
        
        # Contar correos generados
        correos_faltantes_despues = df_al['correo'].isna().sum()
        correos_generados = correos_faltantes_antes - correos_faltantes_despues
        
        etl_logger.info(f"Correos generados automáticamente: {correos_generados}")
        logger.info(f"Correos generados automáticamente: {correos_generados}")
        logger.info(f"Correos faltantes restantes: {correos_faltantes_despues}")
        
        # 3. Redondear calificaciones al rango 0-5
        etl_logger.info("Normalizando calificaciones al rango 0-5")
        calificaciones_fuera_rango = 0
        
        def normalizar_nota(nota):
            """Normaliza las notas al rango 0-5"""
            nonlocal calificaciones_fuera_rango
            if pd.isna(nota):
                return nota
            # Contar notas fuera del rango
            if nota < 0 or nota > 5:
                calificaciones_fuera_rango += 1
            # Redondear al rango 0-5
            return max(0, min(5, round(float(nota), 1)))
        
        df_ca['nota'] = df_ca['nota'].apply(normalizar_nota)
        etl_logger.info(f"Calificaciones fuera de rango corregidas: {calificaciones_fuera_rango}")
        logger.info(f"Calificaciones normalizadas al rango 0-5")

        # === TRANSFORMACIÓN ===
        etl_logger.info("Iniciando proceso de merge de datos")
        # Identifica la llave primaria
        key = "id_alumno"
        
        # Verificar que todas las tablas tienen la llave correcta
        etl_logger.info(f"Verificando estructura de datos")
        logger.info(f"Columnas alumnos: {list(df_al.columns)}")
        logger.info(f"Columnas calificaciones: {list(df_ca.columns)}")
        logger.info(f"Columnas matrículas: {list(df_ma.columns)}")

        # Filtrar registros válidos (sin nulos en la llave)
        etl_logger.info("Filtrando registros válidos")
        valid_al = df_al[df_al[key].notna()].copy()
        valid_ca = df_ca[df_ca[key].notna()].copy()
        valid_ma = df_ma[df_ma[key].notna()].copy()
        
        registros_invalidos = (len(df_al) - len(valid_al)) + (len(df_ca) - len(valid_ca)) + (len(df_ma) - len(valid_ma))
        if registros_invalidos > 0:
            etl_logger.warning(f"Registros con ID nulo descartados: {registros_invalidos}")

        # === MERGES DETALLADOS ===
        etl_logger.info("Ejecutando merge de calificaciones con alumnos")
        # 1. Merge calificaciones con datos de alumnos (mantener detalle por materia)
        df_temp = valid_ca.merge(valid_al, on=key, how='left')
        etl_logger.info(f"Merge calificaciones-alumnos: {len(df_temp)} registros")
        logger.info(f"Después de merge calificaciones con alumnos: {len(df_temp)} filas")
        
        etl_logger.info("Ejecutando merge con datos de matrícula")
        # 2. Merge con datos de matrícula
        df_final = df_temp.merge(valid_ma, on=key, how='left')
        etl_logger.info(f"Merge final con matrículas: {len(df_final)} registros")
        logger.info(f"Después de merge con matrículas: {len(df_final)} filas")
        
        # Verificar que los datos de matrícula se asociaron
        matriculas_asociadas = df_final[df_final['anio'].notna()].shape[0]
        etl_logger.info(f"Registros con matrícula asociada: {matriculas_asociadas}")
        logger.info(f"Registros con datos de matrícula asociados: {matriculas_asociadas}")
        
        # Contar alumnos únicos con matrícula
        alumnos_unicos_con_matricula = df_final[df_final['anio'].notna()][key].nunique()
        etl_logger.info(f"Alumnos únicos con matrícula: {alumnos_unicos_con_matricula}")
        logger.info(f"Alumnos únicos con matrícula: {alumnos_unicos_con_matricula}")

        # Calcular registros descartados
        discarded = (len(df_al) - len(valid_al)) + (len(df_ca) - len(valid_ca)) + (len(df_ma) - len(valid_ma))

        # Reorganizar columnas para mejor visualización
        etl_logger.info("Reorganizando columnas del dataset final")
        columnas_ordenadas = [
            key, 'nombre', 'apellido', 'grado', 'correo', 'fecha_nacimiento',
            'asignatura', 'nota', 'periodo', 
            'anio', 'estado', 'jornada'
        ]
        # Solo incluir columnas que existan
        columnas_finales = [col for col in columnas_ordenadas if col in df_final.columns]
        df_final = df_final[columnas_finales]

        # Guarda salida intermedia
        etl_logger.info(f"Guardando dataset final en: {FINAL_PARQUET}")
        df_final.to_parquet(FINAL_PARQUET, index=False)
        
        # Log de muestra de datos finales
        logger.info(f"Muestra de datos finales:\n{df_final.head(10)}")
        logger.info(f"Columnas finales: {list(df_final.columns)}")
        logger.info(f"Ejemplo - Alumno A001 tiene {len(df_final[df_final[key] == 'A001'])} materias")

        # Calcular estadísticas
        promedio_general = df_final['nota'].mean() if 'nota' in df_final.columns else 0
        total_alumnos_unicos = df_final[key].nunique()
        total_materias_diferentes = df_final['asignatura'].nunique() if 'asignatura' in df_final.columns else 0

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        
        etl_logger.info("=== TRANSFORMACIÓN COMPLETADA EXITOSAMENTE ===")
        etl_logger.info(f"Duración: {duration:.2f} segundos")
        etl_logger.info(f"Registros procesados: {len(df_final)}")
        etl_logger.info(f"Alumnos únicos: {total_alumnos_unicos}")
        etl_logger.info(f"Materias diferentes: {total_materias_diferentes}")
        etl_logger.info(f"Promedio general de notas: {promedio_general:.2f}")

        metrics = {
            "ts_transform": start.isoformat(),
            "valid_rows": int(len(df_final)),
            "discarded_rows": int(discarded),
            "alumnos_con_matricula": int(alumnos_unicos_con_matricula),
            "promedio_notas_general": float(promedio_general),
            "total_alumnos_unicos": int(total_alumnos_unicos),
            "total_materias_diferentes": int(total_materias_diferentes),
            "correos_generados": int(correos_generados)
        }
        logger.info(f"TRANSFORM OK: {metrics}")
        return {"df_final_path": str(FINAL_PARQUET), "metrics": metrics}
        
    except Exception as e:
        etl_logger.error(f"ERROR EN TRANSFORMACIÓN: {str(e)}")
        etl_logger.error(f"Tipo de error: {type(e).__name__}")
        raise

@task
def load(transform_out: Dict[str, Any]) -> Dict[str, Any]:
    logger = get_run_logger()
    start = datetime.utcnow()
    
    # Log de inicio
    etl_logger.info("=== INICIANDO FASE DE CARGA ===")
    etl_logger.info(f"Timestamp de inicio: {start.isoformat()}")

    try:
        etl_logger.info(f"Leyendo datos transformados desde: {transform_out['df_final_path']}")
        df_final = pd.read_parquet(transform_out["df_final_path"])
        etl_logger.info(f"Registros a cargar: {len(df_final)}")

        # Carga a SQLite
        etl_logger.info(f"Conectando a base de datos: {DB_PATH}")
        conn = sqlite3.connect(DB_PATH)
        
        etl_logger.info("Cargando datos en tabla 'hechos' (reemplazando contenido anterior)")
        df_final.to_sql("hechos", conn, if_exists="replace", index=False)
        conn.commit()
        
        # Verificar la carga
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hechos")
        registros_cargados = cursor.fetchone()[0]
        etl_logger.info(f"Verificación: {registros_cargados} registros en tabla hechos")
        
        conn.close()
        etl_logger.info("Conexión a base de datos cerrada")

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        
        etl_logger.info("=== CARGA COMPLETADA EXITOSAMENTE ===")
        etl_logger.info(f"Duración: {duration:.2f} segundos")
        etl_logger.info(f"Registros cargados: {registros_cargados}")

        metrics = {
            "ts_load": start.isoformat(),
            "loaded_rows": int(len(df_final))
        }
        logger.info(f"LOAD OK: {metrics}")
        return {"metrics": metrics}
        
    except Exception as e:
        etl_logger.error(f"ERROR EN CARGA: {str(e)}")
        etl_logger.error(f"Tipo de error: {type(e).__name__}")
        raise

@task
def log_run(extract_m: Dict[str, Any], transform_m: Dict[str, Any], load_m: Dict[str, Any], estado: str = "OK", mensaje: str = "") -> None:
    logger = get_run_logger()
    registros_leidos = (extract_m.get("alumnos_rows", 0) +
                        extract_m.get("calificaciones_rows", 0) +
                        extract_m.get("matriculas_rows", 0))
    registros_validos = transform_m.get("valid_rows", 0)
    registros_descartados = transform_m.get("discarded_rows", 0)
    alumnos_con_matricula = transform_m.get("alumnos_con_matricula", 0)
    promedio_notas = transform_m.get("promedio_notas_general", 0)
    total_alumnos_unicos = transform_m.get("total_alumnos_unicos", 0)
    total_materias_diferentes = transform_m.get("total_materias_diferentes", 0)
    correos_generados = transform_m.get("correos_generados", 0)

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
            alumnos_con_matricula INTEGER,
            total_alumnos_unicos INTEGER,
            total_materias_diferentes INTEGER,
            correos_generados INTEGER,
            promedio_notas_general REAL,
            duracion_s TEXT,
            estado TEXT,
            mensaje TEXT
        );
    """)
    conn.commit()
    cur.execute("""
        INSERT INTO etl_monitor(run_ts, registros_leidos, registros_validos, registros_descartados, alumnos_con_matricula, total_alumnos_unicos, total_materias_diferentes, correos_generados, promedio_notas_general, duracion_s, estado, mensaje)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (ts_now, int(registros_leidos), int(registros_validos), int(registros_descartados), int(alumnos_con_matricula), int(total_alumnos_unicos), int(total_materias_diferentes), int(correos_generados), float(promedio_notas), "-", estado, mensaje[:500]))
    conn.commit()
    conn.close()

    resumen = {
        "registros_leidos": registros_leidos,
        "registros_validos": registros_validos,
        "registros_descartados": registros_descartados,
        "alumnos_con_matricula": alumnos_con_matricula,
        "total_alumnos_unicos": total_alumnos_unicos,
        "total_materias_diferentes": total_materias_diferentes,
        "correos_generados": correos_generados,
        "promedio_notas_general": round(promedio_notas, 2),
        "estado": estado,
        "mensaje": mensaje[:200]
    }
    logger.info("RESUMEN LAB2 ETL PREFECT: %s", json.dumps(resumen, indent=2))

@flow(name="lab2_etl_prefect")
def etl_flow():
    flow_start = datetime.utcnow()
    etl_logger.info("🚀 ========================================")
    etl_logger.info("🚀 INICIANDO PIPELINE ETL LAB2 CON PREFECT")
    etl_logger.info("🚀 ========================================")
    etl_logger.info(f"🚀 Timestamp de inicio del flow: {flow_start.isoformat()}")
    
    try:
        etl_logger.info("📥 Ejecutando fase de EXTRACCIÓN")
        ext = extract()
        
        etl_logger.info("🔄 Ejecutando fase de TRANSFORMACIÓN")
        trn = transform(ext)
        
        etl_logger.info("💾 Ejecutando fase de CARGA")
        lod = load(trn)
        
        # Extraer métricas individuales para el monitor
        extract_metrics = ext["metrics"]
        transform_metrics = trn["metrics"]
        load_metrics = lod["metrics"]

        etl_logger.info("📊 Registrando métricas de ejecución")
        log_run(extract_metrics, transform_metrics, load_metrics, estado="OK", mensaje="")
        
        flow_end = datetime.utcnow()
        total_duration = (flow_end - flow_start).total_seconds()
        
        etl_logger.info("✅ ========================================")
        etl_logger.info("✅ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        etl_logger.info("✅ ========================================")
        etl_logger.info(f"✅ Duración total: {total_duration:.2f} segundos")
        etl_logger.info(f"✅ Registros procesados: {transform_metrics.get('valid_rows', 0)}")
        etl_logger.info(f"✅ Alumnos únicos: {transform_metrics.get('total_alumnos_unicos', 0)}")
        etl_logger.info(f"✅ Timestamp de finalización: {flow_end.isoformat()}")
        
    except Exception as e:
        flow_end = datetime.utcnow()
        total_duration = (flow_end - flow_start).total_seconds()
        
        etl_logger.error("❌ ========================================")
        etl_logger.error("❌ ERROR EN PIPELINE ETL")
        etl_logger.error("❌ ========================================")
        etl_logger.error(f"❌ Error: {str(e)}")
        etl_logger.error(f"❌ Tipo de error: {type(e).__name__}")
        etl_logger.error(f"❌ Duración hasta el error: {total_duration:.2f} segundos")
        etl_logger.error(f"❌ Timestamp del error: {flow_end.isoformat()}")
        
        # Si algo falla, registramos FAIL con métricas mínimas
        try:
            # Métricas vacías si no alcanzó a correr
            log_run.submit({}, {}, {}, estado="FAIL", mensaje=str(e))
        finally:
            raise

if __name__ == "__main__":
    etl_flow()
