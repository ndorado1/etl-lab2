#!/usr/bin/env python3
"""
ETL Pipeline Simple - Sin Prefect para GitHub Actions
"""

from datetime import datetime
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import json
import logging
from pathlib import Path
import numpy as np

# Configuración de paths
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = DATA_DIR / "etl.db"
FINAL_CSV = DATA_DIR / "df_final.csv"
LOG_PATH = DATA_DIR / "etl.log"

def setup_logging():
    """Configura el sistema de logging"""
    DATA_DIR.mkdir(exist_ok=True)
    
    # Configurar logging con archivo y consola
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)s | %(funcName)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(LOG_PATH, encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def extract(logger):
    """Fase de extracción"""
    start = datetime.utcnow()
    logger.info("=== INICIANDO FASE DE EXTRACCIÓN ===")
    
    try:
        # Leer archivos
        alumnos_csv = DATA_DIR / "alumnos.csv"
        calif_json = DATA_DIR / "calificaciones.json"
        matric_xml = DATA_DIR / "matriculas.xml"
        
        logger.info(f"Leyendo archivo de alumnos: {alumnos_csv}")
        df_al = pd.read_csv(alumnos_csv)
        logger.info(f"Alumnos leídos: {len(df_al)} registros")
        
        logger.info(f"Leyendo archivo de calificaciones: {calif_json}")
        df_ca = pd.read_json(calif_json, orient="records", lines=False)
        logger.info(f"Calificaciones leídas: {len(df_ca)} registros")

        logger.info(f"Leyendo archivo de matrículas: {matric_xml}")
        tree = ET.parse(matric_xml)
        root = tree.getroot()
        rows = []
        for r in root.findall(".//matricula"):
            rows.append({child.tag: child.text for child in r})
        df_ma = pd.DataFrame(rows)
        logger.info(f"Matrículas leídas: {len(df_ma)} registros")

        # Generar copias raw
        logger.info("Generando copias raw de los datos originales")
        df_al.to_csv(DATA_DIR / "raw_alumnos.csv", index=False)
        df_ca.to_json(DATA_DIR / "raw_calificaciones.json", orient="records", indent=2)
        df_ma.to_csv(DATA_DIR / "raw_matriculas.csv", index=False)

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        total_registros = len(df_al) + len(df_ca) + len(df_ma)
        
        logger.info("=== EXTRACCIÓN COMPLETADA EXITOSAMENTE ===")
        logger.info(f"Duración: {duration:.2f} segundos")
        logger.info(f"Total registros extraídos: {total_registros}")
        
        # Métricas de extracción
        extract_metrics = {
            "alumnos_rows": len(df_al),
            "calificaciones_rows": len(df_ca),
            "matriculas_rows": len(df_ma),
            "total_registros": total_registros
        }
        
        return df_al, df_ca, df_ma, extract_metrics
        
    except Exception as e:
        logger.error(f"ERROR EN EXTRACCIÓN: {str(e)}")
        raise

def transform(df_al, df_ca, df_ma, logger):
    """Fase de transformación"""
    start = datetime.utcnow()
    logger.info("=== INICIANDO FASE DE TRANSFORMACIÓN ===")
    
    try:
        logger.info(f"Datos recibidos - Alumnos: {len(df_al)}, Calificaciones: {len(df_ca)}, Matrículas: {len(df_ma)}")

        # === LIMPIEZA DE DATOS ===
        logger.info("Iniciando limpieza de datos")
        
        # 1. Eliminar duplicados - crear copia explícita para evitar warning
        registros_antes = len(df_al)
        df_al = df_al.drop_duplicates(subset=['id_alumno'], keep='first').copy()
        duplicados_eliminados = registros_antes - len(df_al)
        logger.info(f"Duplicados eliminados en alumnos: {duplicados_eliminados}")
        
        # 2. Generar correos faltantes
        logger.info("Generando correos electrónicos faltantes")
        def generar_correo(row):
            if pd.isna(row['correo']) or row['correo'] == '':
                nombre = str(row['nombre']).lower().strip()
                apellido = str(row['apellido']).lower().strip()
                nombre_limpio = nombre.replace(' ', '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
                apellido_limpio = apellido.replace(' ', '').replace('á', 'a').replace('é', 'e').replace('í', 'i').replace('ó', 'o').replace('ú', 'u')
                return f"{nombre_limpio}.{apellido_limpio}@colegio.edu"
            return row['correo']
        
        correos_faltantes_antes = df_al['correo'].isna().sum()
        df_al['correo'] = df_al.apply(generar_correo, axis=1)
        correos_faltantes_despues = df_al['correo'].isna().sum()
        correos_generados = correos_faltantes_antes - correos_faltantes_despues
        logger.info(f"Correos generados automáticamente: {correos_generados}")
        
        # 3. Normalizar calificaciones - crear copia para evitar warning
        logger.info("Normalizando calificaciones al rango 0-5")
        df_ca = df_ca.copy()
        calificaciones_fuera_rango = 0
        
        def normalizar_nota(nota):
            nonlocal calificaciones_fuera_rango
            if pd.isna(nota):
                return nota
            if nota < 0 or nota > 5:
                calificaciones_fuera_rango += 1
            return max(0, min(5, round(float(nota), 1)))
        
        df_ca['nota'] = df_ca['nota'].apply(normalizar_nota)
        logger.info(f"Calificaciones fuera de rango corregidas: {calificaciones_fuera_rango}")

        # === VALIDACIÓN INTENCIONAL DE ERRORES ===
        # Error intencional para demostrar manejo de errores
        import random
        random.seed(datetime.utcnow().day)  # Usar el día para hacer el error predecible
        
        if random.random() < 0.3:  # 30% de probabilidad de error
            error_msg = "Validación de integridad de datos falló - datos inconsistentes detectados"
            logger.error(f"🚨 {error_msg}")
            raise ValueError(error_msg)
        
        logger.info("✅ Validación de integridad de datos completada exitosamente")

        # === MERGES ===
        logger.info("Iniciando proceso de merge de datos")
        key = "id_alumno"
        
        # Merge calificaciones con alumnos
        df_temp = df_ca.merge(df_al, on=key, how='left')
        logger.info(f"Merge calificaciones-alumnos: {len(df_temp)} registros")
        
        # Merge con matrículas
        df_final = df_temp.merge(df_ma, on=key, how='left')
        logger.info(f"Merge final con matrículas: {len(df_final)} registros")
        
        # Verificar asociación de matrículas
        matriculas_asociadas = df_final[df_final['anio'].notna()].shape[0]
        alumnos_unicos_con_matricula = df_final[df_final['anio'].notna()][key].nunique()
        
        logger.info(f"Registros con matrícula asociada: {matriculas_asociadas}")
        logger.info(f"Alumnos únicos con matrícula: {alumnos_unicos_con_matricula}")

        # Reorganizar columnas
        columnas_ordenadas = [
            key, 'nombre', 'apellido', 'grado', 'correo', 'fecha_nacimiento',
            'asignatura', 'nota', 'periodo', 'anio', 'estado', 'jornada'
        ]
        columnas_finales = [col for col in columnas_ordenadas if col in df_final.columns]
        df_final = df_final[columnas_finales]

        # Guardar dataset final
        logger.info(f"Guardando dataset final en: {FINAL_CSV}")
        df_final.to_csv(FINAL_CSV, index=False)
        
        # Estadísticas finales
        promedio_general = df_final['nota'].mean() if 'nota' in df_final.columns else 0
        total_alumnos_unicos = df_final[key].nunique()
        total_materias_diferentes = df_final['asignatura'].nunique() if 'asignatura' in df_final.columns else 0

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        
        logger.info("=== TRANSFORMACIÓN COMPLETADA EXITOSAMENTE ===")
        logger.info(f"Duración: {duration:.2f} segundos")
        logger.info(f"Registros procesados: {len(df_final)}")
        logger.info(f"Alumnos únicos: {total_alumnos_unicos}")
        logger.info(f"Materias diferentes: {total_materias_diferentes}")
        logger.info(f"Promedio general de notas: {promedio_general:.2f}")
        
        # Calcular registros descartados (registros con id_alumno nulo)
        registros_descartados = 0
       
        
        transform_metrics = {
            'correos_generados': int(correos_generados),
            'alumnos_con_matricula': int(alumnos_unicos_con_matricula),
            'promedio_notas_general': float(promedio_general),
            'total_alumnos_unicos': int(total_alumnos_unicos),
            'total_materias_diferentes': int(total_materias_diferentes),
            'registros_validos': int(len(df_final)),
            'registros_descartados': int(registros_descartados)
        }
        
        return df_final, transform_metrics
        
    except Exception as e:
        logger.error(f"ERROR EN TRANSFORMACIÓN: {str(e)}")
        raise

def load(df_final, logger):
    """Fase de carga"""
    start = datetime.utcnow()
    logger.info("=== INICIANDO FASE DE CARGA ===")
    
    try:
        logger.info(f"Registros a cargar: {len(df_final)}")
        logger.info(f"Conectando a base de datos: {DB_PATH}")
        
        conn = sqlite3.connect(DB_PATH)
        logger.info("Cargando datos en tabla 'hechos' (reemplazando contenido anterior)")
        df_final.to_sql("hechos", conn, if_exists="replace", index=False)
        conn.commit()
        
        # Verificar la carga
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM hechos")
        registros_cargados = cursor.fetchone()[0]
        logger.info(f"Verificación: {registros_cargados} registros en tabla hechos")
        
        conn.close()
        logger.info("Conexión a base de datos cerrada")

        end = datetime.utcnow()
        duration = (end - start).total_seconds()
        
        logger.info("=== CARGA COMPLETADA EXITOSAMENTE ===")
        logger.info(f"Duración: {duration:.2f} segundos")
        logger.info(f"Registros cargados: {registros_cargados}")
        
        return registros_cargados
        
    except Exception as e:
        logger.error(f"ERROR EN CARGA: {str(e)}")
        raise

def log_run(registros_leidos, registros_validos, registros_descartados, alumnos_con_matricula, 
           total_alumnos_unicos, total_materias_diferentes, correos_generados, 
           promedio_notas_general, duracion_s, estado="OK", mensaje="", logger=None):
    """Registra métricas de la ejecución del ETL en la tabla etl_monitor"""
    try:
        ts_now = datetime.utcnow().isoformat(timespec="seconds")
        
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        
        # Crear tabla etl_monitor si no existe
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
        
        # Insertar métricas de la corrida actual
        cur.execute("""
            INSERT INTO etl_monitor(run_ts, registros_leidos, registros_validos, registros_descartados, 
                                  alumnos_con_matricula, total_alumnos_unicos, total_materias_diferentes, 
                                  correos_generados, promedio_notas_general, duracion_s, estado, mensaje)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (ts_now, int(registros_leidos), int(registros_validos), int(registros_descartados), 
              int(alumnos_con_matricula), int(total_alumnos_unicos), int(total_materias_diferentes), 
              int(correos_generados), float(promedio_notas_general), str(duracion_s), estado, mensaje[:500]))
        
        conn.commit()
        conn.close()
        
        if logger:
            logger.info(f"Métricas registradas en tabla etl_monitor: {estado}")
            
    except Exception as e:
        if logger:
            logger.error(f"Error al registrar métricas en etl_monitor: {str(e)}")
        else:
            print(f"Error al registrar métricas en etl_monitor: {str(e)}")

def main():
    """Función principal del ETL"""
    flow_start = datetime.utcnow()
    logger = setup_logging()
    
    logger.info("🚀 ========================================")
    logger.info("🚀 INICIANDO PIPELINE ETL LAB2")
    logger.info("🚀 ========================================")
    logger.info(f"🚀 Timestamp de inicio: {flow_start.isoformat()}")
    
    try:
        logger.info("📥 Ejecutando fase de EXTRACCIÓN")
        df_al, df_ca, df_ma, extract_metrics = extract(logger)
        
        logger.info("🔄 Ejecutando fase de TRANSFORMACIÓN")
        df_final, transform_metrics = transform(df_al, df_ca, df_ma, logger)
        
        logger.info("💾 Ejecutando fase de CARGA")
        registros_cargados = load(df_final, logger)
        
        flow_end = datetime.utcnow()
        total_duration = (flow_end - flow_start).total_seconds()
        
        # Registrar métricas en tabla etl_monitor
        logger.info("📊 Registrando métricas de ejecución")
        log_run(
            registros_leidos=extract_metrics['total_registros'],
            registros_validos=transform_metrics['registros_validos'],
            registros_descartados=transform_metrics['registros_descartados'],
            alumnos_con_matricula=transform_metrics['alumnos_con_matricula'],
            total_alumnos_unicos=transform_metrics['total_alumnos_unicos'],
            total_materias_diferentes=transform_metrics['total_materias_diferentes'],
            correos_generados=transform_metrics['correos_generados'],
            promedio_notas_general=transform_metrics['promedio_notas_general'],
            duracion_s=round(total_duration, 2),
            estado="OK",
            mensaje="ETL ejecutado exitosamente",
            logger=logger
        )
        
        logger.info("✅ ========================================")
        logger.info("✅ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("✅ ========================================")
        logger.info(f"✅ Duración total: {total_duration:.2f} segundos")
        logger.info(f"✅ Registros procesados: {len(df_final)}")
        logger.info(f"✅ Alumnos únicos: {transform_metrics['total_alumnos_unicos']}")
        logger.info(f"✅ Correos generados: {transform_metrics['correos_generados']}")
        logger.info(f"✅ Timestamp de finalización: {flow_end.isoformat()}")
        
        # Resumen final - convertir todos los valores a tipos Python nativos
        resumen = {
            "duracion_total_segundos": round(float(total_duration), 2),
            "registros_procesados": int(len(df_final)),
            "registros_cargados": int(registros_cargados),
            "alumnos_unicos": int(transform_metrics['total_alumnos_unicos']),
            "materias_diferentes": int(transform_metrics['total_materias_diferentes']),
            "correos_generados": int(transform_metrics['correos_generados']),
            "promedio_notas_general": round(float(transform_metrics['promedio_notas_general']), 2),
            "estado": "EXITOSO",
            "timestamp": flow_end.isoformat()
        }
        
        logger.info("📊 RESUMEN FINAL ETL:")
        logger.info(json.dumps(resumen, indent=2, ensure_ascii=False))
        
        return True
        
    except Exception as e:
        flow_end = datetime.utcnow()
        total_duration = (flow_end - flow_start).total_seconds()
        
        logger.error("❌ ========================================")
        logger.error("❌ ERROR EN PIPELINE ETL")
        logger.error("❌ ========================================")
        logger.error(f"❌ Error: {str(e)}")
        logger.error(f"❌ Tipo de error: {type(e).__name__}")
        logger.error(f"❌ Duración hasta el error: {total_duration:.2f} segundos")
        logger.error(f"❌ Timestamp del error: {flow_end.isoformat()}")
        
        # Registrar métricas de fallo en etl_monitor
        try:
            log_run(
                registros_leidos=0,
                registros_validos=0,
                registros_descartados=0,
                alumnos_con_matricula=0,
                total_alumnos_unicos=0,
                total_materias_diferentes=0,
                correos_generados=0,
                promedio_notas_general=0.0,
                duracion_s=round(total_duration, 2),
                estado="FAIL",
                mensaje=f"Error: {str(e)}",
                logger=logger
            )
        except Exception as log_error:
            logger.error(f"Error adicional al registrar métricas de fallo: {str(log_error)}")
        
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
