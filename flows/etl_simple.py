#!/usr/bin/env python3
"""
ETL Pipeline Simple - Sin Prefect para GitHub Actions
Mantiene toda la funcionalidad original pero sin dependencias complejas
"""

from datetime import datetime
import pandas as pd
import sqlite3
import xml.etree.ElementTree as ET
import json
import logging
from pathlib import Path

# Configuración de paths
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DB_PATH = DATA_DIR / "etl.db"
FINAL_PARQUET = DATA_DIR / "df_final.parquet"
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
        
        return df_al, df_ca, df_ma
        
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
        
        # 1. Eliminar duplicados
        registros_antes = len(df_al)
        df_al = df_al.drop_duplicates(subset=['id_alumno'], keep='first')
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
        
        # 3. Normalizar calificaciones
        logger.info("Normalizando calificaciones al rango 0-5")
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
        logger.info(f"Guardando dataset final en: {FINAL_PARQUET}")
        df_final.to_parquet(FINAL_PARQUET, index=False)
        
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
        
        return df_final, {
            'correos_generados': correos_generados,
            'alumnos_con_matricula': alumnos_unicos_con_matricula,
            'promedio_notas_general': promedio_general,
            'total_alumnos_unicos': total_alumnos_unicos,
            'total_materias_diferentes': total_materias_diferentes
        }
        
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
        df_al, df_ca, df_ma = extract(logger)
        
        logger.info("🔄 Ejecutando fase de TRANSFORMACIÓN")
        df_final, metrics = transform(df_al, df_ca, df_ma, logger)
        
        logger.info("💾 Ejecutando fase de CARGA")
        registros_cargados = load(df_final, logger)
        
        flow_end = datetime.utcnow()
        total_duration = (flow_end - flow_start).total_seconds()
        
        logger.info("✅ ========================================")
        logger.info("✅ PIPELINE ETL COMPLETADO EXITOSAMENTE")
        logger.info("✅ ========================================")
        logger.info(f"✅ Duración total: {total_duration:.2f} segundos")
        logger.info(f"✅ Registros procesados: {len(df_final)}")
        logger.info(f"✅ Alumnos únicos: {metrics['total_alumnos_unicos']}")
        logger.info(f"✅ Correos generados: {metrics['correos_generados']}")
        logger.info(f"✅ Timestamp de finalización: {flow_end.isoformat()}")
        
        # Resumen final
        resumen = {
            "duracion_total_segundos": round(total_duration, 2),
            "registros_procesados": len(df_final),
            "registros_cargados": registros_cargados,
            "alumnos_unicos": metrics['total_alumnos_unicos'],
            "materias_diferentes": metrics['total_materias_diferentes'],
            "correos_generados": metrics['correos_generados'],
            "promedio_notas_general": round(metrics['promedio_notas_general'], 2),
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
        
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
