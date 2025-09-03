#!/usr/bin/env python3
"""
Script standalone para generar archivos parquet con estructura bronze
Uso: python scripts/generate_parquet.py [table_name]
"""

import pandas as pd
import os
import sys
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def load_config():
    """Cargar configuración desde .env"""
    load_dotenv()
    
    return {
        'dbname': os.getenv('CONSUMER_DB_NAME'),
        'host': os.getenv('CONSUMER_DB_HOST'),
        'port': os.getenv('CONSUMER_DB_PORT', '5432'),
        'user': os.getenv('CONSUMER_DB_USER'),
        'password': os.getenv('CONSUMER_DB_PASSWORD'),
        'output_path': os.getenv('OUTPUT_PATH', '/app/output'),
        'compression': os.getenv('PARQUET_COMPRESSION', 'snappy'),
        'source_name': 'magenta'  # Fuente fija como especificaste
    }



def create_postgres_connection(config):
    """Crear conexión a PostgreSQL"""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['dbname']}"
        )
        print("✓ Conexión a PostgreSQL establecida.")
        return engine
    except SQLAlchemyError as e:
        print(f"✗ Error en conexión a PostgreSQL: {e}")
        return None

def get_table_queries():
    """Definir queries para diferentes tablas"""
    queries = {
        'vehicles': """
            SELECT id, account_id, make, "year", color, "label", model, "domain", subtype, engine_number, chassis_number,mileage,   latitude, longitude, things, location_datetime, created_datetime
            FROM strix.vvehicle;
        """,
        'accounts': """
            SELECT id, identification_type, identification_number, "name", active, country_id, created_datetime, services, last_update_datetime
            FROM strix.vaccount
        """,
        'devices': """
            SELECT
                id::text AS id,
                account_id::text AS account_id,
                user_id::text AS user_id,
                data ->> 'app_installation_id' AS app_installation_id,
                data ->> 'app_version_id'      AS app_version_id,
                (data ->> 'battery_level')::float AS battery_level,
                data ->> 'created_by'          AS created_by,
                to_timestamp((data ->> 'created_timestamp')::bigint / 1000) AS created_timestamp,
                data ->> 'identifier'          AS identifier,
                data ->> 'last_modified_by'    AS last_modified_by,
                to_timestamp((data ->> 'last_modified_timestamp')::bigint / 1000) AS last_modified_timestamp,
                (data ->> 'location_accuracy')::float AS location_accuracy,
                data ->> 'location_coordinates' AS location_coordinates,
                data ->> 'location_type'        AS location_type,
                to_timestamp((data ->> 'location_timestamp')::bigint / 1000) AS location_timestamp,
                data ->> 'make'                AS make,
                data ->> 'model'               AS model,
                data ->> 'name'                AS name,
                (data ->> 'push_notifications_enabled')::boolean AS push_notifications_enabled,
                data ->> 'system_name'         AS system_name,
                data ->> 'system_version'      AS system_version,
                data ->> 'token'               AS token,
                (data ->> 'tracking_enabled')::boolean AS tracking_enabled,
                last_update_datetime
            FROM strix.device  WHERE last_update_datetime >= (
				      SELECT max(j1.step_datetime)
				      FROM strix.job j1
				      WHERE j1.step = 'job start'  );
        """,
        'flexes': """
       SELECT
            thing.id::text AS id,
            thing.account_id::text AS account_id,
            thing.data -> 'info' ->> 'label' AS label,

        ((((thing.data -> 'state'::text) -> 'location'::text) -> 'coordinates'::text) ->> 0)::double precision AS latitude,
        ((((thing.data -> 'state'::text) -> 'location'::text) -> 'coordinates'::text) ->> 1)::double precision AS longitude,    
            (thing.data -> 'state' ->> 'battery_level')::float AS battery_level,
            (thing.data -> 'things' ->> 0) AS things,
            (thing.data -> 'state' -> 'location' -> 'coordinates')::text AS location,
            to_timestamp(((thing.data #>> '{metadata,state,location,timestamp}')::bigint / 1000)) AS location_recorded_at,
            to_timestamp(((thing.data ->> 'created_timestamp')::bigint / 1000)) AS created_datetime
        FROM strix.thing
        WHERE (thing.data ->> 'type') = 'mrn:things:flex'
        AND thing.last_update_datetime >= (
            SELECT max(j1.step_datetime)
            FROM strix.job j1
            WHERE j1.step = 'job start'
        );
        """,
        'gpses': """
            SELECT id, account_id, make, model, serial_number, parent_id, template_id, created_datetime
                FROM strix.vgps;
        """,
         'homes': """
            SELECT id, account_id, "label", address_line1, city, state, latitude, longitude, things, status_datetime, created_datetime
                FROM strix.vhome;
        """,
         'users': """
            SELECT id, account_id,  username, first_name, last_name, signup_completed, has_ios, has_android, has_device, last_device_login
            FROM strix.vuser;
        """,
 
      
    }
    return queries

def extract_table_data(engine, table_name):
    """Extraer datos de la tabla especificada"""
    queries = get_table_queries()
    
    if table_name not in queries:
        print(f"✗ Tabla '{table_name}' no está configurada")
        print(f"  Tablas disponibles: {list(queries.keys())}")
        return None
    
    query = queries[table_name]
    
    try:
        print(f"→ Ejecutando consulta para tabla '{table_name}'...")
        df = pd.read_sql(query, engine)
        print(f"✓ Datos extraídos: {len(df)} registros")
        return df
    except Exception as e:
        print(f"✗ Error extrayendo datos de '{table_name}': {e}")
        return None

def create_bronze_path(config, table_name):
    """Crear la estructura de carpetas bronze"""
    execution_date = datetime.now().strftime("%Y-%m-%d")
    
    # Estructura: /bronze/{source_name}/{dataset_name}/execution_date=YYYY-MM-DD/
    bronze_path = os.path.join(
        config['output_path'],
        'bronze',
        config['source_name'],
        table_name,
        f'execution_date={execution_date}'
    )
    
    return bronze_path

def save_parquet(df, config, table_name):
    """Guardar DataFrame como archivo parquet en estructura bronze"""
    bronze_path = create_bronze_path(config, table_name)
    
    # Crear directorio si no existe
    os.makedirs(bronze_path, exist_ok=True)
    
    # Nombre del archivo con timestamp
    timestamp = datetime.now().strftime("%H%M%S")
    filename = f"{table_name}_{timestamp}.parquet"
    filepath = os.path.join(bronze_path, filename)
    
    try:
        # Guardar como parquet
        df.to_parquet(
            filepath, 
            index=False, 
            engine='pyarrow',
            compression=config['compression']
        )
        
        file_size_mb = os.path.getsize(filepath) / 1024 / 1024
        print(f"✓ Archivo guardado: {filepath}")
        print(f"  Tamaño: {file_size_mb:.2f} MB")
        print(f"  Estructura: /bronze/{config['source_name']}/{table_name}/execution_date={datetime.now().strftime('%Y-%m-%d')}/")
        
        return filepath
        
    except Exception as e:
        print(f"✗ Error guardando archivo: {e}")
        return None

def verify_parquet(filepath, table_name):
    """Verificar el archivo parquet generado"""
    try:
        df_check = pd.read_parquet(filepath)
        print(f"\n📋 Verificación del archivo '{table_name}':")
        print(f"   Registros: {len(df_check)}")
        print(f"   Columnas: {len(df_check.columns)}")
        print(f"   Archivo: {os.path.basename(filepath)}")
        
        print(f"\n📊 Schema:")
        for col in df_check.columns:
            dtype = str(df_check[col].dtype)
            if 'int' in dtype:
                print(f"   {col:<20} INTEGER")
            elif 'float' in dtype:
                print(f"   {col:<20} FLOAT")
            elif 'datetime' in dtype:
                print(f"   {col:<20} DATETIME")
            else:
                print(f"   {col:<20} STRING")
                
        return True
        
    except Exception as e:
        print(f"✗ Error verificando archivo: {e}")
        return False

def main():
    """Función principal"""
    # Obtener nombre de tabla desde argumentos o usar default
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'vehicle'
    
    print(f"🚀 Iniciando generación de archivo parquet para tabla '{table_name}'...")
    print("=" * 60)
    
    # Cargar configuración
    config = load_config()
    print(f"Base de datos: {config['host']}/{config['dbname']}")
    print(f"Output path: {config['output_path']}")
    print(f"Source name: {config['source_name']}")
    print(f"Dataset name: {table_name}")
    
    # Conectar a PostgreSQL
    engine = create_postgres_connection(config)
    if not engine:
        return False
    
    # Extraer datos
    df = extract_table_data(engine, table_name)
    if df is None or df.empty:
        print("✗ No se pudieron extraer datos")
        return False
    
    # Guardar como parquet en estructura bronze
    filepath = save_parquet(df, config, table_name)
    if not filepath:
        return False
    
    # Verificar archivo
    if verify_parquet(filepath, table_name):
        print(f"\n🎉 ¡Proceso completado exitosamente!")
        print(f"Archivo disponible en: {filepath}")
        return True
    else:
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)