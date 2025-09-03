#!/usr/bin/env python3
"""
Script standalone para generar archivos parquet con estructura bronze y subirlos a S3
Uso: python scripts/generate_parquet_s3.py [table_name]
"""

import pandas as pd
import os
import sys
import boto3
import io
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from botocore.exceptions import ClientError, NoCredentialsError

def load_config():
    """Cargar configuración desde .env"""
    load_dotenv()
    
    return {
        'dbname': os.getenv('CONSUMER_DB_NAME'),
        'host': os.getenv('CONSUMER_DB_HOST'),
        'port': os.getenv('CONSUMER_DB_PORT', '5432'),
        'user': os.getenv('CONSUMER_DB_USER'),
        'password': os.getenv('CONSUMER_DB_PASSWORD'),
        'compression': os.getenv('PARQUET_COMPRESSION', 'snappy'),
        'source_name': 'magenta',
        'aws_access_key_id': os.getenv('aws_access_key_id'),
        'aws_secret_access_key': os.getenv('aws_secret_access_key'),
        's3_bucket': 'strix-production-datalake-for-growth',
        'aws_region': 'us-east-1'  # Región por defecto
    }

def create_postgres_connection(config):
    """Crear conexión a PostgreSQL"""
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{config['user']}:{config['password']}@"
            f"{config['host']}:{config['port']}/{config['dbname']}"
        )
        print("✅ Conexión a PostgreSQL establecida.")
        return engine
    except SQLAlchemyError as e:
        print(f"❌ Error en conexión a PostgreSQL: {e}")
        return None

def create_s3_client(config):
    """Crear cliente S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key'],
            region_name=config['aws_region']
        )
        
        # Verificar conexión listando el bucket
        s3_client.head_bucket(Bucket=config['s3_bucket'])
        print("✅ Conexión a S3 establecida.")
        return s3_client
        
    except NoCredentialsError:
        print("❌ Error: Credenciales de AWS no configuradas")
        return None
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            print(f"❌ Error: Bucket '{config['s3_bucket']}' no encontrado")
        else:
            print(f"❌ Error conectando a S3: {e}")
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
        print(f"❌ Tabla '{table_name}' no está configurada")
        print(f"  Tablas disponibles: {list(queries.keys())}")
        return None
    
    query = queries[table_name]
    
    try:
        print(f"→ Ejecutando consulta para tabla '{table_name}'...")
        df = pd.read_sql(query, engine)
        print(f"✅ Datos extraídos: {len(df)} registros")
        return df
    except Exception as e:
        print(f"❌ Error extrayendo datos de '{table_name}': {e}")
        return None

def create_s3_path(config, table_name):
    """Crear la ruta S3 con estructura bronze"""
    execution_date = datetime.now().strftime("%Y-%m-%d")
    
    # Estructura: bronze/{source_name}/{dataset_name}/execution_date=YYYY-MM-DD/
    s3_path = f"bronze/{config['source_name']}/{table_name}/execution_date={execution_date}/"
    
    return s3_path

def upload_parquet_to_s3(df, s3_client, config, table_name):
    """Convertir DataFrame a parquet y subirlo a S3"""
    s3_path = create_s3_path(config, table_name)
    
    # Nombre del archivo con timestamp
    timestamp = datetime.now().strftime("%H%M%S")
    filename = f"{table_name}_{timestamp}.parquet"
    s3_key = s3_path + filename
    
    try:
        # Crear buffer en memoria para el archivo parquet
        parquet_buffer = io.BytesIO()
        
        # Guardar DataFrame como parquet en el buffer
        print(f"→ Convirtiendo {len(df)} registros a formato parquet...")
        df.to_parquet(
            parquet_buffer,
            index=False,
            engine='pyarrow',
            compression=config['compression']
        )
        
        # Obtener tamaño antes de resetear posición
        buffer_size = parquet_buffer.tell()
        file_size_mb = buffer_size / 1024 / 1024
        print(f"→ Archivo parquet generado: {file_size_mb:.2f} MB")
        
        # Resetear posición del buffer
        parquet_buffer.seek(0)
        
        # Subir a S3
        print(f"→ Subiendo archivo a S3...")
        
        # Usar put_object en lugar de upload_fileobj para mayor control
        parquet_data = parquet_buffer.getvalue()
        
        s3_client.put_object(
            Bucket=config['s3_bucket'],
            Key=s3_key,
            Body=parquet_data,
            ContentType='application/octet-stream'
        )
        
        s3_url = f"s3://{config['s3_bucket']}/{s3_key}"
        
        print(f"✅ Archivo subido a S3: {s3_url}")
        print(f"  Tamaño: {file_size_mb:.2f} MB")
        print(f"  Estructura: /{s3_path}")
        
        # Cerrar buffer explícitamente
        parquet_buffer.close()
        
        return s3_url, s3_key
        
    except Exception as e:
        print(f"❌ Error subiendo archivo a S3: {e}")
        print(f"   Detalles del error: {type(e).__name__}")
        
        # Información adicional para debugging
        print(f"   DataFrame info: {len(df)} filas, {len(df.columns)} columnas")
        print(f"   Columnas: {list(df.columns)}")
        
        # Cerrar buffer si existe
        try:
            parquet_buffer.close()
        except:
            pass
            
        return None, None

def verify_s3_file(s3_client, config, s3_key, table_name):
    """Verificar el archivo parquet en S3"""
    try:
        # Obtener metadatos del archivo
        response = s3_client.head_object(Bucket=config['s3_bucket'], Key=s3_key)
        file_size_mb = response['ContentLength'] / 1024 / 1024
        
        print(f"\n📋 Verificación del archivo '{table_name}' en S3:")
        print(f"   Bucket: {config['s3_bucket']}")
        print(f"   Key: {s3_key}")
        print(f"   Tamaño: {file_size_mb:.2f} MB")
        print(f"   Última modificación: {response['LastModified']}")
        
        # Opcional: descargar y verificar contenido (solo para archivos pequeños)
        if file_size_mb < 50:  # Solo para archivos menores a 50MB
            try:
                obj = s3_client.get_object(Bucket=config['s3_bucket'], Key=s3_key)
                df_check = pd.read_parquet(io.BytesIO(obj['Body'].read()))
                
                print(f"\n📊 Schema del archivo:")
                print(f"   Registros: {len(df_check)}")
                print(f"   Columnas: {len(df_check.columns)}")
                
                for col in df_check.columns[:10]:  # Mostrar solo primeras 10 columnas
                    dtype = str(df_check[col].dtype)
                    if 'int' in dtype:
                        print(f"   {col:<20} INTEGER")
                    elif 'float' in dtype:
                        print(f"   {col:<20} FLOAT")
                    elif 'datetime' in dtype:
                        print(f"   {col:<20} DATETIME")
                    else:
                        print(f"   {col:<20} STRING")
                        
                if len(df_check.columns) > 10:
                    print(f"   ... y {len(df_check.columns) - 10} columnas más")
                    
            except Exception as e:
                print(f"   Nota: No se pudo verificar el contenido completo: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificando archivo en S3: {e}")
        return False

def main():
    """Función principal"""
    # Obtener nombre de tabla desde argumentos
    table_name = sys.argv[1] if len(sys.argv) > 1 else 'vehicles'
    
    print(f"🚀 Iniciando generación de archivo parquet para tabla '{table_name}'...")
    print("=" * 70)
    
    # Cargar configuración
    config = load_config()
    print(f"Base de datos: {config['host']}/{config['dbname']}")
    print(f"S3 Bucket: {config['s3_bucket']}")
    print(f"Source name: {config['source_name']}")
    print(f"Dataset name: {table_name}")
    print("")
    
    # Conectar a PostgreSQL
    engine = create_postgres_connection(config)
    if not engine:
        return False
    
    # Conectar a S3
    s3_client = create_s3_client(config)
    if not s3_client:
        return False
    
    print("")
    
    # Extraer datos
    df = extract_table_data(engine, table_name)
    if df is None or df.empty:
        print("❌ No se pudieron extraer datos")
        return False
    
    # Subir archivo parquet a S3
    s3_url, s3_key = upload_parquet_to_s3(df, s3_client, config, table_name)
    if not s3_url:
        return False
    
    # Verificar archivo en S3
    if verify_s3_file(s3_client, config, s3_key, table_name):
        print(f"\n🎉 ¡Proceso completado exitosamente!")
        print(f"Archivo disponible en: {s3_url}")
        return True
    else:
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)