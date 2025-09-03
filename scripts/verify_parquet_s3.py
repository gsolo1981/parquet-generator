#!/usr/bin/env python3
"""
Script para verificar archivos parquet generados en S3
Uso: python scripts/verify_parquet_s3.py [table_name] [date]
"""

import pandas as pd
import boto3
import io
import sys
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from botocore.exceptions import ClientError, NoCredentialsError

def load_config():
    """Cargar configuración desde .env"""
    load_dotenv()
    
    return {
        'aws_access_key_id': os.getenv('aws_access_key_id'),
        'aws_secret_access_key': os.getenv('aws_secret_access_key'),
        's3_bucket': 'strix-production-datalake-for-growth',
        'aws_region': 'us-east-1',
        'source_name': 'magenta'
    }

def create_s3_client(config):
    """Crear cliente S3"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config['aws_access_key_id'],
            aws_secret_access_key=config['aws_secret_access_key'],
            region_name=config['aws_region']
        )
        
        # Verificar conexión
        s3_client.head_bucket(Bucket=config['s3_bucket'])
        return s3_client
        
    except NoCredentialsError:
        print("❌ Error: Credenciales de AWS no configuradas")
        return None
    except ClientError as e:
        print(f"❌ Error conectando a S3: {e}")
        return None

def list_files_for_date(s3_client, config, table_name, target_date):
    """Listar archivos parquet para una tabla y fecha específica"""
    prefix = f"bronze/{config['source_name']}/{table_name}/execution_date={target_date}/"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=config['s3_bucket'],
            Prefix=prefix
        )
        
        if 'Contents' not in response:
            return []
        
        files = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'],
                    'filename': os.path.basename(obj['Key'])
                })
        
        return files
        
    except Exception as e:
        print(f"❌ Error listando archivos: {e}")
        return []

def get_available_dates(s3_client, config, table_name, days_back=7):
    """Obtener fechas disponibles para una tabla"""
    base_prefix = f"bronze/{config['source_name']}/{table_name}/"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=config['s3_bucket'],
            Prefix=base_prefix,
            Delimiter='/'
        )
        
        dates = []
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                # Extraer fecha del prefix execution_date=YYYY-MM-DD/
                prefix_path = prefix['Prefix']
                if 'execution_date=' in prefix_path:
                    date_part = prefix_path.split('execution_date=')[1].rstrip('/')
                    dates.append(date_part)
        
        # Ordenar fechas (más recientes primero)
        dates.sort(reverse=True)
        return dates[:days_back]  # Últimos N días
        
    except Exception as e:
        print(f"❌ Error obteniendo fechas disponibles: {e}")
        return []

def verify_parquet_content(s3_client, config, file_info, sample_size=1000):
    """Verificar el contenido de un archivo parquet"""
    try:
        # Descargar archivo
        obj = s3_client.get_object(Bucket=config['s3_bucket'], Key=file_info['key'])
        
        # Leer parquet
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        
        # Información básica
        info = {
            'records': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'null_counts': df.isnull().sum().to_dict(),
            'data_types': df.dtypes.to_dict()
        }
        
        if len(df) > 0:
            info['sample_data'] = df.head(min(sample_size, len(df)))
        
        return info, None
        
    except Exception as e:
        # Muestra de datos
        return None, str(e)

def print_file_verification(file_info, content_info, error):
    """Imprimir información de verificación de un archivo"""
    print(f"\n📄 Archivo: {file_info['filename']}")
    print(f"   📍 S3 Key: {file_info['key']}")
    print(f"   📊 Tamaño: {file_info['size'] / 1024 / 1024:.2f} MB")
    print(f"   🕒 Modificado: {file_info['last_modified']}")
    
    if error:
        print(f"   ❌ Error leyendo contenido: {error}")
        return False
    
    if content_info:
        print(f"   📈 Registros: {content_info['records']:,}")
        print(f"   🏛️ Columnas: {content_info['columns']}")
        print(f"   💾 Memoria: {content_info['memory_usage_mb']:.2f} MB")
        
        # Mostrar tipos de datos
        print(f"   🏗️ Schema:")
        for col, dtype in content_info['data_types'].items():
            null_count = content_info['null_counts'].get(col, 0)
            null_pct = (null_count / content_info['records']) * 100 if content_info['records'] > 0 else 0
            print(f"      {col:<25} {str(dtype):<15} (nulls: {null_count:,} - {null_pct:.1f}%)")
        
        # Verificaciones de calidad
        print(f"   ✅ Verificaciones:")
        checks = []
        
        # Check 1: Archivo no vacío
        if content_info['records'] > 0:
            checks.append("✅ Archivo contiene datos")
        else:
            checks.append("❌ Archivo está vacío")
        
        # Check 2: Tiene columnas
        if content_info['columns'] > 0:
            checks.append("✅ Archivo tiene columnas")
        else:
            checks.append("❌ Archivo sin columnas")
        
        # Check 3: Tamaño razonable
        if file_info['size'] > 1024:  # Más de 1KB
            checks.append("✅ Archivo tiene tamaño razonable")
        else:
            checks.append("⚠️ Archivo muy pequeño")
        
        # Check 4: No todas las columnas son null
        total_nulls = sum(content_info['null_counts'].values())
        total_cells = content_info['records'] * content_info['columns']
        if total_cells > 0 and (total_nulls / total_cells) < 0.9:
            checks.append("✅ Datos no están completamente vacíos")
        elif total_cells > 0:
            checks.append("⚠️ Demasiados valores nulos")
        
        for check in checks:
            print(f"      {check}")
        
        return all("✅" in check for check in checks)
    
    return False

def print_summary(table_name, target_date, files, verifications):
    """Imprimir resumen de la verificación"""
    print(f"\n" + "="*80)
    print(f"📊 RESUMEN DE VERIFICACIÓN")
    print(f"="*80)
    print(f"📋 Tabla: {table_name}")
    print(f"📅 Fecha: {target_date}")
    print(f"📁 Archivos encontrados: {len(files)}")
    
    if not files:
        print("❌ No se encontraron archivos para esta fecha")
        return
    
    total_size = sum(f['size'] for f in files) / 1024 / 1024
    total_records = sum(v[0]['records'] if v[0] else 0 for v in verifications)
    successful_verifications = sum(1 for v in verifications if v[0] and not v[1])
    
    print(f"💾 Tamaño total: {total_size:.2f} MB")
    print(f"📈 Total de registros: {total_records:,}")
    print(f"✅ Verificaciones exitosas: {successful_verifications}/{len(files)}")
    
    if successful_verifications == len(files):
        print(f"\n🎉 ¡Todos los archivos están OK!")
    elif successful_verifications > 0:
        print(f"\n⚠️ Algunos archivos tienen problemas")
    else:
        print(f"\n❌ Todos los archivos tienen problemas")

def main():
    """Función principal"""
    # Argumentos
    table_name = sys.argv[1] if len(sys.argv) > 1 else None
    target_date = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    
    if not table_name:
        print("❌ Debe especificar el nombre de la tabla")
        print("Uso: python verify_parquet_s3.py [table_name] [date]")
        print("Ejemplo: python verify_parquet_s3.py vehicles 2024-01-15")
        return False
    
    print(f"🔍 Verificando archivos parquet en S3...")
    print(f"📋 Tabla: {table_name}")
    print(f"📅 Fecha: {target_date}")
    print("="*80)
    
    # Cargar configuración
    config = load_config()
    
    # Conectar a S3
    s3_client = create_s3_client(config)
    if not s3_client:
        return False
    
    print(f"✅ Conectado a S3: {config['s3_bucket']}")
    
    # Buscar archivos para la fecha especificada
    files = list_files_for_date(s3_client, config, table_name, target_date)
    
    if not files:
        print(f"\n❌ No se encontraron archivos para {table_name} en fecha {target_date}")
        
        # Mostrar fechas disponibles
        print(f"\n🔍 Buscando fechas disponibles...")
        available_dates = get_available_dates(s3_client, config, table_name)
        if available_dates:
            print(f"📅 Fechas disponibles para '{table_name}':")
            for date in available_dates:
                date_files = list_files_for_date(s3_client, config, table_name, date)
                print(f"   {date}: {len(date_files)} archivo(s)")
        else:
            print(f"❌ No se encontraron datos para la tabla '{table_name}'")
        
        return False
    
    print(f"📁 Encontrados {len(files)} archivo(s)")
    
    # Verificar cada archivo
    verifications = []
    for i, file_info in enumerate(files, 1):
        print(f"\n🔍 Verificando archivo {i}/{len(files)}...")
        
        content_info, error = verify_parquet_content(s3_client, config, file_info)
        verifications.append((content_info, error))
        
        is_ok = print_file_verification(file_info, content_info, error)
        
        if is_ok:
            print(f"   🎉 Archivo OK")
        else:
            print(f"   ❌ Archivo con problemas")
    
    # Resumen final
    print_summary(table_name, target_date, files, verifications)
    
    # Retornar True si todos los archivos están OK
    all_ok = all(v[0] and not v[1] for v in verifications)
    return all_ok

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)