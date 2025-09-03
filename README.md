# 🚀 Parquet Generator

Proyecto containerizado para extraer datos de PostgreSQL y generar archivos Parquet optimizados.

## 📋 Características

- ✅ **Dockerizado**: Entorno aislado y reproducible
- ✅ **Jupyter Lab**: Interface web interactiva
- ✅ **Script automático**: Generación por línea de comandos
- ✅ **PostgreSQL**: Conexión a AWS RDS
- ✅ **Optimizado**: Archivos Parquet comprimidos
- ✅ **Verificación**: Validación automática de datos

## 📁 Estructura del Proyecto

```
parquet-generator/
├── docker-compose.yml      # Configuración de servicios
├── Dockerfile             # Imagen personalizada
├── requirements.txt       # Dependencias Python
├── .env.example          # Template de variables
├── .env                  # Variables de entorno (crear)
├── notebooks/            # Jupyter notebooks
│   └── generate_parquet.ipynb
├── scripts/              # Scripts standalone
│   └── generate_parquet.py
├── output/              # Archivos parquet generados
└── README.md            # Este archivo
```

## 🚀 Instalación y Uso

### 1. Preparar el proyecto

```bash
# Crear directorio del proyecto
mkdir parquet-generator && cd parquet-generator

# Crear estructura de carpetas
mkdir notebooks scripts output

# Descargar todos los archivos en sus respectivas ubicaciones
```

### 2. Configurar variables de entorno

```bash
# Copiar template
cp .env.example .env

# Editar con tus credenciales de PostgreSQL
nano .env
```

**Contenido de `.env`:**
```bash
CONSUMER_DB_NAME=tu_base_datos
CONSUMER_DB_HOST=tu-host.rds.amazonaws.com
CONSUMER_DB_PORT=5432
CONSUMER_DB_USER=tu_usuario
CONSUMER_DB_PASSWORD=tu_password
OUTPUT_PATH=/app/output
PARQUET_COMPRESSION=snappy
```

### 3. Levantar el proyecto

```bash
# Construir y levantar contenedores
docker-compose up -d

# Ver logs (opcional)
docker-compose logs -f
```

### 4. Acceder a Jupyter Lab

- **URL**: http://localhost:8888
- **Token**: `parquet123`

## 🎯 Formas de Uso

### Opción A: Jupyter Notebook (Interactivo)

1. Abrir http://localhost:8888
2. Navegar a `notebooks/generate_parquet.ipynb`
3. Ejecutar las celdas paso a paso
4. Ver análisis y verificaciones en tiempo real

### Opción B: Script Automático

```bash
# Ejecutar script desde fuera del contenedor
docker-compose exec jupyter python scripts/generate_parquet.py

# O desde dentro del contenedor
docker-compose exec jupyter bash
python scripts/generate_parquet.py
```

## 📊 Output

Los archivos Parquet se generan en:
- **Contenedor**: `/app/output/`
- **Host**: `./output/`
- **Formato**: `vehicles_YYYYMMDD_HHMMSS.parquet`

**Ejemplo:**
```
output/
└── vehicles_20250125_143022.parquet  (45.2 MB)
```

## 🔧 Comandos Útiles

```bash
# Ver archivos generados
ls -la output/

# Acceder al contenedor
docker-compose exec jupyter bash

# Ver logs en tiempo real
docker-compose logs -f jupyter

# Reiniciar servicios
docker-compose restart

# Parar el proyecto
docker-compose down

# Rebuilding con cambios
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## 📈 Esquema de Datos

El archivo Parquet contiene las siguientes columnas:

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id | STRING | ID único del vehículo |
| account_id | STRING | ID de la cuenta |
| make | STRING | Marca del vehículo |
| year | INTEGER | Año del modelo |
| color | STRING | Color del vehículo |
| label | STRING | Etiqueta/nombre |
| model | STRING | Modelo del vehículo |
| domain | STRING | Dominio/región |
| subtype | STRING | Subtipo de vehículo |
| engine_number | STRING | Número de motor |
| chassis_number | STRING | Número de chasis |
| mileage | FLOAT | Kilometraje |
| latitude | FLOAT | Latitud GPS |
| longitude | FLOAT | Longitud GPS |
| things | STRING | Datos adicionales |
| location_datetime | DATETIME | Timestamp de ubicación |
| created_datetime | DATETIME | Timestamp de creación |

## 🔍 Verificaciones Automáticas

El sistema incluye verificaciones de:

- ✅ **Conectividad**: Conexión a PostgreSQL
- ✅ **Integridad**: IDs duplicados y valores nulos
- ✅ **Coordenadas**: Rangos válidos de lat/lng
- ✅ **Formato**: Estructura del archivo Parquet
- ✅ **Tamaño**: Compresión y estadísticas

## 🐛 Troubleshooting

### Error de conexión a PostgreSQL
```bash
# Verificar variables de entorno
docker-compose exec jupyter env | grep CONSUMER

# Probar conexión manualmente
docker-compose exec jupyter python -c "
from scripts.generate_parquet import load_config, create_postgres_connection
config = load_config()
engine = create_postgres_connection(config)
print('Conexión:', 'OK' if engine else 'ERROR')
"
```

### Puerto 8888 ocupado
```bash
# Cambiar puerto en docker-compose.yml
ports:
  - "8889:8888"  # Usar puerto 8889
```

### Archivos no se generan
```bash
# Verificar permisos de carpeta output
sudo chmod 777 output/

# Ver logs detallados
docker-compose logs jupyter
```

## 📝 Personalización

### Cambiar la consulta SQL
Editar `scripts/generate_parquet.py` línea 45:

```python
def extract_vehicle_data(engine):
    query = """
    SELECT 
        -- Agregar/quitar columnas aquí
        id,
        account_id,
        -- tu consulta personalizada
    FROM strix.vehicle
    WHERE created_datetime >= NOW() - INTERVAL '30 days'  -- filtro ejemplo
    """
```

### Cambiar configuración de Parquet
Editar `.env`:

```bash
# Opciones: snappy, gzip, lz4, brotli
PARQUET_COMPRESSION=gzip

# Cambiar directorio output
OUTPUT_PATH=/app/output/custom
```

## 🤝 Contribuir

1. Fork el proyecto
2. Crear rama feature (`git checkout -b feature/AmazingFeature`)
3. Commit cambios (`git commit -m 'Add AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abrir Pull Request

## 📞 Soporte

Si encuentras problemas:

1. Revisar logs: `docker-compose logs -f`
2. Verificar `.env` tiene las credenciales correctas
3. Comprobar conectividad a PostgreSQL
4. Asegurar que existe la tabla `strix.vehicle`

---

**¡Listo para generar Parquets! 🎉**