# etl_supabase.py
import os
import pandas as pd
import cdsapi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
import xarray as xr

# --- CREAR .cdsapirc DINÁMICAMENTE ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ.get('CDSAPI_URL')}\n")
    f.write(f"key: {os.environ.get('CDSAPI_KEY')}\n")

# --- VARIABLES DE CONEXIÓN A SUPABASE ---
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME")

# --- CONEXIÓN ---
def crear_engine():
    conexion_str = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(conexion_str, connect_args={'sslmode': 'require'})

# --- DESCARGA DE DATOS COPERNICUS ERA5 LAND ---
def descargar_datos():
    print("🌍 Descargando datos desde Copernicus CDS...")
    c = cdsapi.Client()

    archivo_salida = "era5_land_daily.nc"

    # Empieza con el mes actual menos 1
    fecha = datetime.now() - timedelta(days=1)
    year = fecha.year
    month = fecha.month

    # Retrocede hasta encontrar datos válidos
    for _ in range(3):  # intentará hasta 3 meses hacia atrás
        try:
            dias_validos = [f"{d:02d}" for d in range(1, 29)]
            c.retrieve(
                "reanalysis-era5-land-timeseries",
                {
                    "variable": [
                        "2m_temperature",
                        "total_precipitation",
                        "surface_pressure",
                        "surface_solar_radiation_downwards",
                    ],
                    "year": str(year),
                    "month": str(month),
                    "day": dias_validos,
                    "time": ["00:00"],
                    "latitude": 13.7,
                    "longitude": -89.2,
                    "format": "netcdf",
                },
                archivo_salida
            )
            print(f"✅ Datos descargados en {archivo_salida} ({year}-{month:02d})")
            return archivo_salida
        except Exception as e:
            print(f"⚠️ No hay datos para {year}-{month:02d}: {e}")
            month -= 1
            if month == 0:
                month = 12
                year -= 1

    print("❌ No se pudieron obtener datos de los últimos 3 meses.")
    return None


# --- PROCESAR Y CARGAR A SUPABASE ---
def procesar_y_cargar(archivo):
    if not archivo:
        print("⚠️ No hay archivo para procesar. ETL detenido.")
        return

    try:
        print("⚙️ Procesando archivo NetCDF...")
        ds = xr.open_dataset(archivo)
        df = ds.to_dataframe().reset_index()

        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df["fecha_actualizacion"] = datetime.utcnow()

        engine = crear_engine()
        nombre_tabla = "era5_land_data"

        df.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
        print(f"✅ Datos cargados correctamente en la tabla '{nombre_tabla}'.")

    except SQLAlchemyError as e:
        print(f"❌ Error al subir datos a Supabase: {e}")
    except Exception as e:
        print(f"❌ Error procesando datos: {e}")

# --- FLUJO PRINCIPAL ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL diario...")
    archivo = descargar_datos()
    procesar_y_cargar(archivo)
    print("🎯 ETL completado.")
