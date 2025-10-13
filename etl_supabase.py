# etl_supabase.py
import os
import pandas as pd
import cdsapi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import xarray as xr
from calendar import monthrange

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

    # Calcular último día válido del mes
    year = datetime.now().year
    month = datetime.now().month
    last_day = monthrange(year, month)[1]
    dias_validos = [f"{d:02d}" for d in range(1, last_day+1)]

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
            "format": "netcdf",
        },
        archivo_salida
    )
    print(f"✅ Datos descargados en {archivo_salida}")
    return archivo_salida

# --- PROCESAR Y CARGAR A SUPABASE ---
def procesar_y_cargar(archivo):
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
    print("🎯 ETL completado exitosamente.")
