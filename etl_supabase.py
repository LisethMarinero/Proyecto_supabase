# etl_supabase.py
import os
import pandas as pd
import cdsapi
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import xarray as xr

# --- CONFIGURACIÓN MANUAL (solo para pruebas locales) ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "TU_EMAIL:TU_API_KEY"  # 🔹 reemplaza por tu clave real

os.environ["DB_USER"] = "postgres"
os.environ["DB_PASSWORD"] = "TU_CONTRASEÑA_SUPABASE"
os.environ["DB_HOST"] = "TU_HOST_SUPABASE.supabase.co"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

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

    archivo_salida = "era5_land_2005_2025.nc"

    try:
        c.retrieve(
            "reanalysis-era5-land-timeseries",
            {
                "variable": [
                    "2m_dewpoint_temperature",
                    "2m_temperature",
                    "surface_pressure",
                    "total_precipitation",
                    "surface_solar_radiation_downwards",
                    "surface_thermal_radiation_downwards",
                    "skin_temperature",
                    "snow_cover",
                    "soil_temperature_level_1",
                    "soil_temperature_level_2",
                    "soil_temperature_level_3",
                    "soil_temperature_level_4",
                    "volumetric_soil_water_level_1",
                    "volumetric_soil_water_level_2",
                    "volumetric_soil_water_level_3",
                    "volumetric_soil_water_level_4",
                    "10m_u_component_of_wind",
                    "10m_v_component_of_wind"
                ],
                "latitude": 13.8,
                "longitude": -89.5,
                "date": "2005-01-01/2025-10-02",  # 🔹 rango de fechas completo
                "time": ["00:00"],
                "format": "netcdf",
            },
            archivo_salida
        )
        print(f"✅ Datos descargados correctamente en {archivo_salida}")
        return archivo_salida

    except Exception as e:
        print(f"❌ Error descargando datos: {e}")
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

        # Normalizar nombres de columnas
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
    print("🚀 Iniciando ETL completo (2005-2025)...")
    archivo = descargar_datos()
    procesar_y_cargar(archivo)
    print("🎯 ETL completado con éxito.")
