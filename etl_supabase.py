# etl_supabase_ultimo_dia.py
import os
import cdsapi
import pandas as pd
import xarray as xr
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
import pytz

# --- CONFIGURACIÓN ---
os.environ["CDSAPI_URL"] = "https://cds.climate.copernicus.eu/api"
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"  # 🔹 Asegúrate de poner tu UID:APIKEY correcto

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# --- CREAR .cdsapirc ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ['CDSAPI_URL']}\n")
    f.write(f"key: {os.environ['CDSAPI_KEY']}\n")

# --- CONEXIÓN SUPABASE ---
def crear_engine():
    conexion = (
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(conexion, connect_args={'sslmode': 'require'})

# --- DESCARGA DEL ÚLTIMO DÍA ---
def descargar_ultimo_dia(fecha):
    año, mes, dia = fecha.year, fecha.month, fecha.day
    archivo = f"reanalysis-era5-land_{año}_{mes:02d}_{dia:02d}.nc"

    if os.path.exists(archivo):
        print(f"ℹ️ Archivo ya existe: {archivo}, se omite descarga.")
        return archivo

    print(f"🌍 Descargando datos ERA5-Land para {año}-{mes:02d}-{dia:02d}...")
    c = cdsapi.Client()
    try:
        c.retrieve(
            'reanalysis-era5-land',
            {
                'format': 'netcdf',
                'variable': [
                    "2m_temperature",
                    "2m_dewpoint_temperature",
                    "surface_pressure",
                    "total_precipitation"
                ],
                'year': [str(año)],
                'month': [f"{mes:02d}"],
                'day': [f"{dia:02d}"],
                'time': ['00:00'],
                'area': [14, -90, 13, -89],  # El Salvador
            },
            archivo
        )
        print(f"✅ Archivo descargado: {archivo}")
        return archivo
    except Exception as e:
        print(f"⚠️ Error descargando {archivo}: {e}")
        return None

# --- PROCESAR Y CARGAR ---
def procesar_y_cargar(archivo):
    if not archivo or not os.path.exists(archivo):
        print("⚠️ No hay archivo válido para procesar.")
        return

    if os.path.getsize(archivo) < 1000:
        print(f"⚠️ Archivo inválido o vacío: {archivo}")
        return

    try:
        print(f"⚙️ Procesando {archivo}...")
        ds = xr.open_dataset(archivo, engine="netcdf4", decode_cf=True)

        if not ds.variables:
            print(f"⚠️ Archivo sin variables: {archivo}")
            return

        df = ds.to_dataframe().reset_index()
        df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
        df["fecha_actualizacion"] = datetime.now(pytz.UTC)

        engine = crear_engine()
        nombre_tabla = "reanalysis_era5_land"
        df.to_sql(nombre_tabla, engine, if_exists="append", index=False)
        print(f"✅ Datos cargados en Supabase: {archivo} ({len(df)} filas)")
    except Exception as e:
        print(f"❌ Error procesando {archivo}: {e}")

# --- EJECUCIÓN PRINCIPAL ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL último día ERA5-Land...")
    fecha_ultimo_dia = datetime(2025, 10, 10)  # 🔹 Cambia a la última fecha que quieras
    archivo = descargar_ultimo_dia(fecha_ultimo_dia)
    procesar_y_cargar(archivo)
    print("🎯 ETL completado con éxito.")
