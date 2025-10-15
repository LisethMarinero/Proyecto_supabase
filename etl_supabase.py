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
os.environ["CDSAPI_KEY"] = "da593dcf-84ac-4790-a785-9aca76da8fee"  # 🔹 Reemplaza por tu API key

os.environ["DB_USER"] = "postgres.gkzvbidocktfkwhvngpg"
os.environ["DB_PASSWORD"] = "Hipopotamo123456"
os.environ["DB_HOST"] = "aws-1-us-east-2.pooler.supabase.com"
os.environ["DB_PORT"] = "6543"
os.environ["DB_NAME"] = "postgres"

# --- CREAR .cdsapirc DINÁMICAMENTE ---
cdsapi_path = os.path.expanduser("~/.cdsapirc")
with open(cdsapi_path, "w") as f:
    f.write(f"url: {os.environ.get('CDSAPI_URL')}\n")
    f.write(f"key: {os.environ.get('CDSAPI_KEY')}\n")

# --- CONEXIÓN A SUPABASE ---
def crear_engine():
    conexion_str = f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    return create_engine(conexion_str, connect_args={'sslmode': 'require'})

# --- DESCARGA DE DATOS POR MES ---
def descargar_datos(años):
    print("🌍 Descargando datos desde Copernicus CDS (ERA5-Land)...")
    c = cdsapi.Client()

    años = range(2005, 2026)
    archivos = []

    for year in años:
        for month in range(1, 13):
            archivo_salida = f"reanalysis-era5-land_{year}_{month:02d}.nc"
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
                            # 🔹 Agrega más variables si tu cuota lo permite
                        ],
                        'year': [str(year)],
                        'month': [f"{month:02d}"],
                        'day': [f"{d:02d}" for d in range(1, 32)],
                        'time': ['00:00'],
                        'area': [14, -90, 13, -89]  # [N, W, S, E] ejemplo El Salvador
                    },
                    archivo_salida
                )
                print(f"✅ Datos descargados para {year}-{month:02d}: {archivo_salida}")
                archivos.append(archivo_salida)
            except Exception as e:
                print(f"⚠️ No se pudo descargar datos para {year}-{month:02d}: {e}")

    return archivos

# --- PROCESAR Y CARGAR A SUPABASE ---
def procesar_y_cargar(archivos):
    if not archivos:
        print("⚠️ No hay archivos para procesar. ETL detenido.")
        return

    engine = crear_engine()
    nombre_tabla = "reanalysis_era5_land"

    for archivo in archivos:
        try:
            print(f"⚙️ Procesando {archivo}...")
            ds = xr.open_dataset(archivo)
            df = ds.to_dataframe().reset_index()
            df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]
            df["fecha_actualizacion"] = datetime.utcnow()

            df.to_sql(nombre_tabla, engine, if_exists="append", index=False)
            print(f"✅ Datos cargados correctamente desde {archivo}")

        except SQLAlchemyError as e:
            print(f"❌ Error al subir datos a Supabase desde {archivo}: {e}")
        except Exception as e:
            print(f"❌ Error procesando {archivo}: {e}")

# --- FLUJO PRINCIPAL ---
if __name__ == "__main__":
    print("🚀 Iniciando ETL completo (2005-2025)...")
    años = range(2005, 2026)
    archivos = descargar_datos(años)
    procesar_y_cargar(archivos)
    print("🎯 ETL completado con éxito.")
