# FILE: T2_api_enrichment.py - LIVE API CALL & THREADING (FINAL VERSION)

import pandas as pd
import numpy as np
import os
import sys
import logging
import requests 
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import create_engine
import db_utils 
import T1_data_cleaning 
import pyarrow 

# =========================================================================
# === KONFIGURASI DAN SETTINGS ===
# =========================================================================
LOG = logging.getLogger("T2_api_enrichment")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

STAGING_SCHEMA = 'staging'
INPUT_CLEANED_TABLE = 'cleaned_delivery_data' 
OUTPUT_ENRICHED_TABLE = 'enriched_data' 
PARQUET_PATH = '/opt/airflow/local_staging/enriched_data.parquet'

MAX_WORKERS = 5 
BATCH_SIZE = 50 
SLEEP_TIME = 1.0 

API_KEY = os.getenv("OPENWEATHER_API_KEY", "").strip() 
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

if not API_KEY:
    LOG.error("❌ ERROR: OPENWEATHER_API_KEY is not set. Cannot proceed with live API call.")
    sys.exit(1)

weather_cache = {} 


# =========================================================================
# === HELPER FUNCTIONS ===
# =========================================================================

def query_weather(lat, lon):
    key = (round(float(lat), 6), round(float(lon), 6))
    if key in weather_cache:
        return key, weather_cache[key]
    
    try:
        resp = requests.get(BASE_URL, params={
            "lat": key[0],
            "lon": key[1],
            "appid": API_KEY, 
            "units": "metric"
        }, timeout=10)
        
        if resp.status_code == 200:
            data = resp.json()
            weather_cache[key] = {
                "temp": data.get("main", {}).get("temp"),
                "humidity": data.get("main", {}).get("humidity"),
                "weather": (data.get("weather") or [{}])[0].get("main")
            }
        else:
            LOG.warning("API query failed for %s (Status: %s)", key, resp.status_code)
            weather_cache[key] = {"temp": None, "humidity": None, "weather": None}
            
    except Exception as e:
        LOG.warning("Failed for %s: %s", key, e)
        weather_cache[key] = {"temp": None, "humidity": None, "weather": None}
        
    return key, weather_cache[key]

def get_current_weather_data(r):
    key = (round(float(r.latitude), 6), round(float(r.longitude), 6))
    return weather_cache.get(key, {})

def save_to_parquet(df, path):
    try:
        df.to_parquet(path, index=False, engine='pyarrow', compression='snappy')
        LOG.info("✅ Data enriched berhasil disimpan ke lokal Parquet: %s", path)
    except Exception as e:
        LOG.error("❌ ERROR saat menyimpan Parquet: %s", e)


# =========================================================================
# === MAIN EXECUTION: T2 ENRICHMENT ===
# =========================================================================

def enrich_and_load_staging():
    LOG.info("--- T2: Memulai Enrichment Data (Live API) ---")
    
    db_name = os.environ.get("POSTGRES_DB", "airflow")
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    engine_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    engine = create_engine(engine_url)
    
    try:
        sql_query = f"SELECT * FROM {STAGING_SCHEMA}.{INPUT_CLEANED_TABLE};"
        df_main = pd.read_sql(sql_query, engine)
        LOG.info("Loaded cleaned data from Staging: %d rows.", len(df_main))
        
        if df_main.empty:
            LOG.warning("Data bersih kosong. Tidak ada yang bisa diperkaya.")
            sys.exit(0)
        
        df_main[['latitude','longitude']] = df_main[['latitude','longitude']].astype(float).round(6)
        unique_coords = df_main[['latitude','longitude']].drop_duplicates().values.tolist()
        LOG.info("Found %d unique coordinates to query.", len(unique_coords))

        total_unique = len(unique_coords)
        total_batches = (total_unique + BATCH_SIZE - 1) // BATCH_SIZE 

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            
            for i in range(0, total_unique, BATCH_SIZE):
                batch_number = (i // BATCH_SIZE) + 1
                batch = unique_coords[i:i + BATCH_SIZE]
                LOG.info("Processing batch %d of %d (size: %d)...", batch_number, total_batches, len(batch))

                batch_futures = [executor.submit(query_weather, lat, lon) for lat, lon in batch]
                futures.extend(batch_futures)
                
                if batch_number < total_batches:
                    time.sleep(SLEEP_TIME)

            for f in as_completed(futures):
                f.result() 

        LOG.info("Finished querying %d unique weather points. Cache size: %d", total_unique, len(weather_cache))

        df_main['weather_data'] = df_main.apply(get_current_weather_data, axis=1)

        for col in ["temp","humidity","weather"]:
            df_main[col] = df_main['weather_data'].apply(lambda d: d.get(col))

        df_main = df_main.drop(columns=['weather_data'], errors='ignore')
        
        with engine.begin() as conn:
            df_main.to_sql(
                OUTPUT_ENRICHED_TABLE, 
                conn, 
                schema=STAGING_SCHEMA, 
                if_exists='replace', 
                index=False
            )
        LOG.info("✅ Data diperkaya (Enriched Data) berhasil dimuat ke %s.%s", STAGING_SCHEMA, OUTPUT_ENRICHED_TABLE)
        
        save_to_parquet(df_main, PARQUET_PATH)
        
    except Exception as e:
        LOG.error("❌ ERROR T2: Gagal dalam Enrichment dan Load ke Staging. Detail: %s", e)
        LOG.error(sys.exc_info()[2])
        sys.exit(1)
        
    finally:
        if engine:
            engine.dispose()
        LOG.info("--- T2: Enrichment Selesai ---")

if __name__ == '__main__':
    enrich_and_load_staging()