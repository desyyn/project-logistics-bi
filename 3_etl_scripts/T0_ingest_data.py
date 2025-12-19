#D:\SEMESTER 5\Kecerdasan Bisnis\UAS\project-logistics\3_etl_scripts\T0_ingest_data.py
import pandas as pd
import os
import sys
import psycopg2
from sqlalchemy import create_engine
import db_utils 

RAW_FILE_PATH = '/opt/airflow/data_sources/df_raw.csv'
STAGING_TABLE = 'raw_delivery_data'
STAGING_SCHEMA = 'staging'

def ingest_raw_data():
    print(f"--- T0: Memulai Ingest Data Mentah ke {STAGING_SCHEMA}.{STAGING_TABLE} ---")
    
    if not os.path.exists(RAW_FILE_PATH):
        print(f"Error: File raw data tidak ditemukan di {RAW_FILE_PATH}")
        sys.exit(1)

    try:
        df_raw = pd.read_csv(RAW_FILE_PATH)
        print(f"Data CSV dimuat. Jumlah baris: {len(df_raw)}")
    except Exception as e:
        print(f"Error saat membaca CSV: {e}")
        sys.exit(1)

    conn_raw = None
    engine = None
    try:
        db_name = os.environ.get("POSTGRES_DB", "airflow")
        user = os.environ.get("POSTGRES_USER", "airflow")
        password = os.environ.get("POSTGRES_PASSWORD", "airflow")
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        
        conn_raw = db_utils.get_postgres_conn()
        cursor = conn_raw.cursor()
        
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {STAGING_SCHEMA};")
        print(f"Skema {STAGING_SCHEMA} dipastikan ada.")
        
        conn_raw.commit()
        
        try:
            cursor.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.{STAGING_TABLE};")
            print(f"Tabel {STAGING_SCHEMA}.{STAGING_TABLE} berhasil di TRUNCATE.")
            conn_raw.commit() 
            
            to_sql_if_exists_mode = 'append'
            
        except psycopg2.ProgrammingError as pe:
            if 'does not exist' in str(pe):
                print("Tabel belum ada. Pandas to_sql akan membuatnya.")
                to_sql_if_exists_mode = 'replace' 
            else:
                raise pe 
        
        engine_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
        engine = create_engine(engine_url)
        
        df_raw.to_sql(
            STAGING_TABLE, 
            engine, 
            schema=STAGING_SCHEMA, 
            if_exists=to_sql_if_exists_mode, 
            index=False,
            chunksize=10000 
        )
        
        conn_raw.commit() 
        
        print(f"Berhasil memuat {len(df_raw)} baris ke {STAGING_SCHEMA}.{STAGING_TABLE}")

    except Exception as e:
        print(f"Error saat menjalankan Ingest Data: {e}")
        if conn_raw:
            conn_raw.rollback()
        sys.exit(1)
        
    finally:
        if conn_raw:
            conn_raw.close()
        if engine:
            engine.dispose()
        print("--- T0: Ingest Selesai ---")

if __name__ == '__main__':
    ingest_raw_data()