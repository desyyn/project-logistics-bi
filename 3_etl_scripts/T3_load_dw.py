# FILE: T3_load_dw.py (FINAL DATA WAREHOUSE LOAD - ALL DIMS & FACT TO PARQUET)

import pandas as pd
import numpy as np
import warnings
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import logging
import pyarrow 

warnings.filterwarnings('ignore')

# ==================== LOGGER ====================
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ==================== KONFIGURASI ELT ====================
STAGING_SCHEMA = 'staging'
INPUT_TABLE = 'enriched_data' 
DW_SCHEMA = 'dw' 
PARQUET_BASE_PATH = '/opt/airflow/local_staging'
PARQUET_FACT_PATH = os.path.join(PARQUET_BASE_PATH, 'dw_fact_shipments.parquet')


# ==================== DATABASE CONNECTION ====================
DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')

DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"

# ==================== UTILITY FUNCTIONS ====================

def get_db_engine():
    return create_engine(DB_CONNECTION_STRING)

def save_to_parquet(df, path):
    try:
        df.to_parquet(path, index=False, engine='pyarrow', compression='snappy')
        logger.info(f"✅ Tabel (%d baris) berhasil disimpan ke Parquet: %s", len(df), path)
        return True
    except Exception as e:
        logger.error("❌ ERROR saat menyimpan DW ke Parquet: %s", e)
        return False

# ==================== DIMENSION CREATION FUNCTIONS ====================
def create_dim_market(df):
    dim = df[['market','order_region']].drop_duplicates(subset=['market']).reset_index(drop=True)
    dim['market_key'] = dim.index + 1
    dim['created_at'] = datetime.now()
    return dim[['market_key','market','order_region','created_at']]

def create_dim_payment(df):
    dim = df[['payment_type']].drop_duplicates(subset=['payment_type']).reset_index(drop=True)
    dim['payment_key'] = dim.index + 1
    dim['created_at'] = datetime.now()
    return dim[['payment_key','payment_type','created_at']]

def create_dim_time(df):
    all_dates = pd.Series(dtype='datetime64[ns]')
    all_dates = pd.concat([all_dates, df['order_date_parsed'].dropna(), df['shipping_date_parsed'].dropna()], ignore_index=True)

    all_dates = all_dates.dropna().drop_duplicates()
    if len(all_dates) == 0:
        all_dates = pd.Series([datetime.now()])
        
    dim = pd.DataFrame({'order_date': all_dates})
    dim['order_date'] = pd.to_datetime(dim['order_date']).dt.normalize()
    dim['date_key'] = dim['order_date'].dt.strftime('%Y%m%d').astype(int)
    dim['year'] = dim['order_date'].dt.year
    dim['month'] = dim['order_date'].dt.month
    dim['quarter'] = dim['order_date'].dt.quarter
    dim['week'] = dim['order_date'].dt.isocalendar().week.astype(int)
    dim['day'] = dim['order_date'].dt.day
    dim['created_at'] = datetime.now()
    return dim[['date_key','order_date','year','month','quarter','week','day','created_at']]

def create_dim_customer(df):
    id_cols = ['customer_id','order_customer_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: x.split('.')[0] if pd.notnull(x) and '.' in x else x)
            
    dim = df[['customer_id','order_customer_id','customer_segment',
             'customer_city','customer_state','customer_country','customer_zipcode']] \
             .drop_duplicates(subset=['customer_id']).reset_index(drop=True)
    dim['customer_key'] = dim.index + 1
    dim['created_at'] = datetime.now()
    return dim[['customer_key','customer_id','order_customer_id','customer_segment',
                 'customer_city','customer_state','customer_country','customer_zipcode','created_at']]

def create_dim_product(df):
    id_cols = ['product_card_id','product_category_id','category_id','department_id']
    for col in id_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).apply(lambda x: x.split('.')[0] if pd.notnull(x) and '.' in x else x)

    dim = df[['product_card_id','product_name','product_category_id','category_id',
             'category_name','department_id','department_name','product_price']] \
             .drop_duplicates(subset=['product_card_id']).reset_index(drop=True)
    dim['product_key'] = dim.index + 1
    dim['created_at'] = datetime.now()
    return dim[['product_key','product_card_id','product_name','product_category_id',
                 'category_id','category_name','department_id','department_name',
                 'product_price','created_at']]

def create_dim_shipping(df):
    shipping_cols = ['shipping_mode','order_city','order_state','order_country','latitude','longitude']
    dim = df[shipping_cols].drop_duplicates(subset=['shipping_mode','order_city','order_state']).reset_index(drop=True)
    dim['shipping_key'] = dim.index + 1
    dim['created_at'] = datetime.now()
    base_cols = ['shipping_key','shipping_mode','order_city','order_state','order_country','latitude','longitude']
    return dim[base_cols + ['created_at']]

def create_dim_weather(df):
    dim = df[['weather_join_key','temp','humidity','weather']].drop_duplicates(subset=['weather_join_key'])
    dim['weather_key'] = range(1, len(dim)+1)
    dim['created_at'] = datetime.now()
    return dim[['weather_key','weather_join_key','temp','humidity','weather','created_at']]

# ==================== MAIN EXECUTION: T3 LOAD DIMENSION & FACT ====================

def main_load_dw():
    engine = get_db_engine()
    logger.info("🌐 Database Engine Created.")

    try:
        with engine.begin() as connection:
             connection.execute(text(f"CREATE SCHEMA IF NOT EXISTS {DW_SCHEMA};"))
             logger.info("✅ Skema DW dipastikan ada.")

        with engine.begin() as connection:
            logger.warning("⚠️ DROP DEPENDENT OBJECTS: Dropping Fact Table and Views with CASCADE...")
            connection.execute(text(f"DROP TABLE IF EXISTS {DW_SCHEMA}.fact_shipments CASCADE;"))
            for dim_table in ['dim_market', 'dim_payment', 'dim_time', 'dim_customer', 'dim_product', 'dim_shipping', 'dim_weather']:
                connection.execute(text(f"DROP TABLE IF EXISTS {DW_SCHEMA}.{dim_table} CASCADE;"))
            logger.info("✅ Dependent objects dropped. Ready to build.")
        
        sql_query = f"SELECT * FROM {STAGING_SCHEMA}.{INPUT_TABLE};"
        df_main = pd.read_sql(sql_query, engine)
        
        df_main.columns = df_main.columns.str.lower() 

        df_main['order_date_parsed'] = pd.to_datetime(df_main['order_date_parsed'], errors='coerce').dt.normalize()
        df_main['shipping_date_parsed'] = pd.to_datetime(df_main['shipping_date_parsed'], errors='coerce').dt.normalize()
        
        logger.info(f"✅ Loaded ENRICHED data from Staging: {len(df_main)} rows")

        # ==================== BUILD DIMENSIONS ====================
        logger.info("🏗 CREATING DIMENSIONS...")
        
        dims = {}
        dims['dim_market'] = create_dim_market(df_main)
        dims['dim_payment'] = create_dim_payment(df_main)
        dims['dim_time'] = create_dim_time(df_main) 
        dims['dim_customer'] = create_dim_customer(df_main)
        dims['dim_product'] = create_dim_product(df_main)
        dims['dim_shipping'] = create_dim_shipping(df_main)
        dims['dim_weather'] = create_dim_weather(df_main) 

        for table_name, df_dim in dims.items():
            df_dim.to_sql(table_name, engine, schema=DW_SCHEMA, if_exists='append', index=False)
            logger.info("-> Dimensi %s dimuat: %d baris.", table_name, len(df_dim))


        # ==================== CREATE FACT TABLE ====================
        logger.info("📊 CREATING FACT TABLE...")
        
        df_fact = df_main.copy()
        
        df_fact = df_fact.merge(dims['dim_customer'][['customer_id','customer_key']], on='customer_id', how='left')
        df_fact = df_fact.merge(dims['dim_product'][['product_card_id','product_key']], on='product_card_id', how='left')
        df_fact = df_fact.merge(dims['dim_market'][['market','market_key']], on='market', how='left')
        df_fact = df_fact.merge(dims['dim_payment'][['payment_type','payment_key']], on='payment_type', how='left')
        df_fact = df_fact.merge(dims['dim_shipping'][['shipping_mode','order_city','order_state','shipping_key']], 
                                     on=['shipping_mode','order_city','order_state'], how='left')
        df_fact = df_fact.merge(dims['dim_weather'][['weather_join_key','weather_key']], on='weather_join_key', how='left')

        df_time_lookup = dims['dim_time'][['order_date', 'date_key']].rename(columns={'order_date': 'shipping_date_parsed_lookup'})
        df_fact = df_fact.merge(df_time_lookup, left_on='shipping_date_parsed', right_on='shipping_date_parsed_lookup', how='left')
        df_fact = df_fact.drop(columns=['shipping_date_parsed_lookup'], errors='ignore') 
        
        cols_to_keep = ['customer_key', 'product_key', 'market_key', 'payment_key', 'shipping_key', 'weather_key', 'date_key',
                         'order_item_quantity', 'order_item_discount', 'order_item_discount_rate', 
                         'order_item_product_price', 'order_item_profit_ratio', 'order_profit_per_order', 
                         'profit_per_order', 'sales', 'sales_per_customer', 'order_status', 
                         'delivery_time', 'delivery_status_binary', 'label']

        df_fact_final = df_fact[cols_to_keep].copy()
        
        # ==================== LOAD FACT TABLE ====================
        df_fact_final.to_sql('fact_shipments', engine, schema=DW_SCHEMA, if_exists='replace', index=False)
        logger.info(f"✅ fact_shipments loaded to PostgreSQL ({len(df_fact_final)} rows).")

        # ==================== EKSTRAKSI KE PARQUET (DW Layer) ====================
        
        save_to_parquet(df_fact_final, PARQUET_FACT_PATH)
        
        for table_name, df_dim in dims.items():
            dim_parquet_path = os.path.join(PARQUET_BASE_PATH, f"{table_name}.parquet")
            save_to_parquet(df_dim, dim_parquet_path)
            
        logger.info("🎉 DATA WAREHOUSE LOAD TO POSTGRESQL & PARQUET COMPLETED SUCCESSFULLY!")
        
    except Exception as e:
        logger.error(f"❌ ERROR T3: Gagal memuat DW. Detail: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()

if __name__ == '__main__':
    main_load_dw()