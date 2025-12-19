# FILE: T1_transform_and_clean.py (KOREKSI FINAL: JAGA KOLOM PARSED DATE)

import pandas as pd
import numpy as np
from datetime import datetime
import warnings
import os
import sys
from sqlalchemy import create_engine
import db_utils 

warnings.filterwarnings('ignore')

STAGING_SCHEMA = 'staging'
INPUT_TABLE = 'raw_delivery_data' 
OUTPUT_CLEANED_TABLE = 'cleaned_delivery_data' 

# =========================================================================
# 1. LOAD DATA DARI POSTGRES STAGING AREA
# =========================================================================
def load_data_from_db():
    print("="*60)
    print(f"--- 1. LOAD DATA dari {STAGING_SCHEMA}.{INPUT_TABLE} ---")
    
    db_name = os.environ.get("POSTGRES_DB", "airflow")
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    engine_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    
    engine = create_engine(engine_url)
    try:
        sql_query = f"SELECT * FROM {STAGING_SCHEMA}.{INPUT_TABLE};"
        df = pd.read_sql(sql_query, engine)
        initial_rows = len(df)
        print(f"✅ 1. Data Raw Loaded: {initial_rows} rows from PostgreSQL")
        return df, initial_rows 
    except Exception as e:
        print(f"❌ ERROR: Failed to load data from DB. Detail: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()

df, initial_rows = load_data_from_db()
if df is None or df.empty:
    sys.exit(1)


# =========================================================================
# 3. DATA STANDARDIZATION 
# =========================================================================
print("\n--- 3. DATA STANDARDIZATION (ID Conversion) ---")

id_cols = ['customer_id', 'product_card_id', 'order_id']
for col in id_cols:
    if col in df.columns:
        df[col] = df[col].astype(str).apply(lambda x: x.split('.')[0] if pd.notnull(x) and '.' in x else x)


# =========================================================================
# 2. DATA CLEANING & FILTERING
# =========================================================================
print("\n--- 2. DATA CLEANING & FILTERING ---")
df_current = len(df)

df.drop_duplicates(subset=['order_id', 'customer_id', 'product_card_id'], inplace=True)
dropped_duplicates = df_current - len(df)
df_current = len(df)
print(f" 2.1. Duplicates removed: {dropped_duplicates} rows. Remaining: {df_current} rows.")

important_cols = ['order_date', 'shipping_date', 'sales', 'order_item_product_price']
df_before_null_drop = len(df)
df.dropna(subset=important_cols, inplace=True)
dropped_nulls = df_before_null_drop - len(df)
df_current = len(df)
print(f" 2.2. Nulls in critical cols removed: {dropped_nulls} rows. Remaining: {df_current} rows.")

profit_col = 'profit_per_order' 
df_before_profit_clean = len(df)

Q1 = df[profit_col].quantile(0.25)
Q3 = df[profit_col].quantile(0.75)
IQR = Q3 - Q1
LOWER_BOUND = Q1 - (100 * IQR) 
UPPER_BOUND = Q3 + (10 * IQR) 

mask_valid_profit = (df[profit_col] >= LOWER_BOUND) & (df[profit_col] <= UPPER_BOUND)

mask_valid_margin = (df['profit_per_order'] < df['sales']) 

final_profit_mask = mask_valid_profit & mask_valid_margin

df.dropna(subset=[profit_col], inplace=True) 
df = df[final_profit_mask].copy()

dropped_profit_outliers = df_before_profit_clean - len(df)
df_current = len(df)
print(f" 2.3-2.4. Profit Integrity Check (Outlier & Margin > 100%) removed: {dropped_profit_outliers} rows. Remaining: {df_current} rows.")

# =========================================================================
# 4. DATA VALIDATION
# =========================================================================
print("\n--- 4. DATA VALIDATION (Temporal & Categorical) ---")

def parse_datetime_with_timezone(date_str):
    try:
        if pd.isna(date_str):
            return pd.NaT
        date_str = str(date_str)
        date_str_clean = date_str.split('+')[0].strip()
        return pd.to_datetime(date_str_clean, errors='coerce').normalize() 
    except:
        return pd.NaT

df['order_date_parsed'] = df['order_date'].apply(parse_datetime_with_timezone)
df['shipping_date_parsed'] = df['shipping_date'].apply(parse_datetime_with_timezone)

parsed_order = df['order_date_parsed'].notna().sum()
parsed_shipping = df['shipping_date_parsed'].notna().sum()
print(f" 4.1. Date parsing success: Order({parsed_order}/{df_current}), Shipping({parsed_shipping}/{df_current}).")

mask_valid_dates = df['shipping_date_parsed'] >= df['order_date_parsed']
anomalous_dates_initial = (~mask_valid_dates).sum()
print(f" 4.2. Temporal anomaly (Shipping < Order): {anomalous_dates_initial} rows flagged.")

df_before_payment_clean = df['payment_type'].nunique()
valid_payment = ['CASH', 'DEBIT', 'PAYMENT', 'TRANSFER']
df['payment_type'] = df['payment_type'].astype(str).str.upper() 
df.loc[~df['payment_type'].isin(valid_payment), 'payment_type'] = np.nan
df_after_payment_clean = df['payment_type'].nunique()
print(f" 4.3. Payment Type cleaned. Unique values before: {df_before_payment_clean}. Unique values after: {df_after_payment_clean}.")

# =========================================================================
# 5. CREATE DERIVED COLUMNS & FINAL ANOMALY CHECK
# =========================================================================
print("\n--- 5. FEATURE ENGINEERING & FINAL CHECK ---")

df['delivery_status_binary'] = df['label'].astype('Int64')
print(f" 5.1. Target variable 'delivery_status_binary' created.")

mask_valid = df['order_date_parsed'].notna() & df['shipping_date_parsed'].notna()
df.loc[mask_valid, 'delivery_time'] = (
    df.loc[mask_valid, 'shipping_date_parsed'] - df.loc[mask_valid, 'order_date_parsed']
).dt.days
df.loc[~mask_valid, 'delivery_time'] = np.nan

negative_mask = (df['delivery_time'] < 0) & df['delivery_time'].notna()
anomalous_dates_negative = negative_mask.sum()
df.loc[negative_mask | (~mask_valid_dates), 'delivery_time'] = np.nan 
print(f" 5.3. Final time anomalies (negative selisih): {anomalous_dates_negative} rows flagged.")

total_anomalies_flagged = df['delivery_time'].isna().sum()
print(f" Total rows flagged as invalid time for DW: {total_anomalies_flagged} rows.")

# =========================================================================
# 6. ENRICHMENT - COMPOSITE KEY & FINAL CLEANUP
# =========================================================================
print("\n--- 6. COMPOSITE KEY GENERATION & FINAL CLEANUP ---")

df['shipping_date_clean'] = df['shipping_date_parsed'].dt.date.astype(str)
df['weather_join_key'] = df['shipping_date_clean'] + '_' + df['order_state']
print(f" 6.1. 'weather_join_key' created successfully.")

cols_to_drop = ['order_date', 'shipping_date', 'shipping_date_clean'] 
df_final = df.drop(columns=cols_to_drop, errors='ignore').copy()

df_final = df_final[df_final['delivery_time'].notna()].copy()

# =========================================================================
# 7. LOAD CLEANED DATA KE STAGING BARU
# =========================================================================
def load_cleaned_data_to_db(df):
    db_name = os.environ.get("POSTGRES_DB", "airflow")
    user = os.environ.get("POSTGRES_USER", "airflow")
    password = os.environ.get("POSTGRES_PASSWORD", "airflow")
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    engine_url = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"
    
    engine = create_engine(engine_url)
    try:
        print(f"\n--- 7. LOADING {len(df)} baris ke {STAGING_SCHEMA}.{OUTPUT_CLEANED_TABLE} ---")
        
        df.to_sql(
            OUTPUT_CLEANED_TABLE, 
            engine, 
            schema=STAGING_SCHEMA, 
            if_exists='replace',
            index=False,
            chunksize=10000 
        )
        print(f"✅ Data bersih berhasil dimuat ke {STAGING_SCHEMA}.{OUTPUT_CLEANED_TABLE}")
    except Exception as e:
        print(f"❌ ERROR: Gagal memuat data bersih ke DB. Detail: {e}")
        sys.exit(1)
    finally:
        if engine:
            engine.dispose()
            
load_cleaned_data_to_db(df_final)

print("="*60)
print(f"✅ Transform T1 completed. Total rows loaded: {initial_rows}. Final cleaned rows: {len(df_final)}.")
print("="*60)