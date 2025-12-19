# FILE: T4_prepare_ml_dataset.py (FINAL FIX: Cleaning Invalid Target & Correct Logging)

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import os
import sys
from collections import Counter
import logging
from imblearn.over_sampling import SMOTE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DB_USER = os.getenv('POSTGRES_USER', 'airflow')
DB_PASS = os.getenv('POSTGRES_PASSWORD', 'airflow')
DB_HOST = os.getenv('POSTGRES_HOST', 'postgres')
DB_NAME = os.getenv('POSTGRES_DB', 'airflow')

DB_CONNECTION_STRING = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}"
OUTPUT_FILE = "/tmp/ml_dataset_ready.csv"
TARGET_COLUMN = "is_delayed"

logger.info("=" * 70)
logger.info("🚀 T4: STARTING ML DATASET PREPARATION (Optimized)")
logger.info("=" * 70)

try:
    engine = create_engine(DB_CONNECTION_STRING, pool_pre_ping=True)
    logger.info(f"🌐 Database engine created. Using host: {DB_HOST}")
except Exception as e:
    logger.error(f"❌ ERROR creating database engine: {e}")
    sys.exit(1)

SQL_QUERY = """
WITH MarketDelay AS (
    SELECT
        d_m.market,
        CAST(COUNT(CASE WHEN f.delivery_status_binary = 1 THEN 1 END) AS FLOAT) /
        NULLIF(COUNT(f.delivery_status_binary), 0) AS market_delayed_rate
    FROM dw.fact_shipments f
    JOIN dw.dim_market d_m ON f.market_key = d_m.market_key
    GROUP BY 1
)
SELECT
    f.delivery_status_binary AS is_delayed,
    f.order_item_quantity,
    f.order_item_discount_rate,
    f.order_item_profit_ratio,
    f.sales,
    f.profit_per_order,
    f.order_item_product_price,
    f.sales / NULLIF(f.order_item_quantity, 0) AS sales_per_quantity,
    f.profit_per_order / NULLIF(f.order_item_quantity, 0) AS profit_per_quantity,
    d_t.year AS order_year,
    d_t.month AS order_month,
    d_t.day AS order_day,
    d_t.order_date,
    CASE WHEN d_t.month IN (11, 12) THEN 1 ELSE 0 END AS is_peak_month,
    d_c.customer_segment,
    d_p.product_price,
    d_p.department_name,
    d_s.shipping_mode,
    d_m.market,
    d_m.order_region,
    md.market_delayed_rate,
    d_w.temp,
    d_w.humidity
FROM dw.fact_shipments f
LEFT JOIN dw.dim_shipping d_s ON f.shipping_key = d_s.shipping_key
LEFT JOIN dw.dim_market d_m ON f.market_key = d_m.market_key
LEFT JOIN dw.dim_time d_t ON f.date_key = d_t.date_key
LEFT JOIN dw.dim_customer d_c ON f.customer_key = d_c.customer_key
LEFT JOIN dw.dim_product d_p ON f.product_key = d_p.product_key
LEFT JOIN dw.dim_weather d_w ON f.weather_key = d_w.weather_key
LEFT JOIN MarketDelay md ON d_m.market = md.market;
"""

try:
    df_ml = pd.read_sql(text(SQL_QUERY), engine.connect())
    logger.info(f"📥 Extracted rows: {len(df_ml)}")
except Exception as e:
    logger.error(f"❌ ERROR extracting data: {e}")
    sys.exit(1)

if df_ml.empty:
    logger.error("❌ DATASET EMPTY. No ML dataset can be generated.")
    sys.exit(0)

df_ml['order_date'] = pd.to_datetime(df_ml['order_date'], errors='coerce')
df_ml['is_weekend'] = df_ml['order_date'].dt.dayofweek.apply(lambda x: 1 if x >= 5 else 0)
df_ml['total_value'] = df_ml['order_item_product_price'] * df_ml['order_item_quantity']
df_ml['effective_discount'] = df_ml['order_item_discount_rate'] * df_ml['order_item_quantity']
df_ml['price_ratio_vs_total'] = df_ml['order_item_product_price'] / df_ml['total_value']
df_ml['price_ratio_vs_total'] = df_ml['price_ratio_vs_total'].fillna(0)

cols_to_impute = [
    'temp',
    'humidity',
    'market_delayed_rate',
    'order_item_quantity',
    'order_item_discount_rate',
    'order_item_product_price',
    'sales',
    'profit_per_order'
]

for col in cols_to_impute:
    if col in df_ml.columns:
        df_ml[col] = pd.to_numeric(df_ml[col], errors='coerce').fillna(0)

df_ml['is_hot'] = df_ml['temp'].apply(lambda t: 1 if t >= 30 else 0)
df_ml['is_cold'] = df_ml['temp'].apply(lambda t: 1 if t <= 10 else 0)

df_ml = df_ml.drop(columns=['order_date'], errors='ignore')

rows_before = len(df_ml)
df_ml[TARGET_COLUMN] = df_ml[TARGET_COLUMN].replace(-1, 0)
df_ml = df_ml[df_ml[TARGET_COLUMN].isin([0, 1])].copy()
rows_after = len(df_ml)

logger.info(f"Rows dropped (invalid target values/NaN): {rows_before - rows_after}")

if df_ml.empty:
    logger.error("❌ All rows dropped after cleaning. No dataset available.")
    sys.exit(0)

X = df_ml.drop(columns=[TARGET_COLUMN])
y = df_ml[TARGET_COLUMN]

categorical_cols = X.select_dtypes(include=["object"]).columns.tolist()
X_encoded = pd.get_dummies(X, columns=categorical_cols, dummy_na=False, drop_first=True)
X_encoded[TARGET_COLUMN] = y

try:
    smote = SMOTE(random_state=42)
    X_res, y_res = smote.fit_resample(
        X_encoded.drop(columns=[TARGET_COLUMN]),
        X_encoded[TARGET_COLUMN]
    )
    df_final = pd.concat([X_res, y_res], axis=1)
except ImportError:
    logger.error("❌ ERROR: Imbalanced-learn (SMOTE) not installed. Skipping balancing.")
    df_final = X_encoded

status_counts = Counter(df_final[TARGET_COLUMN])

delayed_count = status_counts.get(1, 0)
on_time_count = status_counts.get(0, 0)
total = delayed_count + on_time_count

delayed_percent = (delayed_count / total) * 100
on_time_percent = (on_time_count / total) * 100

logger.info(f"Delayed(1): {delayed_count} ({delayed_percent:.2f}%)")
logger.info(f"On Time(0): {on_time_count} ({on_time_percent:.2f}%)")

try:
    df_final.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"💾 ML dataset saved → {OUTPUT_FILE}")
    logger.info("🎯 T4 DONE — Ready for training.")
except Exception as e:
    logger.error(f"❌ ERROR saving ML dataset: {e}")
    sys.exit(1)
