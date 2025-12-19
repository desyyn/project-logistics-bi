-- ===============================================
-- DDL untuk Logistik DW (Berdasarkan Python ETL Skrip Anda)
-- ===============================================

-- ===============================================
-- 1. DROP TABEL (Bersihkan Skema DW dan Staging)
-- ===============================================

DROP TABLE IF EXISTS dw.fact_shipments CASCADE; 

DROP TABLE IF EXISTS staging.raw_delivery_data CASCADE; 

DROP TABLE IF EXISTS dw.dim_customer CASCADE;
DROP TABLE IF EXISTS dw.dim_product CASCADE;
DROP TABLE IF EXISTS dw.dim_market CASCADE;
DROP TABLE IF EXISTS dw.dim_payment CASCADE;
DROP TABLE IF EXISTS dw.dim_shipping CASCADE;
DROP TABLE IF EXISTS dw.dim_weather CASCADE;
DROP TABLE IF EXISTS dw.dim_time CASCADE;

-- ===============================================
-- 2. SETUP SCHEMA
-- ===============================================

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;


-- ===============================================
-- 3. CREATE TABEL STAGING (Opsional, untuk memuat data mentah)
-- ===============================================
CREATE TABLE staging.raw_delivery_data (
    payment_type VARCHAR(50),
    profit_per_order DOUBLE PRECISION,    
    sales_per_customer DOUBLE PRECISION,    
    category_id DOUBLE PRECISION,           
    category_name VARCHAR(100),
    customer_city VARCHAR(100),
    customer_country VARCHAR(100),
    customer_id DOUBLE PRECISION,           
    customer_segment VARCHAR(50),
    customer_state VARCHAR(50),
    customer_zipcode DOUBLE PRECISION,      
    department_id DOUBLE PRECISION,         
    department_name VARCHAR(100),
    latitude DOUBLE PRECISION,             
    longitude DOUBLE PRECISION,             
    market VARCHAR(50),
    order_city VARCHAR(100),
    order_country VARCHAR(100),
    order_customer_id DOUBLE PRECISION,     
    order_date VARCHAR(255),                
    order_id DOUBLE PRECISION,              
    order_item_cardprod_id DOUBLE PRECISION, 
    order_item_discount DOUBLE PRECISION,  
    order_item_discount_rate DOUBLE PRECISION, 
    order_item_id DOUBLE PRECISION,        
    order_item_product_price DOUBLE PRECISION, 
    order_item_profit_ratio DOUBLE PRECISION, 
    order_item_quantity DOUBLE PRECISION, 
    sales DOUBLE PRECISION,                
    order_item_total_amount DOUBLE PRECISION, 
    order_profit_per_order DOUBLE PRECISION,  
    order_region VARCHAR(50),
    order_state VARCHAR(50),
    order_status VARCHAR(50),
    product_card_id DOUBLE PRECISION,       
    product_category_id DOUBLE PRECISION,   
    product_name VARCHAR(255),
    product_price DOUBLE PRECISION,       
    shipping_date VARCHAR(255),         
    shipping_mode VARCHAR(50),
    label DOUBLE PRECISION            
);


-- ===============================================
-- 4. CREATE TABEL DIMENSI
-- ===============================================

-- DIMENSI WAKTU (dim_time) 
CREATE TABLE dw.dim_time (
    date_key INT PRIMARY KEY, 
    order_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, 
    year INT NOT NULL,
    month INT NOT NULL,
    quarter INT NOT NULL,
    week INT NOT NULL,
    day INT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI CUSTOMER (dim_customer) 
CREATE TABLE dw.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id NUMERIC(15, 0) NOT NULL,
    order_customer_id NUMERIC(15, 0),
    customer_segment VARCHAR(50),
    customer_city VARCHAR(100),
    customer_state VARCHAR(50),
    customer_country VARCHAR(100),
    customer_zipcode NUMERIC(10, 0),
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI PRODUCT (dim_product)
CREATE TABLE dw.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_card_id NUMERIC(15, 0) NOT NULL,
    product_name VARCHAR(255),
    product_category_id NUMERIC(10, 0),
    category_id NUMERIC(10, 0),
    category_name VARCHAR(100),
    department_id NUMERIC(10, 0),
    department_name VARCHAR(100),
    product_price NUMERIC(15, 4),
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI MARKET (dim_market)
CREATE TABLE dw.dim_market (
    market_key SERIAL PRIMARY KEY,
    market VARCHAR(50) NOT NULL,
    order_region VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI PAYMENT (dim_payment)
CREATE TABLE dw.dim_payment (
    payment_key SERIAL PRIMARY KEY,
    payment_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI SHIPPING (dim_shipping) 
CREATE TABLE dw.dim_shipping (
    shipping_key SERIAL PRIMARY KEY,
    shipping_mode VARCHAR(50) NOT NULL,
    order_city VARCHAR(100),
    order_state VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE
);

-- DIMENSI WEATHER (dim_weather) 
CREATE TABLE dw.dim_weather (
    weather_key SERIAL PRIMARY KEY,
    composite_key VARCHAR(50), 
    temp NUMERIC(5, 2),
    humidity NUMERIC(5, 2),
    weather VARCHAR(50),
    created_at TIMESTAMP WITHOUT TIME ZONE
);


-- ===============================================
-- 5. CREATE TABEL FAKTA (fact_shipments)
-- ===============================================
CREATE TABLE dw.fact_shipments (
    fact_key SERIAL PRIMARY KEY, 
    
    date_key INT REFERENCES dw.dim_time(date_key), 
    customer_key INT REFERENCES dw.dim_customer(customer_key),
    product_key INT REFERENCES dw.dim_product(product_key),
    market_key INT REFERENCES dw.dim_market(market_key),
    payment_key INT REFERENCES dw.dim_payment(payment_key),
    shipping_key INT REFERENCES dw.dim_shipping(shipping_key),
    weather_key INT REFERENCES dw.dim_weather(weather_key),

    order_item_quantity INT,
    order_item_discount NUMERIC(15, 4),
    order_item_discount_rate NUMERIC(5, 4),
    order_item_product_price NUMERIC(15, 4),
    order_item_profit_ratio NUMERIC(5, 4),
    order_profit_per_order NUMERIC(15, 4),
    profit_per_order NUMERIC(15, 4),
    sales NUMERIC(15, 4),
    sales_per_customer NUMERIC(15, 4),
    
    order_status VARCHAR(50),
    delivery_status_binary INT, 
    label INT 
);

-- ===============================================
-- 6. VIEW untuk Dataset ML (Memenuhi Aturan UAS Poin 2)
-- ===============================================

CREATE OR REPLACE VIEW dw.vw_ml_otd_dataset AS
SELECT
    f.label AS target_otd,
    f.order_item_quantity,
    f.sales,
    dc.customer_segment,
    dp.category_name AS product_category,
    dmk.market,
    dsh.shipping_mode,
    dw.temp AS weather_temp,
    dw.humidity AS weather_humidity
FROM
    dw.fact_shipments f
JOIN
    dw.dim_customer dc ON f.customer_key = dc.customer_key
JOIN
    dw.dim_product dp ON f.product_key = dp.product_key
JOIN
    dw.dim_market dmk ON f.market_key = dmk.market_key
JOIN
    dw.dim_shipping dsh ON f.shipping_key = dsh.shipping_key
JOIN
    dw.dim_weather dw ON f.weather_key = dw.weather_key
;