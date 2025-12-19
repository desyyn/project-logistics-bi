#D:\SEMESTER 5\Kecerdasan Bisnis\UAS\project-logistics\6_bi_dashboard\app.py

import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os
import sys
import traceback
from datetime import datetime
import pyarrow 
import altair as alt 

# ==================== CONFIGURATIONS ====================
st.set_page_config(
    layout="wide", 
    page_title="Logistics OTD Dashboard"
)

# -----------------------------------------------------------
STAGING_LOCAL_PATH = '/app/9_staging_local' 

FACT_PARQUET_PATH = os.path.join(STAGING_LOCAL_PATH, 'dw_fact_shipments.parquet') 
DIM_MARKET_PATH = os.path.join(STAGING_LOCAL_PATH, 'dim_market.parquet')
DIM_SHIPPING_PATH = os.path.join(STAGING_LOCAL_PATH, 'dim_shipping.parquet') 
DIM_CUSTOMER_PATH = os.path.join(STAGING_LOCAL_PATH, 'dim_customer.parquet')
MODEL_PATH = os.path.join('/app/5_ml_model/model_artifacts', 'xgb_otd_model.joblib')
# -----------------------------------------------------------


FEATURE_COLS = [
    'shipping_mode_Standard Class', 'profit_per_order', 'profit_per_quantity',
    'order_item_profit_ratio', 'order_day', 'effective_discount',
    'shipping_mode_Second Class', 'order_item_discount_rate', 'order_month', 'sales',
    'profit_over_quantity', 'discount_ratio'
]

# ==================== 1. UTILITY FUNCTIONS ====================

def _load_parquet_file(path, name):
    if not os.path.exists(path):
        st.error(f"❌ File {name} tidak ditemukan di {path}!")
        return None
    try:
        return pd.read_parquet(path, engine='pyarrow')
    except Exception as e:
        st.error(f"❌ ERROR saat memuat {name}: {str(e)}")
        return None

def _create_hist_df(data_series, num_bins=20, name="Frequency"):
    data = data_series.dropna()
    if data.empty:
        return pd.DataFrame()
        
    hist, bin_edges = np.histogram(data, bins=num_bins)
    bin_labels = [f"${b:,.0f}" for b in bin_edges[:-1]]
    
    hist_df = pd.DataFrame(hist, index=bin_labels, columns=[name])
    return hist_df


@st.cache_data(ttl=300)
def load_data():
    """Load dan gabungkan Fact dan Dimension data dari Parquet lokal."""
    try:
        with st.spinner("📥 Loading and joining data..."):

            df_main = _load_parquet_file(FACT_PARQUET_PATH, "dw_fact_shipments")
            if df_main is None:
                return pd.DataFrame()

            df_dim_market = _load_parquet_file(DIM_MARKET_PATH, "dim_market")
            df_dim_shipping = _load_parquet_file(DIM_SHIPPING_PATH, "dim_shipping")
            df_dim_customer = _load_parquet_file(DIM_CUSTOMER_PATH, "dim_customer") 

            if df_dim_market is None or df_dim_shipping is None:
                st.warning(
                    "Gagal memuat semua file dimensi. Analisis mungkin tidak lengkap."
                )

            if df_dim_market is not None:
                df_main = df_main.merge(
                    df_dim_market[["market_key", "market"]],
                    on="market_key",
                    how="left",
                )

            if df_dim_shipping is not None:
                df_main = df_main.merge(
                    df_dim_shipping[["shipping_key", "shipping_mode"]],
                    on="shipping_key",
                    how="left",
                )
            
            if df_dim_customer is not None and 'customer_key' in df_main.columns:
                customer_cols_to_merge = ['customer_key', 'customer_segment']
                customer_cols_to_merge = [col for col in customer_cols_to_merge if col in df_dim_customer.columns]
                
                if len(customer_cols_to_merge) > 1:
                    df_main = df_main.merge(
                        df_dim_customer[customer_cols_to_merge].drop_duplicates(subset=['customer_key']),
                        on="customer_key",
                        how="left",
                    )
                else:
                    st.warning("Kolom 'customer_segment' tidak ditemukan di `dim_customer.parquet`.")

            if "date_key" in df_main.columns:
                df_main["order_date"] = pd.to_datetime(
                    df_main["date_key"], format="%Y%m%d", errors="coerce"
                )
            else:
                df_main["order_date"] = datetime.now()

            st.sidebar.success(
                f"✅ Data loaded and joined: {len(df_main):,} records"
            )

            return df_main

    except Exception as e:
        st.error("❌ ERROR: Gagal memuat atau menggabungkan data.")
        st.code(str(e))
        return pd.DataFrame()


@st.cache_resource
def load_model():
    """Load model machine learning dari file joblib lokal."""
    try:
        if not os.path.exists(MODEL_PATH):
            st.error(f"❌ Model file tidak ditemukan di {MODEL_PATH}!")
            st.info("Pastikan 'xgb_otd_model.joblib' ada di folder '5_ml_model/model_artifacts'.")
            return None
            
        with st.spinner("🤖 Loading ML model..."):
            model = joblib.load(MODEL_PATH)
        
        st.sidebar.success(f"✅ Model loaded: {type(model).__name__}")
        
        return model
        
    except Exception as e:
        st.error(f"❌ Gagal memuat model: {str(e)}")
        return None

def create_ml_features(data_input):
    """Create features untuk prediksi ML dari input data, meniru preprocessing T4."""
    try:
        data = data_input.copy()
        
        # 1. Feature yang Anda sudah buat:
        data['effective_discount'] = data['order_item_discount_rate'] * data['order_item_quantity']
        # KOREKSI: order_item_profit_ratio biasanya dihitung dari profit_per_order / sales,
        # tapi di T4 Anda tidak membuatnya, jadi kita hapus. Jika model Anda dilatih TANPA
        # fitur ini (karena T4 tidak membuatnya), maka harus dihapus di sini juga.
        # Jika Anda yakin fitur ini ada di data training:
        # data['order_item_profit_ratio'] = data['profit_per_order'] / (data['sales'] + 1e-5)
        # Sesuai data T4 yang Anda tunjukkan sebelumnya, mari kita asumsikan fitur tersebut valid
        
        data['order_item_profit_ratio'] = data['profit_per_order'] / (data['sales'] + 1e-5)
        
        # 2. Tambahkan fitur temporal (set ke nilai aman, ini sudah benar)
        # Jika order_date tidak tersedia di form input, kita gunakan nilai default.
        data['order_day'] = 1
        data['order_month'] = 1
        
        # 3. KOREKSI LOGIKA FEATURE BARU (sesuai T4 dan T5)
        data['profit_per_quantity'] = data['profit_per_order'] / (data['order_item_quantity'] + 1e-5)
        data['profit_over_quantity'] = data['profit_per_order'] / (data['profit_per_quantity'] + 1e-5)
        data['discount_ratio'] = data['effective_discount'] / (data['order_item_discount_rate'] + 1e-5)

        # ----------------------------------------------------------------------
        # 4. KOREKSI DUMMY VARIABLES (Meniru T4: get_dummies dan drop_first=True)
        # ----------------------------------------------------------------------
        
        # Simpan semua kemungkinan mode pengiriman dari data historis Anda.
        ALL_SHIPPING_MODES = ['Standard Class', 'Second Class', 'First Class', 'Same Day']
        
        # Konversi kolom ke tipe 'category' dan tambahkan semua kategori yang mungkin
        # agar pd.get_dummies tahu semua kolom yang harus dibuat.
        data['shipping_mode'] = pd.Categorical(
            data['shipping_mode'], categories=ALL_SHIPPING_MODES
        )
        
        # Lakukan One-Hot Encoding (drop_first=True akan menghilangkan salah satu mode)
        # Ini meniru persis langkah T4
        data_ohe = pd.get_dummies(data, columns=['shipping_mode'], drop_first=True)
        
        # 5. Membangun Final Feature Set
        X_pred = pd.DataFrame()
        
        for col in FEATURE_COLS:
            if col in data_ohe.columns:
                # Ambil fitur yang ada dari data yang sudah di-OHE
                X_pred[col] = data_ohe[col]
            else:
                # Jika fitur OHE tidak ada (misalnya: 'shipping_mode_First Class' atau
                # 'shipping_mode_Same Day' yang di-drop), set nilai ke 0.
                # Ini adalah representasi dari kategori baseline.
                X_pred[col] = 0
        
        # Pastikan indeks diset dengan benar
        X_pred.index = [0]
        
        return X_pred[FEATURE_COLS]
        
    except Exception as e:
        # Untuk debugging, cetak trace penuh
        import traceback
        st.error(f"❌ Error dalam feature engineering: {str(e)}")
        st.code(traceback.format_exc())
        return pd.DataFrame(columns=FEATURE_COLS)
    
# ==================== 2. BI DASHBOARD COMPONENTS ====================

def apply_global_filters(df):
    st.sidebar.subheader("🎯 Global Filters")

    markets = sorted(df['market'].dropna().unique()) if 'market' in df.columns else []
    shipping_modes = sorted(df['shipping_mode'].dropna().unique()) if 'shipping_mode' in df.columns else []

    selected_markets = st.sidebar.multiselect("Market", markets, default=markets)
    selected_shipping = st.sidebar.multiselect("Shipping Mode", shipping_modes, default=shipping_modes)

    min_date, max_date = df['order_date'].min(), df['order_date'].max()
    date_range = st.sidebar.date_input(
        "Order Date Range",
        value=(min_date, max_date)
    )

    df_filtered = df.copy()

    if selected_markets:
        df_filtered = df_filtered[df_filtered['market'].isin(selected_markets)]

    if selected_shipping:
        df_filtered = df_filtered[df_filtered['shipping_mode'].isin(selected_shipping)]

    if len(date_range) == 2:
        df_filtered = df_filtered[
            (df_filtered['order_date'] >= pd.to_datetime(date_range[0])) &
            (df_filtered['order_date'] <= pd.to_datetime(date_range[1]))
        ]

    st.sidebar.caption(f"📌 Records after filter: {len(df_filtered):,}")
    return df_filtered


def render_bi_dashboard(df):
    """Render dashboard Business Intelligence"""
    st.header("📊 Business Intelligence Dashboard")
    st.markdown("---")
    
    # Key Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_orders = len(df)
        st.metric(
            label="Total Orders", 
            value=f"{total_orders:,}",
            help="Jumlah total pesanan"
        )
    
    with col2:
        total_sales = df['sales'].sum()
        st.metric(
            label="Total Sales", 
            value=f"$ {total_sales:,.0f}",
            help="Total penjualan"
        )
    
    with col3:
        total_profit = df['profit_per_order'].sum()
        st.metric(
            label="Total Profit", 
            value=f"$ {total_profit:,.0f}",
            help="Total keuntungan"
        )
    
    with col4:
        if total_orders > 0:
            delayed_count = df[df['delivery_status_binary'] == 1].shape[0]
            otd_rate = (total_orders - delayed_count) / total_orders
            st.metric(
                label="On-Time Delivery Rate", 
                value=f"{otd_rate:.2%}",
                delta=f"-{delayed_count} delayed" if delayed_count > 0 else "Perfect",
                help="Persentase pengiriman tepat waktu"
            )
        else:
            st.metric(label="On-Time Delivery Rate", value="N/A")
    
    st.markdown("---")
    
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Performance", "📍 Market Analysis", "🚚 Shipping Analysis", "📅 Time Analysis"])
    
    # =================================================================
    # --- TAB 1: PERFORMANCE (CUSTOMER SEGMENT FOCUS) ---
    # =================================================================
    with tab1:
        st.subheader("Performance Overview by Customer Segment")
        
        if not df.empty and 'customer_segment' in df.columns:
            
            segment_summary = df.groupby('customer_segment').agg(
                Total_Sales=('sales', 'sum'),
                Total_Profit=('profit_per_order', 'sum'),
                Order_Count=('order_item_quantity', 'count')
            ).reset_index()
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Order Volume Distribution by Customer Segment** (Pie Chart)")
                
                base_pie = alt.Chart(segment_summary).encode(
                    theta=alt.Theta("Order_Count", stack=True)
                ).properties(title='Order Count by Segment')

                pie = base_pie.mark_arc(outerRadius=120).encode(
                    color=alt.Color("customer_segment", title='Segment'),
                    order=alt.Order("Order_Count", sort="descending"),
                    tooltip=["customer_segment", alt.Tooltip("Order_Count", format=","), alt.Tooltip("Total_Sales", format="$.0f")]
                )
                
                text = base_pie.mark_text(radius=140).encode(
                    text=alt.Text("Order_Count", format=","),
                    order=alt.Order("Order_Count", sort="descending"),
                    color=alt.value("black") 
                )
                
                st.altair_chart(pie + text, use_container_width=True)
        
            with col2:
                st.markdown("**Total Sales and Profit by Customer Segment** (Bar Chart)")
                
                segment_sales_profit = segment_summary.set_index('customer_segment')[['Total_Sales', 'Total_Profit']]
                st.bar_chart(segment_sales_profit)

        else:
            st.warning("Kolom 'customer_segment' tidak tersedia. Pastikan `dim_customer.parquet` ada dan digabungkan dengan `customer_key`.")
        
        st.markdown("---")
        st.markdown("**Summary Statistics**")
        if not df.empty:
            stats_df = df[['sales', 'profit_per_order', 'order_item_quantity']].describe()
            st.dataframe(stats_df.style.format({
                'sales': '$ {:,.0f}',
                'profit_per_order': '$ {:,.0f}',
                'order_item_quantity': '{:,.0f}'
            }))
    
    # =================================================================
    # --- TAB 2: MARKET ANALYSIS (Menggunakan Market) ---
    # =================================================================
    with tab2:
        st.subheader("Market Performance Analysis")
        
        if not df.empty:
            if 'market' in df.columns:
                 
                market_stats = df.groupby('market').agg({
                    'sales': 'sum',
                    'profit_per_order': 'sum',
                    'order_item_quantity': 'count',
                    'delivery_status_binary': ['sum', 'mean']
                }).round(2)
                
                market_stats.columns = ['Total Sales', 'Total Profit', 'Order Count', 'Delayed Count', 'Delay Rate']
                market_stats['Delay Rate'] = market_stats['Delay Rate'].map(lambda x: f"{x:.2%}")
                
                market_stats['Total Sales'] = market_stats['Total Sales'].map(lambda x: f"$ {x:,.0f}")
                market_stats['Total Profit'] = market_stats['Total Profit'].map(lambda x: f"$ {x:,.0f}")
                
                st.dataframe(market_stats, use_container_width=True)
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Sales by Market**")
                    market_sales = df.groupby('market')['sales'].sum()
                    st.bar_chart(market_sales)
                
                with col2:
                    st.markdown("**Delivery Delay Rate by Market**")
                    market_delay = df.groupby('market')['delivery_status_binary'].mean()
                    st.bar_chart(market_delay)
            else:
                st.info("Kolom 'market' masih hilang setelah mencoba *join*. Cek ketersediaan `dim_market.parquet`.")
    
    # =================================================================
    # --- TAB 3: SHIPPING ANALYSIS (Tidak Berubah) ---
    # =================================================================
    with tab3:
        st.subheader("Shipping Mode Analysis")
        
        if not df.empty:
            if 'shipping_mode' in df.columns:
                
                shipping_stats_raw = df.groupby('shipping_mode').agg({
                    'sales': 'mean',
                    'profit_per_order': 'mean',
                    'order_item_quantity': 'count',
                }).round(2)
                
                shipping_stats_raw['sales_per_qty'] = df.groupby('shipping_mode')['sales'].sum() / df.groupby('shipping_mode')['order_item_quantity'].sum()
                
                shipping_stats = shipping_stats_raw.copy()
                shipping_stats.columns = ['Avg Sales', 'Avg Profit', 'Order Count', 'Avg Sales per Item']
                
                shipping_stats['Avg Sales'] = shipping_stats['Avg Sales'].map(lambda x: f"$ {x:,.0f}")
                shipping_stats['Avg Profit'] = shipping_stats['Avg Profit'].map(lambda x: f"$ {x:,.0f}")
                shipping_stats['Avg Sales per Item'] = shipping_stats['Avg Sales per Item'].map(lambda x: f"$ {x:,.0f}")
                
                st.dataframe(shipping_stats, use_container_width=True)
                
                st.markdown("---")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown("**Average Sales by Shipping Mode**")
                    st.bar_chart(shipping_stats_raw['sales'])

                with col2:
                    st.markdown("**Average Profit by Shipping Mode**")
                    st.bar_chart(shipping_stats_raw['profit_per_order'])
                    
            else:
                st.info("Kolom 'shipping_mode' masih hilang setelah mencoba *join*. Cek ketersediaan `dim_shipping.parquet`.")
    
    # =================================================================
    # --- TAB 4: TIME ANALYSIS (Tidak Berubah) ---
    # =================================================================
    with tab4:
        st.subheader("Time Series Analysis")
        
        if not df.empty and 'order_date' in df.columns:
            df_time = df.copy()
            df_time['month'] = df_time['order_date'].dt.to_period('M')
            
            monthly_stats = df_time.groupby('month').agg({
                'sales': 'sum',
                'profit_per_order': 'sum',
                'delivery_status_binary': 'mean',
                'order_item_quantity': 'count'
            }).reset_index()
            
            monthly_stats['month'] = monthly_stats['month'].astype(str)
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Monthly Sales Trend**")
                st.line_chart(monthly_stats.set_index('month')['sales'])
            
            with col2:
                st.markdown("**Monthly Delay Rate Trend**")
                st.line_chart(monthly_stats.set_index('month')['delivery_status_binary'])

# ==================== 3. ML DASHBOARD COMPONENTS ====================

def render_ml_dashboard(model):
    """Render dashboard Machine Learning untuk prediksi"""
    st.header("🤖 Machine Learning Prediction")
    st.markdown("Predict whether a shipment will be **on-time** or **delayed**")
    
    st.markdown("---")
    
    with st.form("prediction_form", border=True):
        st.subheader("Input Parameters")
        
        col1, col2 = st.columns(2)
        
        with col1:
            sales = st.number_input(
                "Sales Amount ($)",
                min_value=1.0,
                max_value=1000000000.0,
                value=100000.0,
                step=1000.0,
                help="Total sales amount"
            )
            
            profit = st.number_input(
                "Profit per Order ($)",
                value=20000.0,
                step=1000.0,
                help="Profit generated from this order"
            )
        
        with col2:
            quantity = st.number_input(
                "Item Quantity",
                min_value=1,
                max_value=1000,
                value=5,
                help="Number of items in the order"
            )
            
            price = st.number_input(
                "Product Price per Unit ($)",
                min_value=1.0,
                value=20000.0,
                step=1000.0,
                help="Price of each individual item"
            )
        
        col3, col4 = st.columns(2)
        
        with col3:
            discount_rate = st.slider(
                "Discount Rate (%)",
                min_value=0.0,
                max_value=100.0,
                value=10.0,
                step=0.5,
                help="Discount percentage applied"
            )
        
        with col4:
            shipping_mode = st.selectbox(
                "Shipping Mode",
                options=['Standard Class', 'Second Class', 'First Class', 'Same Day'],
                help="Shipping method selected"
            )
        
        submitted = st.form_submit_button(
            "🚀 Predict Delivery Status",
            use_container_width=True,
            type="primary"
        )
    
    if submitted and model is not None:
        with st.spinner("Analyzing data and making prediction..."):
            input_data = pd.DataFrame({
                'sales': [sales],
                'profit_per_order': [profit],
                'order_item_quantity': [quantity],
                'order_item_product_price': [price],
                'order_item_discount_rate': [discount_rate / 100], 
                'shipping_mode': [shipping_mode]
            })
            
            X_input = create_ml_features(input_data)
            
            if X_input.empty:
                st.error("❌ Failed to create features for prediction")
                return
            
            try:
                proba = model.predict_proba(X_input)[0]
                prediction = model.predict(X_input)[0]
                
                st.markdown("---")
                st.subheader("📊 Prediction Results")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if prediction == 0:  
                        st.success("✅ **PREDICTION: ON-TIME DELIVERY**")
                        st.metric(
                            label="Confidence Level",
                            value=f"{proba[0]:.2%}",
                            help="Probability of on-time delivery"
                        )
                    else:  
                        st.error("🚨 **PREDICTION: DELAYED DELIVERY**")
                        st.metric(
                            label="Risk Level",
                            value=f"{proba[1]:.2%}",
                            help="Probability of delayed delivery"
                        )
                
                with col2:
                    st.markdown("**Probability Distribution**")
                    
                    prob_df = pd.DataFrame({
                        'Status': ['On-Time', 'Delayed'],
                        'Probability': [proba[0], proba[1]]
                    })
                    
                    st.bar_chart(prob_df.set_index('Status'))
                
                if hasattr(model, 'feature_importances_'):
                    with st.expander("🔍 Feature Importance", expanded=False):
                        feature_importance = pd.DataFrame({
                            'Feature': FEATURE_COLS,
                            'Importance': model.feature_importances_
                        }).sort_values('Importance', ascending=False)
                        
                        st.dataframe(feature_importance.head(10))
                        
                        st.markdown("**Top Influencing Factors:**")
                        top_features = feature_importance.head(3)
                        for idx, row in top_features.iterrows():
                            st.markdown(f"- **{row['Feature']}** ({row['Importance']:.3f})")
                
                st.markdown("---")
                st.subheader("💡 Recommendations")
                
                if prediction == 1:  
                    st.warning("**Suggestions to improve delivery time:**")
                    
                    if discount_rate > 20:
                        st.markdown("- 📉 **Reduce discount rate** (current: {:.1f}%)".format(discount_rate))
                    
                    if shipping_mode == 'Standard Class':
                        st.markdown("- 🚚 **Consider faster shipping method** (current: Standard Class)")
                    
                    if quantity > 50:
                        st.markdown("- 📦 **Split large orders** (current quantity: {})".format(quantity))
                    
                    st.markdown("- ⏰ **Expedite processing** for high-risk orders")
                    st.markdown("- 📍 **Monitor destination market** for potential delays")
                
                else:
                    st.success("**Good job! Current parameters are optimal for on-time delivery.**")
                    st.markdown("- ✅ Keep current configurations")
                    st.markdown("- 📈 Monitor performance metrics regularly")
                
            except Exception as e:
                st.error(f"❌ Prediction failed: {str(e)}")
                with st.expander("Error Details"):
                    st.code(traceback.format_exc())

# ==================== RUN APPLICATION ====================

def render_sidebar():
    """Render sidebar yang disederhanakan untuk mode OFFLINE"""
    st.sidebar.title("🚚 Logistics Dashboard")
    st.sidebar.markdown("---")
    
    st.sidebar.subheader("📁 Data Source (Local)")
    
    if os.path.exists(FACT_PARQUET_PATH):
        st.sidebar.success(f"✅ Data file found (Fact Parquet)")
    else:
        st.sidebar.error(f"❌ Data file missing: Fact Parquet")
        
    if os.path.exists(MODEL_PATH):
        st.sidebar.success(f"✅ Model file found (Joblib)")
    else:
        st.sidebar.error(f"❌ Model file missing: Joblib")

    if os.path.exists(DIM_CUSTOMER_PATH):
        st.sidebar.success(f"✅ Dim Customer file found")
    else:
        st.sidebar.warning(f"⚠️ Dim Customer file missing. Analysis in 'Performance' tab might be incomplete.")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🔄 Controls")
    
    if st.sidebar.button("Clear Cache & Refresh", type="primary"):
        st.cache_data.clear()
        st.cache_resource.clear()
        st.sidebar.success("Cache cleared!")
        st.rerun()
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("###ℹ️Setup Guide")
    
    with st.sidebar.expander("Setup Guide"):
        st.markdown("Instruksi setup telah disingkat untuk menghindari error parsing pada environment.")


def main():
    render_sidebar()
    
    st.title("Logistics On-Time Delivery (OTD) Dashboard")
    
    with st.spinner("🔄 Loading data and model..."):
        df = load_data()
        model = load_model()
    
    if df.empty or model is None:
        st.error("### ❌ SETUP GAGAL: Tidak dapat memuat Data atau Model.")
        st.warning(f"Pastikan Docker Streamlit sudah di-*build* dengan *dependency* dan *volume* terpasang.")
        return
    
    if 'delivery_status_binary' in df.columns:
        df['delivery_status_binary'] = pd.to_numeric(df['delivery_status_binary'], errors='coerce')
        df['delivery_status_binary'] = df['delivery_status_binary'].replace({-1: 0})
        df['delivery_status_binary'] = df['delivery_status_binary'].fillna(0)
    
    tab_bi, tab_ml = st.tabs(["Business Intelligence", "ML Predictions"])
    
    with tab_bi:
        df_filtered = apply_global_filters(df)
        render_bi_dashboard(df_filtered)
    
    with tab_ml:
        render_ml_dashboard(model)

if __name__ == '__main__':
    st.markdown("""
    <style>
    .stMetric {
        background-color: #f0f2f6;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #0A400C;
    }
    
    .stAlert {
        border-radius: 10px;
    }
    
    .stButton > button {
        width: 100%;
        border-radius: 5px;
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 15px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        width: 150px;
        white-space: pre-wrap;
        border-radius: 10px 10px 0px 0px;
    }
    
    .stTabs [aria-selected="true"] {
        background-color: #748873;
        color: white;
    }
    </style>
    """, unsafe_allow_html=True)
    
    main()