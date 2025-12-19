#D:\SEMESTER 5\Kecerdasan Bisnis\UAS\project-logistics\2_airflow_dags\delivery_dw_etl_pipeline.py
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
import os

def slack_fail_notification(context):
    ti = context.get('ti')
    msg = f"""
    :x: *DAG GAGAL* :x:
    *DAG ID*: {ti.dag_id}
    *Task ID*: {ti.task_id}
    *Status*: GAGAL pada Task {ti.task_id}
    *Log URL*: {ti.log_url}
    """
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_default', message=msg)
    slack_hook.execute()

DAG_ID = 'delivery_dw_etl_pipeline'
PROJECT_PATH = '/opt/airflow' 
DDL_FILE = f'{PROJECT_PATH}/1_dw_setup/ddl_tables.sql'

LOCAL_TZ = timezone('Asia/Jakarta')

ENV_VARS = {
    'POSTGRES_HOST': 'postgres',
    'POSTGRES_DB': 'airflow',
    'POSTGRES_USER': 'airflow',
    'POSTGRES_PASSWORD': 'airflow',
    'POSTGRES_PORT': '5432',
    'OPENWEATHER_API_KEY': os.getenv('OPENWEATHER_API_KEY') 
}

def python_bash_cmd(script_name):
    return f"python {PROJECT_PATH}/etl_scripts/{script_name}"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=LOCAL_TZ),
    schedule='@daily',
    catchup=False,
    tags=['dw', 'etl', 'amazon'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 2, 
        'retry_delay': timedelta(minutes=5), 
        'on_failure_callback': [slack_fail_notification],
    }
) as dag:
    
    # -----------------------------------------------------------
    # TAHAP 1: SETUP DATABASE DDL (Membuat Tabel Staging, Fact, dan Dim)
    # -----------------------------------------------------------
    setup_dw_ddl = BashOperator(
        task_id='setup_dw_ddl',
        bash_command=f"python {PROJECT_PATH}/etl_scripts/db_utils.py --run_ddl {DDL_FILE}",
        env=ENV_VARS,
        doc_md="Memanggil db_utils untuk menjalankan DDL dan membuat skema/tabel DW.",
    )

    # -----------------------------------------------------------
    # TAHAP 2: EXTRACT & INGEST (T0)
    # -----------------------------------------------------------
    ingest_raw_data = BashOperator(
        task_id='t0_ingest_raw_data',
        bash_command=python_bash_cmd('T0_ingest_data.py'),
        env=ENV_VARS,
        doc_md="Membaca CSV dan memuat ke staging.raw_data.",
    )

    # -----------------------------------------------------------
    # TAHAP 3: TRANSFORM (T1 & T2)
    # -----------------------------------------------------------
    clean_data = BashOperator(
        task_id='t1_clean_data',
        bash_command=python_bash_cmd('T1_data_cleaning.py'),
        env=ENV_VARS,
        doc_md="Pembersihan, validasi, dan transformasi awal data di Staging.",
    )
    
    api_enrichment = BashOperator(
        task_id='t2_api_enrichment',
        bash_command=python_bash_cmd('T2_api_enrichment.py'),
        env=ENV_VARS,
        doc_md="Melakukan API call dan enrich data.",
    )

    # -----------------------------------------------------------
    # TAHAP 4: LOAD DW (T3)
    # -----------------------------------------------------------
    load_dw_tables = BashOperator(
        task_id='t3_load_dw_tables',
        bash_command=python_bash_cmd('T3_load_dw.py'),
        env=ENV_VARS,
        doc_md="Memuat data dari Staging ke skema bintang (dw.dim_* dan dw.fact_*).",
    )
    
    # -----------------------------------------------------------
    # TAHAP 5: ML DATASET PREPARATION (T4)
    # -----------------------------------------------------------
    prepare_ml_dataset = BashOperator(
        task_id='t4_prepare_ml_dataset',
        bash_command=python_bash_cmd('T4_prepare_ml_dataset.py'),
        env=ENV_VARS,
        doc_md="Extract data dari DW, Feature Engineering, SMOTE, simpan CSV di /tmp/.",
    )

    # -----------------------------------------------------------
    # DEFINISI URUTAN DAG
    # -----------------------------------------------------------
    setup_dw_ddl >> ingest_raw_data >> clean_data >> api_enrichment >> load_dw_tables >> prepare_ml_dataset