#D:\SEMESTER 5\Kecerdasan Bisnis\UAS\project-logistics\2_airflow_dags\ml_training_pipeline.py
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from pendulum import timezone
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
from airflow.utils.dates import days_ago

def slack_fail_notification(context):
    ti = context.get('ti')
    msg = f"""
    :x: *DAG GAGAL* :x:
    *DAG ID*: {ti.dag_id}
    *Task ID*: {ti.task_id}
    *Status*: GAGAL pada Task {ti.task_id}
    *Log URL*: {ti.log_url}
    """
    slack_hook = SlackWebhookHook(
        slack_webhook_conn_id='slack_default', 
        message=msg
    )
    slack_hook.send()

PROJECT_PATH = '/opt/airflow'
LOCAL_TZ = timezone('Asia/Jakarta')
DAG_ID_ML = 'ml_xgboost_training_pipeline'

ENV_VARS = {
    'POSTGRES_HOST': 'postgres',
    'POSTGRES_DB': 'airflow',
    'POSTGRES_USER': 'airflow',
    'POSTGRES_PASSWORD': 'airflow',
}

def python_bash_cmd(script_name):
    return f"python {PROJECT_PATH}/etl_scripts/{script_name}"

with DAG(
    dag_id=DAG_ID_ML,
    start_date=datetime(2025, 1, 1, tzinfo=LOCAL_TZ),
    schedule='@daily',
    catchup=False,
    tags=['ml', 'xgboost'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=10),
        'on_failure_callback': [slack_fail_notification],
    }
) as dag_ml:

    t5_train_xgb = BashOperator(
        task_id='t5_train_xgboost_model',
        bash_command=python_bash_cmd('T5_training_xgb.py'),
        env=ENV_VARS,
        doc_md="Melatih model XGBoost menggunakan dataset dari /tmp/ml_dataset_ready.csv.",
    )

    t_register_model = BashOperator(
        task_id='t_copy_model_to_host',
        bash_command="echo 'Model xgb_otd_model.joblib trained. Use docker cp to extract it from /tmp/.'",
        doc_md="Model disimpan di /tmp/. Perlu docker cp untuk memindahkannya ke host.",
    )

    t_cleanup_tmp = BashOperator(
        task_id='t_cleanup_tmp_dataset',
        bash_command="rm -f /tmp/ml_dataset_ready.csv /tmp/xgb_model_report.txt /tmp/xgb_otd_model.joblib",
        doc_md="Menghapus file sementara di /tmp/.",
    )

    t5_train_xgb >> t_register_model >> t_cleanup_tmp