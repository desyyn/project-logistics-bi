# =============================================
# Dockerfile Airflow custom untuk ETL Project
# =============================================

FROM apache/airflow:2.8.4

# -------------------- USER AIRFLOW --------------------
USER airflow

# -------------------- COPY REQUIREMENTS ----------------
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# -------------------- COPY SCRIPTS & DAGs ----------------
COPY 3_etl_scripts/ /opt/airflow/etl_scripts/
COPY 2_airflow_dags/ /opt/airflow/dags/

# -------------------- SET PERMISSIONS ----------------
RUN mkdir -p /opt/airflow/dags/data_backup /opt/airflow/dags/data_backup_host \
    && chmod -R 777 /opt/airflow/dags/data_backup \
    && chmod -R 777 /opt/airflow/dags/data_backup_host

# -------------------- DEFAULT CMD --------------------
USER airflow
