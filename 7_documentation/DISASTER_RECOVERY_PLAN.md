# Disaster Recovery Plan (DRP) - Logistics OTD Project

## Strategi Backup
- **PostgreSQL DW**: Data tersimpan di Docker volume (`pg_data`) yang persist saat container restart
- **File Parquet ETL Output**: Backup otomatis di `./9_staging_local/` setiap kali T3_load_dw.py dieksekusi
- **ML Models**: Model XGBoost disimpan di `/tmp/` setelah training, harus dicopy manual ke `./5_ml_model/model_artifacts/`
- **Source Code**: Semua kode ada di Git repository untuk version control

## Rollback / Callback
- **Kegagalan T1/T2**: Pipeline dihentikan, data staging diperiksa, dan task di-retry manual dari Airflow UI
- **Kegagalan Load ke DW (T3)**: Transaksi database di-rollback otomatis oleh script Python
- **Kegagalan API Call (T2)**: Retry mechanism dengan exponential backoff, fallback ke nilai default jika gagal
- **Kegagalan ML Training (T5)**: Model lama tetap digunakan, training diulang dengan dataset yang sama
- **Kegagalan Server**: Docker containers di-restart otomatis (`restart: always`), data di-recover dari volume

## Recovery Commands
# Restart semua services
docker-compose restart

# Trigger manual pipeline execution
docker exec uas-bi-airflow-webserver airflow dags trigger delivery_dw_etl_pipeline

# Clear failed tasks
docker exec uas-bi-airflow-webserver airflow tasks clear delivery_dw_etl_pipeline

# Backup PostgreSQL data
docker exec uas-bi-postgres pg_dump -U airflow airflow > backup.sql
```