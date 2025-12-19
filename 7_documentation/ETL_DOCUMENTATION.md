## Alur Data (ETL)
`Source (Excel)` → T1, T2 (Python) → `Staging File` → T3 (SQL, Python) → `Data Warehouse (PostgreSQL)`

## Alur Task

### T1 & T2 (`t2_clean_transform.py`)
- Ekstraksi data mentah, pembersihan, validasi, penanganan *outlier*.
- Output: Staging File.

### T3 (`t3_load_dw_core.py`)
- Membuat tabel DW, *key generation*, dan memuat data ke *Fact* & *Dimension*.

### T5 (`t5_prepare_ml_dataset.py`)
- Ambil data dari DW, lakukan *One-Hot Encoding*, hasilkan *dataset* final (`ml_dataset_ready.csv`).

### T6 (`t6_training_ml.py`)
- *Training* model klasifikasi (XGBoost).
- *Hyperparameter tuning* dengan `RandomizedSearchCV`.
- Penanganan *imbalanced data* (`class_weight='balanced'` / `scale_pos_weight`).
- ROC AUC sekitar 0.76.