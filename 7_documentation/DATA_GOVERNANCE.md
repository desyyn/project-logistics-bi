## Struktur Organisasi

### Peran & Tanggung Jawab
- **Data Owner**: Menetapkan kebijakan akses dan kualitas data di DW.
- **Data Steward (ETL Team)**: Mengimplementasikan kebijakan, membersihkan data (T2), dan memonitor *pipeline*.
- **Data User (Analis/BI)**: Mengakses data yang sudah bersih dari DW (READ-Only).

## Implementasi Teknis

### Audit Trail
- Tabel `fact_shipments` memiliki kolom `etl_load_timestamp` untuk merekam waktu *load* data (diimplementasikan di `t3_load_dw_core.py`).

### Data Quality
- Skrip T2 dan T3 memvalidasi *not null* pada *key* dan membersihkan *outlier*.

## Kebijakan Akses
- Analis hanya memiliki akses **SELECT** ke DW PostgreSQL.
- Akses skrip dan *container* ETL hanya untuk *Data Steward*.