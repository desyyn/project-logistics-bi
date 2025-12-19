## Konsep Staging Area
Staging Area adalah ruang sementara untuk menampung data setelah diekstrak dan dibersihkan awal (T1 & T2).

## Implementasi Staging

### Tahap T1 & T2 (Cleaning)
- Output: `df_cleaned_valid.xlsx`
- Lokasi: `/opt/airflow/data/`
- Fungsi: Menyimpan data yang sudah divalidasi dan distandarisasi sebelum Transformasi Final (*Surrogate Key*).

Data ini kemudian dibaca oleh **Task T3** untuk transformasi akhir sebelum dimuat ke tabel *Fact* dan *Dimension* di PostgreSQL DW.
