# Dokumentasi Data Staging Layer

Dokumentasi ini menjelaskan arsitektur dan proses pada lapisan *staging* di dalam pipeline ETL logistik. 
Lapisan ini berfungsi sebagai area penampungan sementara dan zona pembersihan data sebelum dimuat ke dalam *Data Warehouse* utama.

## 1. Arsitektur Staging
[cite_start]Staging layer diimplementasikan menggunakan pendekatan tiga tahap (*three-stage sequential processing*) untuk memastikan integritas data melalui mekanisme *checkpoint* di setiap tahap:

1.  [cite_start]**Staging Raw**: Menyimpan data mentah hasil ekstraksi langsung dari sumber asli.
2.  [cite_start]**Staging Cleaned**: Area untuk data yang telah divalidasi, dibersihkan, dan distandarisasi.
3.  [cite_start]**Staging Enriched**: Area final di mana data internal diperkaya dengan data eksternal (API Cuaca).

## 2. Struktur Tabel Staging

| Tabel | Deskripsi | Status Data |
| :--- | :--- | :--- |
| `staging.raw_delivery_data` | [cite_start]Hasil ingest langsung dari file CSV (31 kolom asli)[cite: 393]. | *Raw/Uncleaned* |
| `staging.cleaned_delivery_data` | [cite_start]Hasil proses pembersihan ID, parsing tanggal, dan validasi bisnis. | *Validated & Standardized* |
| `staging.enriched_data` | [cite_start]Dataset lengkap yang sudah ditambahkan kolom temperatur, kelembaban, dan kondisi cuaca. | *Enriched & Ready for DW* |

## 3. Proses Transformasi Utama (T1 & T2)

### Pembersihan dan Validasi (T1)
* [cite_start]**Deduplikasi**: Menghapus entri ganda berdasarkan kunci komposit (*order_id*, *customer_id*, *product_card_id*).
* [cite_start]**Standardisasi ID**: Mengonversi kolom ID menjadi format *integer string* yang konsisten tanpa bagian desimal.
* [cite_start]**Validasi Temporal**: Memastikan logika waktu benar (misal: *shipping date* tidak boleh lebih awal dari *order date*).
* [cite_start]**Integritas Finansial**: Memastikan nilai *profit* tidak melebihi nilai *sales* dan menangani *outlier* menggunakan metode IQR.

### Pengayaan Data (T2)
* [cite_start]Menggunakan **OpenWeatherMap API** untuk menarik data cuaca berdasarkan koordinat lokasi pelanggan.
* [cite_start]Implementasi menggunakan `ThreadPoolExecutor` (5 *workers*) dengan *rate limiting* untuk optimalisasi performa pengambilan data.
* [cite_start]Hasil akhir disimpan ke `staging.enriched_data` sebagai dataset final untuk pemodelan *star schema*.

## 4. Standar Kualitas Data
Kualitas data pada lapisan staging diatur dengan ambang batas otomatis:
* **Completeness**: Minimal 95% (memastikan kolom kritis tidak kosong)
* **Validity**: Minimal 98%
* **Consistency**: Minimal 99%

---
*Dokumentasi ini dibuat sebagai bagian dari Laporan Data Warehouse - 2025* 