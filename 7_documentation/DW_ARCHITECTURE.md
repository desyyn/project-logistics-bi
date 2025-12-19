## Perancangan Arsitektur: Kimball (Star Schema)

Proyek ini menggunakan arsitektur **Kimball (Star Schema)** karena desainnya sederhana, mudah diimplementasikan, dan mendukung *query* analitik yang cepat serta intuitif.

## Desain Skema DW

### Tabel Fakta (`fact_shipments`)
- Menyimpan data per item pesanan pengiriman.
- Metrik yang disimpan: `sales`, `profit`, kuantitas, dan status pengiriman (`is_delayed`).

### Tabel Dimensi
- **`dim_time`**: Analisis tren temporal (tahun, bulan, hari, *weekend*).
- **`dim_customer`**: Data pelanggan, termasuk segmen.
- **`dim_product`**: Data produk, termasuk harga.
- **`dim_shipping`**: Data logistik, termasuk mode pengiriman.
- **`dim_market`**: Data wilayah atau pasar.

## Bidang Analisis
- Kinerja Pengiriman Tepat Waktu (On-Time Delivery / OTD)
- Analisis Penjualan Logistik