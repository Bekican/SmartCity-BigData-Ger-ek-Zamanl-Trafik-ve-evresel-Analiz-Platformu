#  SmartCity: Real-time Traffic & Environmental Analysis Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue?style=for-the-badge&logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-2.8-black?style=for-the-badge&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-orange?style=for-the-badge&logo=apachespark)
![Docker](https://img.shields.io/badge/Docker-Compose-blue?style=for-the-badge&logo=docker)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?style=for-the-badge&logo=postgresql)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-F46800?style=for-the-badge&logo=grafana)

## 📖 Overview

**SmartCity**, binlerce IoT sensöründen (simüle edilmiş) gelen trafik ve hava kalitesi verilerini işleyen, analiz eden ve görselleştiren uçtan uca (End-to-End) bir **Büyük Veri (Big Data)** projesidir.

Proje, modern veri mühendisliği prensiplerine uygun olarak **Lambda Mimarisi**'nin basitleştirilmiş bir versiyonunu uygular. **Gerçek zamanlı (Streaming)** veri akışını işleyerek anlık trafik yoğunluğunu tespit ederken, aynı zamanda verileri **Data Lake** (Veri Gölü) üzerinde uzun vadeli analizler için arşivler.

### Key Features
* **High Throughput Ingestion:** Apache Kafka ile saniyede binlerce sensör verisini kayıpsız karşılama.
* **Stream Processing:** Apache Spark Structured Streaming ile akan veri üzerinde pencereleme (windowing) ve filtreleme.
* **Data Lake Architecture:** MinIO (S3 uyumlu) üzerinde Parquet formatında verimli veri saklama.
* **Real-time Dashboard:** PostgreSQL ve Grafana entegrasyonu ile canlı veri görselleştirme.
* **Containerized Environment:** Docker Compose ile tek komutla tüm altyapının ayağa kaldırılması.

---

## Architecture

Veri akışı şu adımları izler:
`IoT Sensors (Python) -> Kafka -> Spark Streaming -> MinIO (Raw Data) & PostgreSQL (Aggregated Stats) -> Grafana`

*(Buraya Grafana ekran görüntüsünü veya çizdiğimiz diyagramı ekleyebilirsin)*
![Architecture Diagram](https://via.placeholder.com/800x400?text=Architecture+Diagram+Placeholder)

## 🛠️ Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Ingestion** | Apache Kafka & Zookeeper | Mesaj kuyruğu ve broker yönetimi. |
| **Processing** | Apache Spark (PySpark) | Stream işleme, filtreleme ve aggregation. |
| **Storage (Lake)** | MinIO (S3 Compatible) | Ham verilerin (Raw Data) Parquet formatında saklanması. |
| **Storage (Ware)**| PostgreSQL | İşlenmiş ve özetlenmiş verilerin saklanması. |
| **Visualization** | Grafana | Canlı izleme panelleri (Time-series graphs, Gauges). |
| **Infrastructure**| Docker & Docker Compose | Servis orkestrasyonu. |

---

## Getting Started

Bu projeyi yerel makinenizde çalıştırmak için aşağıdaki adımları izleyin.

### Prerequisites
* Docker & Docker Compose
* Python 3.8+

### Installation

1.  **Repoyu Klonlayın:**
    ```bash
    git clone [https://github.com/kullaniciadiniz/smartcity-bigdata.git](https://github.com/kullaniciadiniz/smartcity-bigdata.git)
    cd smartcity-bigdata
    ```

2.  **Altyapıyı Ayağa Kaldırın:**
    ```bash
    docker-compose up -d
    ```
    *(Kafka, Zookeeper, Spark, MinIO, Postgres ve Grafana konteynerleri başlayacaktır.)*

3.  **Python Kütüphanelerini Yükleyin:**
    ```bash
    pip install -r producer/requirements.txt
    pip install -r consumer/requirements.txt
    ```

4.  **Veritabanı Tablosunu Oluşturun:**
    ```bash
    python init_db.py
    ```

### Usage

1.  **Producer'ı Başlatın (Veri Akışını Sağlayın):**
    ```bash
    python producer/main.py
    ```

2.  **Consumer'ı Başlatın (Spark Streaming İşini Tetikleyin):**
    ```bash
    python consumer/main.py
    ```
    *Not: İlk çalıştırmada Spark gerekli JAR dosyalarını indireceği için başlaması 1-2 dakika sürebilir.*

3.  **Sistemi İzleyin:**
    * **Grafana:** `http://localhost:3000` (Login: admin/admin)
    * **MinIO (Data Lake):** `http://localhost:9001` (Login: minioadmin/minioadmin)
    * **Spark Master:** `http://localhost:8080`

---

## Dashboard Screenshot


![Grafana Dashboard](https://via.placeholder.com/800x400?text=Grafana+Dashboard+Screenshot)

---

## 🔮 Future Improvements (Roadmap)

* [ ] **Machine Learning:** Geçmiş verileri kullanarak trafik yoğunluğu tahmini yapan regresyon modeli.
* [ ] **Kubernetes:** Docker Compose yapısının Kubernetes Cluster'ına taşınması (Helm Charts).
* [ ] **CI/CD Pipelines:** GitHub Actions ile otomatik test ve deployment süreçleri.
* [ ] **Alerting:** Trafik sıkışıklığında Telegram/Slack üzerinden anlık uyarı gönderimi.

## 👨‍💻 Author

[Bekir Can Çakmak] - Computer Engineering Student & Backend Developer

  LinkedIn:(www.linkedin.com/in/bekircancakmak)
  GitHub: (https://github.com/Bekican)
