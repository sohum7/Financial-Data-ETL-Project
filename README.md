# 📊 ETL Data Pipeline

A cloud-native ETL pipeline for ingesting, transforming, and loading market data into a structured data warehouse.

---

## 🚀 Overview

This pipeline automates the full data lifecycle:

* **Extract**: Pulls raw market data from an external API
* **Transform**: Cleans and standardizes data
* **Load**: Writes data into partitioned warehouse tables

Designed for **batch processing**, **idempotency**, and **scalability**.

---

## 🏗️ Architecture

```
API → Cloud Storage → Staging Tables → Merge → Final Tables
```

---

## 🛠️ Tech Stack

* **Python 3.11**
* **Google Cloud Platform (GCP)**

  * Cloud Storage
  * BigQuery
  * Cloud Run
  * Cloud Scheduler
* **Docker**

---

## 📂 Project Structure

```
├── src/
│   ├── extract/
│   ├── transform/
│   ├── load/
│   └── utils/
├── configs/
├── scripts/
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## ⚙️ Setup (Python 3.11)

### 1. Ensure Python 3.11 is installed

```bash
python3.11 --version
```

---

### 2. Clone the repo

```bash
git clone <your-repo-url>
cd <repo-name>
```

---

### 3. Create virtual environment (Python 3.11)

```bash
python3.11 -m venv venv
```

Activate:

```bash
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

---

### 4. Upgrade pip (recommended)

```bash
pip install --upgrade pip
```

---

### 5. Install dependencies

```bash
pip install -r requirements.txt
```

---

## 🔐 Environment Configuration

Minimal environment setup:

```
ENVIRONMENT=dev | prod
```

* `dev` → local execution
* `prod` → Cloud Run environment

Configuration logic is handled via the `configs/` module.

---

## ▶️ Running the Pipeline

Run the full pipeline:

```bash
python -m src.main
```

---

## 🗓️ Scheduling

* Runs as a **weekly batch job**
* Triggered via **Cloud Scheduler → Cloud Run**
* Processes **date-bounded batches (Mon–Fri)**

---

## 📈 Data Design

* Partitioned tables (by date)
* Incremental ingestion
* Merge-based upserts
* Optimized for analytical queries

---

## 🚢 Deployment

### Build Docker image

```bash
docker build -t etl-pipeline .
```

### Deploy to Cloud Run

```bash
gcloud run deploy etl-service \
  --image gcr.io/<project-id>/etl-pipeline \
  --platform managed
```

---

## ⚠️ Notes

* Python version locked to **3.11**
* No sensitive credentials stored in repo
* Requires appropriate IAM roles for:

  * BigQuery
  * Cloud Storage
* Pipeline supports safe re-runs (idempotent design)

---

## 📌 Future Improvements

* Data quality validation checks
* Monitoring & alerting
* CI/CD pipeline integration
* Backfill automation

---

## 👤 Author

Sohum Patel

---

## 📄 License

MIT License
