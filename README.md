# 📊 Financial Data ETL Pipeline

A cloud-native ETL pipeline for ingesting, transforming, and loading market/dividend data into a structured data warehouse.

## 🚀 Overview

This pipeline automates the full data lifecycle:

* **Extract**: Pulls raw market/dividend data via API
* **Transform**: Cleans, validates, and standardizes data
* **Load**: Writes data into partitioned BigQuery tables

Designed for **weekly batch processing**, **idempotency**, and **scalability**.

## 🏗️ Architecture

```
       +---------+       +------------------+       +----------------+
       |  API    |  -->  | shared.clients   |  -->  | dividends/etl  |
       +---------+       +------------------+       +-------+--------+
                                                        |       |
                                                        v       v
                                                      extract  transform
                                                        |       |
                                                        +-------+
                                                            |
                                                            v
                                                          load
                                                            |
                                                            v
                                                       BigQuery
```

## 🛠️ Tech Stack

* Python 3.11
* Google Cloud Platform (GCP)

  * BigQuery
  * Cloud Storage
  * Cloud Run
  * Cloud Scheduler
* Docker

## 📂 Project Structure

```
shared/
├── clients/        # API clients (GCP, MarketStack)
├── configs/        # Config files and loader
└── misc/           # Metadata & utilities
dividends/
├── etl/
│   ├── extract/    # Extractors
│   ├── transform/  # Transformers
│   └── load/       # Loaders & mergers
└── run_etl.py      # Entry point
scripts/            # Helper & deployment scripts
cloudbuild.yaml     # Cloud Build config
cloudrun.yaml       # Cloud Run deployment config
Dockerfile          # Application containerization
requirements.txt    # Required packages
README.md
```

## ⚙️ Setup (Python 3.11)

1. Ensure Python 3.11 is installed:

```bash
python3.11 --version
```

2. Clone the repo:

```bash
gh repo clone sohum7/market-stack-etl-portfolio
```

3. Create virtual environment:

```bash
python3.11 -m venv venv
source venv/bin/activate      # Mac/Linux
venv\Scripts\activate         # Windows
```

4. Upgrade pip:

```bash
pip install --upgrade pip
```

5. Install dependencies:

```bash
pip install -r requirements.txt
```

## 🔐 Environment Configuration

Minimal `.env`:

```
ENVIRONMENT=dev | prod
```

* `dev` → local development + Cloud Run
* `prod` → Cloud Run / production environment - ## TODO: Create with Terraform ##
* Configuration handled via `shared/configs/config_loader.py`

## ▶️ Running the Pipeline

Run the full ETL:

```bash
python -m dividends.run_etl
```

## 🗓️ Scheduling

* Scheduled as a **weekly batch job**
* Triggered via **Cloud Scheduler → Cloud Run** ## TODO: Cloud Scheduler ##
* Processes data in **date-bounded batches** (Mon–Fri)

## 📈 Data Design

* Partitioned tables (by market date)
* Clustering (by symbol and market date)
* Incremental loads
* Merge-based upserts
* Optimized for analytics and query performance

## ⚠️ Notes

* Python version locked to 3.11
* Ensure proper IAM roles for BigQuery and Cloud Storage
* Pipeline supports idempotent re-runs ## TODO: retries at 0 for now ##

## 📌 Future Improvements

* Cloud Scheduler addition
* Terraform (IaC) addition for production environment deployment
* Data quality validation checks
* Monitoring & alerting
* CI/CD integration
* Backfill automation

## 👤 Author

Sohum Patel

