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
Market Stack API
        │
        ▼
┌─────────────────────────┐
│ Extract                 │
│ • Retrieve JSON data    │
│ • Persist raw extracts  │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Google Cloud Storage    │
│ (Raw Cache)             │
│ • JSON extract files    │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Transform               │
│ • Read cached extracts  │
│ • Convert to Pandas DF  │
│ • Apply cleansing and   │
│   business rules        │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Google Cloud Storage    │
│ (Transformed Cache)     │
│ • Persist intermediate  │
│   datasets              │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Load                    │
│ • Load transformed data │
│   into BigQuery staging │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ BigQuery Staging Tables │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ Merge                   │
│ • Upsert records into   │
│   target tables         │
└──────────┬──────────────┘
           │
           ▼
┌─────────────────────────┐
│ BigQuery Target Tables  │
└─────────────────────────┘
```

## 🛠️ Tech Stack

* Python 3.11
* Google Cloud Platform (GCP)
  * BigQuery (Data Warehouse)
  * Cloud Storage (Intermediate Data Caching)
  * Cloud Run (Container Runtime)
  * Cloud Scheduler (Job Triggering)
  * Cloud Build (CI/CD)
* Docker

## 🔐 Environment Configuration

Minimal `.env`:

```text
ENVIRONMENT=dev | prod
```

* `dev` → Local development and Cloud Run deployment
* `prod` → Reserved for the production environment

Configuration is managed via `shared/configs/config_loader.py`.

> **Note:** Production infrastructure and deployment via Terraform are planned for a future release.

## 🗓️ Scheduling

* Intended to be triggered via **Cloud Scheduler → Cloud Run**
* Processes data in date-bounded batches (Mon–Fri)

> **Note:** Cloud Scheduler integration is planned for a future release.

## 📈 Data Design

* Partitioned tables (by market date)
* Clustering (by symbol and market date)
* Incremental loads
* Merge-based upserts
* Optimized for analytics and query performance

## 🔐 IAM Service Accounts

### market-stack-etl-cloudbuild-sa

* Artifact Registry Writer
* Cloud Build Editor
* Cloud Build Service Account
* Cloud Run Developer
* Service Account User

### market-stack-etl-cloudrun-sa

* BigQuery Data Editor
* BigQuery User
* Logs Writer
* Secret Manager Secret Accessor
* Service Account User
* Storage Object User

## ⚠️ Notes

* Python version locked to 3.11
* Ensure proper IAM roles for BigQuery and Cloud Storage
* Pipeline supports idempotent re-runs
* Retry policies are currently disabled and will be enhanced in future iterations

## 📌 Future Improvements

* Cloud Scheduler integration
* Terraform-based infrastructure provisioning
* Production environment deployment
* Data quality validation checks
* Monitoring and alerting
* Enhanced CI/CD workflows
* Automated backfill support
* Retry and recovery mechanisms

## 👤 Author

**Sohum Patel**

Associate Data Engineer

* Google Cloud Associate Cloud Engineer
* Experience building ETL pipelines on GCP
* Background in healthcare data engineering
