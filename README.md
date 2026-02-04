# Apache Airflow – Postgres to S3 Data Pipeline

This repository contains a **production-style Apache Airflow pipeline** that ingests Airbnb listing data into **PostgreSQL** and exports the processed data to **AWS S3** using a **custom Airflow operator**.

The goal of this project is to demonstrate **real-world data engineering practices**, focusing on correctness, idempotency, and system behavior under failure — not just a happy-path tutorial.

---

## 🔧 Tech Stack

- Apache Airflow (3.x)
- PostgreSQL
- AWS S3
- Python
- Pandas
- SQL
- Docker

---

## 🏗️ Architecture Overview

Airbnb CSV Files
↓
Apache Airflow DAG
↓
Pandas Preprocessing
↓
PostgreSQL (COPY-based ingestion)
↓
Custom Airflow Operator
↓
AWS S3 (CSV exports)


---

## 📌 Project Motivation

While building this pipeline, several real-world issues surfaced that are often skipped in tutorials:

- CSV NULL values breaking Postgres `COPY`
- `invalid input syntax for type numeric`
- Postgres `DEFAULT` values being ignored during bulk loads
- Duplicate data on DAG re-runs
- Pipelines succeeding while the **data itself was incorrect**
- Lack of reusable abstractions for exporting Postgres data to S3

This project documents how each of these problems was identified and resolved incrementally.

---

## 🚨 Key Challenges Encountered

### 1. CSV NULL Handling
Postgres expects `\N` for NULL values during `COPY`.  
Empty strings and improper quoting caused numeric and date parsing errors.

### 2. Postgres `COPY` vs `INSERT`
Unlike `INSERT`, `COPY` does not apply column defaults unless explicitly handled.  
This required careful column mapping and schema design.

### 3. Idempotency & Safe Re-runs
Re-running Airflow DAGs without safeguards can silently corrupt data.  
The pipeline was designed so that **each DAG run represents a single batch**, making re-runs safe.

### 4. Separation of Concerns
Raw ingestion logic was kept separate from export logic using a **custom Airflow operator**, improving reusability and clarity.

---

## 🧠 Key Design Decisions

### ✔ Idempotent Batch Loads
Each DAG run deletes previously ingested data for the current run before loading new data, ensuring consistency and re-run safety.

### ✔ Explicit Column Mapping
All bulk loads use explicit column lists to avoid schema drift and default-value surprises.

### ✔ Custom Airflow Operator
A reusable `PostgresToS3Operator` was implemented using:
- `BaseOperator`
- `PostgresHook`
- `S3Hook`
- Airflow templated fields (`{{ ds }}`)

This keeps the DAG clean and extensible.

---

## 📦 DAG Task Overview

- **download_csv**  
  Downloads Airbnb listing CSV files for multiple snapshot dates.

- **preprocess_csv**  
  Cleans and prepares the data for Postgres ingestion.

- **create_table**  
  Creates the Postgres ingestion table.

- **load_csv_to_postgres**  
  Bulk loads data using Postgres `COPY`.

- **transfer_postgres_to_s3**  
  Exports daily ingested data to AWS S3 using a custom operator.

---

## 📸 Screenshots

Screenshots included in the repository show:

- Airflow task failures caused by CSV and COPY issues
- Postgres error logs
- Successful DAG execution
- Data successfully exported to S3

Refer to the `/screenshots` directory.

---

## 🚀 What This Project Demonstrates

- Production-style Airflow pipeline design
- Safe and idempotent batch ingestion
- Deep understanding of Postgres `COPY` semantics
- Custom Airflow operator development
- Debugging and fixing real data pipeline failures
- Clean separation between ingestion and export layers

---

## 🔮 Future Improvements

- Replace hardcoded snapshot dates with Airflow `logical_date`
- Enable `catchup=True` for native backfills
- Partition S3 outputs by date
- Convert CSV outputs to Parquet
- Add data quality checks and validations

---

## ✍️ Author

Built as part of **self-directed learning and hands-on experimentation** with Apache Airflow and data engineering.

The focus of this project is learning through real-world failure scenarios and production-style fixes.# airflow-postgres-to-s3-pipeline
