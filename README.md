# 🇮🇳 India Data Platform

End-to-end AWS Data Engineering project built on Medallion Architecture
processing real public data from India — weather, RBI rates, NSE stocks.

## 🏗️ Architecture
Bronze → Silver → Gold → Power BI Dashboard

## 🛠️ Tech Stack
- **Language:** Python, PySpark, SQL, Shell Scripting
- **Ingestion:** AWS Lambda, Kinesis, DMS
- **Storage:** AWS S3 — Data Lake
- **Processing:** PySpark on EMR
- **Warehouse:** AWS Redshift
- **Orchestration:** Apache Airflow on MWAA
- **Monitoring:** CloudWatch, SNS
- **CI/CD:** GitHub Actions
- **Visualization:** Power BI

## 📊 Data Sources
| Source | Data | Frequency |
|--------|------|-----------|
| Open-Meteo API | City weather across India | Daily |
| data.gov.in | Government public datasets | Daily |
| RBI API | Exchange rates | Daily |
| NSE India | Stock market data | Daily |

## 📁 Project Structure
- `dags/` — Airflow DAG files
- `spark_jobs/` — PySpark transformation jobs
- `lambdas/` — AWS Lambda functions
- `sql/` — Redshift DDL and stored procedures
- `tests/` — Unit tests
- `infrastructure/` — Terraform IaC

## 🚀 Progress
- [x] Day 1 — Dev environment setup
- [x] Day 2 — Python basics, CSV, live API call
- [ ] Day 3 — SQL + AWS S3 Bronze layer
- [ ] Day 4 — PySpark Silver layer on EMR
- [ ] Day 5 — Redshift Gold layer
- [ ] Day 6 — Airflow orchestration
- [ ] Day 7 — CI/CD + Power BI dashboard

## 👨‍💻 Author
Kevin Josh — Data Engineer