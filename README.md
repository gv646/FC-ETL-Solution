# Customer Purchases Data Pipeline

## Overview
This repository contains an end-to-end **ETL data pipeline** built on AWS cloud to process customer purchase data from raw ingestion in **S3** to analytics-ready datasets in **Amazon RDS**.  
The solution uses **AWS Glue**, **AWS Step Functions**, and **AWS Lambda** to orchestrate and automate data transformations across **Bronze → Silver → Gold** layers.

### Features of the solution
#### High Level Design
1. Medallion Architecture Pattern
2. Event-Driven Orchestration
3. Serverless Architecture
4. Incremental processing — only new data is processed each time.
5. Scalability and Reliability
   - Glue jobs leverage Spark for distributed processing, enabling scaling to large datasets without major code changes.
   - S3 provides virtually unlimited storage with high durability.
   - Step Functions ensure fault-tolerant orchestration with retries and error handling.
7. Automated — no manual intervention.
8. Easily extendable to additional downstream systems (Redshift, Elasticsearch, BI tools).
#### Low Level Design
1. Extensibility
   - Modularised code with OOPs followed.
   - Demonstrated seperation of concerns.
   - Reusability of common functionalities.
2. Testable - Unit test integrations.
---

## Architecture

![Data Pipeline Architecture](customer-purchase-etl/docs/customer-purchases-etl.png)

### Data Flow:
1. **Raw Data Ingestion (Bronze Layer)**
   - Raw CSV/JSON purchase files are uploaded to:
     ```
     s3://customer-purchases/datalake/bronze/
     ```
   - Each file contains customer purchase transactions.

2. **Step Function Trigger**
   - An **S3 Event** triggers a **Lambda function** (`lambda_trigger_step_function`) when new data lands in the bronze folder.
   - The Lambda starts an **AWS Step Functions workflow** to process the data.

3. **Bronze → Silver Transformation**
   - AWS Glue job: `bronze_to_silver_job.py`
   - Cleans and standardizes raw data.
   - Writes transformed data to:
     ```
     s3://customer-purchases/datalake/silver/ingestion_date=YYYY-MM-DD/
     ```
   - **Partitioning by ingestion_date** for efficient downstream reads.

4. **Silver → Gold Transformation**
   - AWS Glue job: `silver_to_gold_job.py`
   - Aggregates purchase data at **customer level**:
     - Creates `full_name` column.
     - Calculates `total_spent_aud`, `first_purchase_date`, and `last_purchase_date`.
   - Writes curated dataset to:
     ```
     s3://customer-purchases/datalake/gold/year=YYYY/month=MM/day=DD/
     ```
   - Data is queryable via **Athena** using the Glue Data Catalog.

5. **Gold → RDS Staging Load**
   - AWS Glue job: `gold_to_rds_job.py`
   - Reads latest gold data.
   - Writes it to an **Amazon RDS staging table** using JDBC in `append` mode.

6. **Staging → Main Table Upsert**
   - AWS Lambda: `lambda_upsert_sync`
   - Runs SQL-based UPSERT from staging table into the main production table:
     - Inserts new customers.
     - Updates existing customers’ total spend and last purchase date.
   - Keeps **`customer_purchase_summary`** table in sync.

---

## Database Design

### Staging Table
```sql
CREATE TABLE customer_purchase_summary_staging (
    customer_id INT,
    full_name VARCHAR(255),
    email VARCHAR(255),
    total_spent_aud DECIMAL(18, 2),
    first_purchase_date DATE,
    last_purchase_date DATE
);
