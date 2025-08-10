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

### Out of Scope
1. The code has been modularized only for one of the packages/layers (bronze_to_silver). The remaining Glue job code is kept within their respective single Python files. Each module can be restructured into classes similar to the bronze_to_silver layer.
2. Unit tests have been implemented for only one of the classes. As mentioned above, all modules can be modularized in the same way to enable a proper testing structure.
3. The deployment strategy has not been demonstrated here. AWS CloudFormation or Terraform could be used to define all resources, and CI/CD can be implemented using AWS CodePipeline or GitHub Actions.

## Architecture

![Data Pipeline Architecture](customer-purchase-etl/docs/customer-purchases-etl.png)

### Data Flow:
1. **Raw Data Ingestion (Bronze Layer)**
   - Raw CSV purchase files are uploaded to:
     ```
     s3://customer-purchases/datalake/bronze/
     ```
   - The folder structure is partitioned on year, month, day of the ingestion date.
   - Each file contains customer purchase transactions.

2. **Step Function Trigger**
   - An **S3 Event** triggers a **Lambda function** (`lambda_trigger_step_function`) when new data lands in the bronze folder.
   - The Lambda starts an **AWS Step Functions workflow** to process the data.

3. **Bronze → Silver Transformation**
   - AWS Glue job: `bronze_to_silver_job.py`
   - Cleans and standardizes raw data.
   - This layer also acts as the validation layer for the incoming data.
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
   - This enables the implementation of the lakehouse architecture in the pipeline.

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
  
### Orchestration using AWS Step Fucntion:
#### step_function.yaml
- The whole pipeline is orchestrated using the AWS step function.
- The YAML for the step function is commmited in the code. (step_function.yaml)
```yaml
Comment: "ETL pipeline with file-specific arguments"
StartAt: BronzeToSilver
States:
  BronzeToSilver:
    Type: Task
    Resource: arn:aws:states:::glue:startJobRun.sync
    Parameters:
      JobName: "bronze_to_silver_job"
      Arguments:
        "--year.$": "$.year"
        "--month.$": "$.month"
        "--day.$": "$.day"
        "--s3_path.$": "$.s3_path"
    Next: SilverToGold
    Catch:
      - ErrorEquals: ["States.ALL"]
        ResultPath: "$.error"
        Next: FailureNotification

  SilverToGold:
    Type: Task
    Resource: arn:aws:states:::glue:startJobRun.sync
    Parameters:
      JobName: "silver_to_gold_job"
      Arguments:
        "--year.$": "$.year"
        "--month.$": "$.month"
        "--day.$": "$.day"
    Next: GoldToRDS
    Catch:
      - ErrorEquals: ["States.ALL"]
        ResultPath: "$.error"
        Next: FailureNotification

  GoldToRDS:
    Type: Task
    Resource: arn:aws:states:::glue:startJobRun.sync
    Parameters:
      JobName: "gold_to_rds_job"
      Arguments:
        "--year.$": "$.year"
        "--month.$": "$.month"
        "--day.$": "$.day"
    Next: UpsertToRDS
    Catch:
      - ErrorEquals: ["States.ALL"]
        ResultPath: "$.error"
        Next: FailureNotification

  UpsertToRDS:
    Type: Task
    Resource: arn:aws:states:::lambda:invoke
    Parameters:
      FunctionName: "upsert_to_rds_lambda"
      Payload:
        year.$: "$.year"
        month.$: "$.month"
        day.$: "$.day"
    Next: SuccessNotification
    Catch:
      - ErrorEquals: ["States.ALL"]
        ResultPath: "$.error"
        Next: FailureNotification

  SuccessNotification:
    Type: Pass
    Result:
      Message: "ETL pipeline completed successfully"
    End: true

  FailureNotification:
    Type: Fail
    Error: "ETLJobFailed"
    Cause: "One of the Glue jobs or Lambda failed."
```
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
    last_purchase_date DATE,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
### Main Table
```sql
CREATE TABLE customer_purchase_summary (
    customer_id INT PRIMARY KEY,
    full_name VARCHAR(255),
    email VARCHAR(255),
    total_spent_aud DECIMAL(18, 2),
    first_purchase_date DATE,
    last_purchase_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);
```
### Index Creation
```sql
CREATE INDEX idx_total_spent_desc 
ON customer_purchase_summary (total_spent_aud DESC);
```
### Query
#### SQL query to return the top 5 customers by total_spent_aud
```sql
SELECT customer_id, 
       full_name, 
       email, 
       total_spent_aud
FROM customer_purchase_summary
ORDER BY total_spent_aud DESC
LIMIT 5;
```

