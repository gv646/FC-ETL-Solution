import sys
from datetime import datetime

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, concat_ws, min, max, sum, year, month, to_date
from awsglue.dynamicframe import DynamicFrame

# Reading arguments passed by the Step Function 
args = getResolvedOptions(sys.argv, ["JOB_NAME", "year","month","day"])
job_name = args["JOB_NAME"]
year = args["year"]
month=args["month"]
day=args["day"]
ingestion_date = datetime(year, month, day).date()
silver_path="s3://customer-purchases/silver"

# Initializing Glue job context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = glueContext.create_job(job_name)

# Reading filtered Silver layer data
# Assumes partitioning by ingestion_date=<yyyy-mm-dd>
silver_df = spark.read.parquet(f"{silver_path}/ingestion_date={ingestion_date}")

# Performing Aggregations

# a. Create full name
silver_df = silver_df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

# b. Aggregate purchases per customer
aggregated_df = silver_df.groupBy("customer_id", "full_name", "email") \
    .agg(
        sum(col("amount_aud")).alias("total_spent_aud"),
        min(col("purchase_date")).alias("first_purchase_date"),
        max(col("purchase_date")).alias("last_purchase_date")
    ) \
    .withColumn("year", year) \
    .withColumn("month", month)\
    .withColumn("day",day)

# Writing to Gold layer (partitioned)
aggregated_dyf = DynamicFrame.fromDF(aggregated_df, glueContext, "aggregated_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=aggregated_dyf,
    connection_type="s3",
    format="parquet",
    connection_options={
        "path": gold_path,
        "partitionKeys": ["year", "month","day"]
    }
)

job.commit()