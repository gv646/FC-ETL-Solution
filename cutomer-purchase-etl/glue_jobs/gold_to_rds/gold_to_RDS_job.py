import sys
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Job Arguments
args = getResolvedOptions(sys.argv, ["JOB_NAME", "year", "month", "day"])
job_name = args["JOB_NAME"]
year = int(args["year"])
month = int(args["month"])
day = int(args["day"])

gold_path = "s3://customer-purchases/gold"
rds_jdbc_url = "jdbc:mysql://your-rds-endpoint:3306/your_database"
rds_user = "your_rds_user"
rds_password = "your_rds_password"
staging_table = "customer_purchase_summary_staging"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = glueContext.create_job(job_name)

# Reading Daily Gold Partition
gold_df = spark.read.parquet(f"{gold_path}/year={year}/month={month}/day={day}")

# Writing to Staging Table in RDS

gold_df.write \
    .format("jdbc") \
    .option("url", rds_jdbc_url) \
    .option("dbtable", staging_table) \
    .option("user", rds_user) \
    .option("password", rds_password) \
    .mode("overwrite") \
    .save()

job.commit()
