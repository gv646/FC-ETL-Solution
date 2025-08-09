import sys
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, lit
from datetime import datetime
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from pyspark.sql.types import *

# Geting input arguments from the lambda function
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'year','month','day','s3_path'])
year = args["year"]
month=args["month"]
day=args["day"]
ingestion_date = datetime(year, month, day).date()
input_path = args['s3_path']
output_path = input_path.replace('bronze', 'silver').replace('.csv', '/')

# Setup Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define schema
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("purchase_id", IntegerType(), True),
    StructField("purchase_date", DateType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True)
])

# Reading input
print(f"Reading from: {input_path}")
df = spark.read.option("header", True).schema(schema).csv(input_path)

# Transform
df_transformed = (
    df.filter(col("currency") == "USD")
      .withColumn("amount_aud", col("amount") * 1.5)
      .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
      .withColumn("ingestion_date", lit(ingestion_date))
)

# Write output
print(f"Writing to: {output_path}")
df_transformed.write.mode("overwrite").partitionBy("ingestion_date").parquet(output_path)

# Sample data
print("Transformed sample:")
df_transformed.show(5)

job.commit()