import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from etl_pipeline import ETLPipeline
from config import ETLConfig
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    # Get input arguments from the lambda function
    args = getResolvedOptions(sys.argv, ['JOB_NAME', 'year', 'month', 'day', 's3_path'])

    # Create configuration
    config = ETLConfig(
        year=int(args["year"]),
        month=int(args["month"]),
        day=int(args["day"]),
        input_path=args['s3_path'],
        job_name=args['JOB_NAME']
    )

    # Setup Spark
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(config.job_name, args)

    try:
        # Run ETL pipeline
        pipeline = ETLPipeline(spark, config)
        pipeline.run_pipeline()

        # Commit job
        job.commit()
        logger.info("Glue job completed successfully")

    except Exception as e:
        logger.error(f"Glue job failed: {str(e)}")
        raise


if __name__ == "__main__":
    main()