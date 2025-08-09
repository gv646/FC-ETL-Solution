from pyspark.sql import SparkSession
from extractors import DataExtractor
from transformers import DataTransformer
from loaders import DataLoader
from schemas import DataSchemas
from config import ETLConfig
import logging

logger = logging.getLogger(__name__)


class ETLPipeline:
    """Main ETL pipeline orchestrator"""

    def __init__(self, spark: SparkSession, config: ETLConfig):
        self.spark = spark
        self.config = config
        self.extractor = DataExtractor(spark)
        self.transformer = DataTransformer()
        self.loader = DataLoader()

    def run_pipeline(self) -> None:
        """Execute the complete ETL pipeline"""
        logger.info("Starting ETL pipeline")

        # Extract
        schema = DataSchemas.get_customer_purchase_schema()
        df_raw = self.extractor.read_csv_with_schema(self.config.input_path, schema)

        # Transform
        df_transformed = self.transformer.apply_all_transformations(
            df_raw,
            self.config.ingestion_date
        )

        # Load
        self.loader.write_parquet_partitioned(df_transformed, self.config.output_path)
        self.loader.show_sample_data(df_transformed)

        logger.info("ETL pipeline completed successfully")