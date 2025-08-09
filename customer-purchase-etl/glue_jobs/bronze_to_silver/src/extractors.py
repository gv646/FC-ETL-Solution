from pyspark.sql import DataFrame, SparkSession
import logging

logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction operations"""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_csv_with_schema(self, file_path: str, schema: StructType) -> DataFrame:
        """
        Read CSV file with predefined schema

        Args:
            file_path: Path to the CSV file
            schema: PySpark schema definition

        Returns:
            DataFrame with loaded data
        """
        logger.info(f"Reading data from: {file_path}")
        try:
            df = self.spark.read.option("header", True).schema(schema).csv(file_path)
            logger.info(f"Successfully loaded {df.count()} rows")
            return df
        except Exception as e:
            logger.error(f"Error reading file {file_path}: {str(e)}")
            raise