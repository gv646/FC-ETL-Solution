from pyspark.sql import DataFrame
import logging

logger = logging.getLogger(__name__)


class DataLoader:
    """Handles data loading/writing operations"""

    @staticmethod
    def write_parquet_partitioned(df: DataFrame, output_path: str,
                                  partition_columns: list = None) -> None:
        """
        Write DataFrame as partitioned parquet files

        Args:
            df: DataFrame to write
            output_path: Path to write the data
            partition_columns: List of columns to partition by
        """
        if partition_columns is None:
            partition_columns = ["ingestion_date"]

        logger.info(f"Writing data to: {output_path}")
        logger.info(f"Partitioning by: {partition_columns}")

        try:
            df.write.mode("overwrite").partitionBy(*partition_columns).parquet(output_path)
            logger.info("Data written successfully")
        except Exception as e:
            logger.error(f"Error writing data to {output_path}: {str(e)}")
            raise

    @staticmethod
    def show_sample_data(df: DataFrame, num_rows: int = 5) -> None:
        """
        Display sample data for verification

        Args:
            df: DataFrame to sample
            num_rows: Number of rows to display
        """
        logger.info(f"Showing sample data ({num_rows} rows):")
        df.show(num_rows)