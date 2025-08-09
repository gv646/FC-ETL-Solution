from pyspark.sql import DataFrame
from pyspark.sql.functions import col, concat_ws, lit
from datetime import date
import logging

logger = logging.getLogger(__name__)


class DataTransformer:
    """Handles data transformation operations"""

    @staticmethod
    def filter_by_currency(df: DataFrame, currency: str = "USD") -> DataFrame:
        """
        Filter DataFrame by currency

        Args:
            df: Input DataFrame
            currency: Currency code to filter by

        Returns:
            Filtered DataFrame
        """
        logger.info(f"Filtering data for currency: {currency}")
        return df.filter(col("currency") == currency)

    @staticmethod
    def convert_to_aud(df: DataFrame, conversion_rate: float = 1.5) -> DataFrame:
        """
        Convert amount to AUD

        Args:
            df: Input DataFrame
            conversion_rate: USD to AUD conversion rate

        Returns:
            DataFrame with AUD amount column
        """
        logger.info(f"Converting amounts to AUD with rate: {conversion_rate}")
        return df.withColumn("amount_aud", col("amount") * conversion_rate)

    @staticmethod
    def create_full_name(df: DataFrame) -> DataFrame:
        """
        Create full_name column from first_name and last_name

        Args:
            df: Input DataFrame

        Returns:
            DataFrame with full_name column
        """
        logger.info("Creating full_name column")
        return df.withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))

    @staticmethod
    def add_ingestion_date(df: DataFrame, ingestion_date: date) -> DataFrame:
        """
        Add ingestion_date column

        Args:
            df: Input DataFrame
            ingestion_date: Date to add as ingestion_date

        Returns:
            DataFrame with ingestion_date column
        """
        logger.info(f"Adding ingestion_date: {ingestion_date}")
        return df.withColumn("ingestion_date", lit(ingestion_date))

    @staticmethod
    def apply_all_transformations(df: DataFrame, ingestion_date: date,
                                  currency: str = "USD", conversion_rate: float = 1.5) -> DataFrame:
        """
        Apply all transformations in sequence

        Args:
            df: Input DataFrame
            ingestion_date: Date to add as ingestion_date
            currency: Currency to filter by
            conversion_rate: USD to AUD conversion rate

        Returns:
            Fully transformed DataFrame
        """
        return (df
                .transform(lambda x: DataTransformer.filter_by_currency(x, currency))
                .transform(lambda x: DataTransformer.convert_to_aud(x, conversion_rate))
                .transform(DataTransformer.create_full_name)
                .transform(lambda x: DataTransformer.add_ingestion_date(x, ingestion_date)))