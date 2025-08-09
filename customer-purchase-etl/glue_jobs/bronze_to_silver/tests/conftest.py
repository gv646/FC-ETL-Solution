import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing"""
    return (SparkSession.builder
            .appName("transformer_tests")
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate())

@pytest.fixture
def sample_data():
    """Sample data for testing transformers"""
    return [
        (1, "John", "Doe", "john@example.com", 101, date(2024, 1, 15), 100.0, "USD"),
        (2, "Jane", "Smith", "jane@example.com", 102, date(2024, 1, 16), 200.0, "USD"),
        (3, "Bob", "Johnson", "bob@example.com", 103, date(2024, 1, 17), 150.0, "EUR"),
        (4, "Alice", "Brown", "alice@example.com", 104, date(2024, 1, 18), 300.0, "USD"),
        (5, "Charlie", "Wilson", "charlie@example.com", 105, date(2024, 1, 19), 50.0, "GBP"),
    ]

@pytest.fixture
def customer_schema():
    """Schema for customer purchase data"""
    return StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("purchase_id", IntegerType(), True),
        StructField("purchase_date", DateType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True)
    ])

@pytest.fixture
def sample_df(spark, sample_data, customer_schema):
    """Create sample DataFrame for testing"""
    return spark.createDataFrame(sample_data, customer_schema)