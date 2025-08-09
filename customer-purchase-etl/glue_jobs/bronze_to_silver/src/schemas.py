class DataSchemas:
    """Centralized schema definitions"""

    @staticmethod
    def get_customer_purchase_schema():
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