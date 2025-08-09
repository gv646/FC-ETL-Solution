# config.py
from pyspark.sql.types import *
from dataclasses import dataclass
from datetime import date


@dataclass
class ETLConfig:
    """Configuration class for ETL job parameters"""
    year: int
    month: int
    day: int
    input_path: str
    job_name: str

    @property
    def ingestion_date(self) -> date:
        return date(self.year, self.month, self.day)

    @property
    def output_path(self) -> str:
        return self.input_path.replace('bronze', 'silver').replace('.csv', '/')