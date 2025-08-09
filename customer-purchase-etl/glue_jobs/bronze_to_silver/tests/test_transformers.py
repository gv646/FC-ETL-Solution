import pytest
from datetime import date
from transformers import DataTransformer


class TestDataTransformer:
    """Essential unit tests for DataTransformer class"""

    def test_filter_by_currency_usd(self, sample_df):
        """Test filtering by USD currency"""
        result_df = DataTransformer.filter_by_currency(sample_df, "USD")

        assert result_df.count() == 3
        currencies = [row.currency for row in result_df.collect()]
        assert all(currency == "USD" for currency in currencies)

    def test_convert_to_aud_default_rate(self, sample_df):
        """Test AUD conversion with default rate"""
        result_df = DataTransformer.convert_to_aud(sample_df)

        assert "amount_aud" in result_df.columns
        first_row = result_df.collect()[0]
        expected_aud = first_row.amount * 1.5
        assert abs(first_row.amount_aud - expected_aud) < 0.001

    def test_create_full_name(self, sample_df):
        """Test full name creation"""
        result_df = DataTransformer.create_full_name(sample_df)

        assert "full_name" in result_df.columns
        first_row = result_df.collect()[0]
        expected_name = f"{first_row.first_name} {first_row.last_name}"
        assert first_row.full_name == expected_name

    def test_add_ingestion_date(self, sample_df):
        """Test adding ingestion date"""
        test_date = date(2024, 2, 15)
        result_df = DataTransformer.add_ingestion_date(sample_df, test_date)

        assert "ingestion_date" in result_df.columns
        first_row = result_df.collect()[0]
        assert first_row.ingestion_date == test_date

    def test_apply_all_transformations(self, sample_df):
        """Test complete transformation pipeline"""
        test_date = date(2024, 3, 1)
        result_df = DataTransformer.apply_all_transformations(sample_df, test_date)

        # Check all transformations applied
        assert result_df.count() == 3  # Only USD records
        assert "amount_aud" in result_df.columns
        assert "full_name" in result_df.columns
        assert "ingestion_date" in result_df.columns

        # Verify first record transformations
        first_row = result_df.collect()[0]
        assert first_row.currency == "USD"
        assert first_row.amount_aud == first_row.amount * 1.5
        assert first_row.full_name == f"{first_row.first_name} {first_row.last_name}"
        assert first_row.ingestion_date == test_date