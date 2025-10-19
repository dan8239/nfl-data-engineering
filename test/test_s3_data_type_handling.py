import io
import unittest
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd

from src.s3_io.s3_client import S3Client


class TestS3DataTypeHandling(unittest.TestCase):
    """Test S3Client handling of various problematic data types that can cause Parquet encoding errors"""

    def setUp(self):
        """Set up test fixtures"""
        self.s3_client = S3Client()
        # Mock the s3_client to avoid actual AWS calls
        self.s3_client.s3_client = Mock()

    def test_mixed_type_numeric_string_columns(self):
        """Test columns with mixed numeric and string data"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'mixed_col': ['123', 456, '789'],  # Mixed string and int
            'mixed_float': [1.5, '2.5', 3.5]   # Mixed float and string
        })

        # This should not raise an error
        try:
            buffer = io.BytesIO()
            # Apply the same conversion logic that should be in push_dataframe_to_s3
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to convert mixed type columns: {e}")

    def test_object_columns_with_none(self):
        """Test object columns containing None values"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'col_with_none': ['value1', None, 'value3'],
            'col_with_nan': ['value1', np.nan, 'value3']
        })

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to handle None/NaN values: {e}")

    def test_percentage_string_columns(self):
        """Test columns with percentage strings"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'pct_col': ['50%', '75.5%', '100%'],
            'mixed_pct': ['50%', 0.75, '100%']  # Mixed percentage strings and floats
        })

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to handle percentage columns: {e}")

    def test_columns_with_special_characters(self):
        """Test columns with special characters like --, +"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'special_col': ['--', '++', '10+'],
            'dash_col': ['5-3', '--', '8-2']
        })

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to handle special characters: {e}")

    def test_empty_string_columns(self):
        """Test columns with empty strings"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'empty_col': ['', 'value', ''],
            'mixed_empty': ['', 123, '']
        })

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to handle empty strings: {e}")

    def test_all_object_dtype_columns(self):
        """Test that all object dtype columns are properly converted"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'obj_col1': [1, 2, 3],  # Integer stored as object
            'obj_col2': [1.5, 2.5, 3.5],  # Float stored as object
            'obj_col3': ['a', 'b', 'c']  # String
        })

        # Convert all to object dtype to simulate the issue
        for col in df.columns:
            if col != 'team':
                df[col] = df[col].astype(object)

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)

            # Verify types were converted properly
            self.assertNotEqual(df_converted['obj_col1'].dtype, object)
            self.assertNotEqual(df_converted['obj_col2'].dtype, object)
        except Exception as e:
            self.fail(f"Failed to convert object dtype columns: {e}")

    def test_offense_scoring_ep_pcnt_last3_reproduction(self):
        """Test to reproduce the specific error from the log with offense_scoring_ep_pcnt_last3"""
        # Simulate data that might cause the UTF-8 encoding error
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'offense_scoring_ep_pcnt_last3': ['50%', '', '75.5%'],
            'other_col': [1, 2, 3]
        })

        # Convert to object dtype (simulating the scraper output)
        df['offense_scoring_ep_pcnt_last3'] = df['offense_scoring_ep_pcnt_last3'].astype(object)

        try:
            buffer = io.BytesIO()
            df_converted = self._convert_dataframe_types(df)
            df_converted.to_parquet(buffer, engine="fastparquet", compression="snappy", index=False)
            buffer.seek(0)
        except Exception as e:
            self.fail(f"Failed to handle offense_scoring_ep_pcnt_last3 column: {e}")

    def test_push_dataframe_to_s3_with_problematic_data(self):
        """Integration test for push_dataframe_to_s3 with various problematic data types"""
        df = pd.DataFrame({
            'team': ['Team A', 'Team B', 'Team C'],
            'mixed_type': [1, '2', 3.0],
            'empty_strings': ['', 'value', ''],
            'percentages': ['50%', '75%', '100%'],
            'none_values': [1.0, None, 3.0],
            'special_chars': ['--', '++', '10']
        })

        # Convert some columns to object dtype
        for col in ['mixed_type', 'empty_strings', 'percentages']:
            df[col] = df[col].astype(object)

        try:
            self.s3_client.push_dataframe_to_s3(df, 'test-bucket', 'test-key')
            self.s3_client.s3_client.upload_fileobj.assert_called_once()
        except Exception as e:
            self.fail(f"push_dataframe_to_s3 failed with problematic data: {e}")

    def _convert_dataframe_types(self, df):
        """
        Helper method that uses the S3Client's type conversion logic.
        """
        return self.s3_client._convert_dataframe_types(df)


if __name__ == '__main__':
    unittest.main()
