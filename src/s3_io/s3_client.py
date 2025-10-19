import io
import os

import boto3
import dotenv
import numpy as np
import pandas as pd
from botocore.exceptions import NoCredentialsError, PartialCredentialsError

dotenv.load_dotenv()


class S3Client:
    def __init__(self):
        """
        Initialize the S3 client with AWS credentials and region.
        """
        self.aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        self.aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        self.region_name = os.environ.get("AWS_REGION_NAME")
        local_exec = os.environ.get("LOCAL_EXECUTION", "false").lower() == "true"
        self.local_execution = local_exec
        self.s3_client = None
        self.initialize_session()

    def initialize_session(self):
        """
        Initialize the S3 session with provided credentials and region.
        """
        try:
            if self.local_execution:
                self.s3_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name,
                )
            else:
                self.s3_client = boto3.client("s3")
            print(f"Successfully initialized session for region: {self.region_name}")
        except (NoCredentialsError, PartialCredentialsError) as e:
            print(f"Error initializing session: {e}")
            raise

    def _convert_dataframe_types(self, df):
        """
        Convert DataFrame column types to ensure Parquet compatibility.
        Handles mixed types, object columns, and ensures proper type conversion.

        :param df: The Pandas DataFrame to convert (Pandas DataFrame).
        :return: DataFrame with properly typed columns (Pandas DataFrame).
        """
        df = df.copy()

        for col in df.columns:
            # Skip if already a proper numeric type
            if pd.api.types.is_numeric_dtype(df[col]) and df[col].dtype != object:
                continue

            # Try to convert to numeric first (handles strings like '123', '45.6', etc.)
            try:
                # This will convert numeric strings to numbers
                converted = pd.to_numeric(df[col], errors='coerce')
                # Only use the conversion if it successfully converted some non-null values
                if not converted.isna().all() and not df[col].equals(converted):
                    # Check if original had any valid numeric-like strings
                    if converted.notna().any():
                        df[col] = converted
                        continue
            except Exception:
                pass

            # Handle object dtype columns
            if df[col].dtype == object:
                # Try to infer the best type
                try:
                    df[col] = df[col].infer_objects(copy=False)
                except Exception:
                    pass

                # If still object type, ensure all values are properly typed
                if df[col].dtype == object:
                    # Replace empty strings with NaN
                    df[col] = df[col].replace('', pd.NA)

                    # Try numeric conversion one more time
                    try:
                        converted = pd.to_numeric(df[col], errors='coerce')
                        if not converted.isna().all():
                            df[col] = converted
                            continue
                    except Exception:
                        pass

                    # Convert to string as last resort, ensuring all values are strings
                    df[col] = df[col].astype(str)
                    # Replace 'None', 'nan', '<NA>' strings with actual NaN
                    df[col] = df[col].replace(['None', 'nan', '<NA>', 'NaN'], pd.NA)

        return df

    def push_dataframe_to_s3(self, df, bucket_name, s3_key):
        """
        Upload a Pandas DataFrame directly to S3 as a Parquet file.
        This method avoids writing to the disk by using an in-memory buffer.

        :param df: The Pandas DataFrame to upload (Pandas DataFrame).
        :param bucket_name: The name of the S3 bucket (string).
        :param s3_key: The S3 object key (path) where the Parquet file will be stored (string).
        :return: None
        """
        try:
            # Convert DataFrame types to ensure Parquet compatibility
            df = self._convert_dataframe_types(df)

            buffer = io.BytesIO()
            df.to_parquet(
                buffer, engine="fastparquet", compression="snappy", index=False
            )
            buffer.seek(0)
            self.s3_client.upload_fileobj(buffer, bucket_name, s3_key)
            print(f"DataFrame uploaded successfully to s3://{bucket_name}/{s3_key}")
        except Exception as e:
            print(f"Error uploading DataFrame to S3: {e}")
            raise

    def read_dataframe_from_s3(self, bucket_name, s3_key, columns=None):
        """
        Read specific columns of a Parquet file from S3 and load it into a Pandas DataFrame.

        :param bucket_name: The name of the S3 bucket (string).
        :param s3_key: The S3 object key (path) of the Parquet file (string).
        :param columns: List of column names to load (optional, list of strings).
        :return: A Pandas DataFrame containing the selected data (Pandas DataFrame).
        """
        try:
            buffer = io.BytesIO()
            self.s3_client.download_fileobj(bucket_name, s3_key, buffer)
            buffer.seek(0)  # Rewind the buffer to the start
            df = pd.read_parquet(buffer, engine="fastparquet", columns=columns)
            print(f"DataFrame loaded successfully from s3://{bucket_name}/{s3_key}")
            return df
        except Exception as e:
            print(f"Error reading DataFrame from S3: {e}")
            return None


if __name__ == "__main__":
    data = {
        "name": ["Alice", "Bob", "Charlie"],
        "age": np.array([25, 30, 35], dtype=np.int32),
        "height": np.array([5.5, 6.0, 5.9], dtype=np.float64),
    }

    df = pd.DataFrame(data)

    s3_client = S3Client()

    bucket_name = "djp-nfl-model"
    s3_key = "test/subfolder/test.parquet"

    s3_client.push_dataframe_to_s3(df, bucket_name, s3_key)

    loaded_df = s3_client.read_dataframe_from_s3(bucket_name, s3_key)
    print(f"full loaded df: {loaded_df}")

    loaded_df = s3_client.read_dataframe_from_s3(bucket_name, s3_key, ["name", "age"])
    print(f"short loaded df: {loaded_df}")
    print("done")
