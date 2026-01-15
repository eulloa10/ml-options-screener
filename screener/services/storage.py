import boto3
import io
import logging
import os

class StorageService:
    def __init__(self, bucket_name, region='us-west-1'):
        self.bucket_name = bucket_name
        
        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', region),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

    def upload_parquet(self, dataframe, file_key):
        try:
            buffer = io.BytesIO()
            dataframe.to_parquet(buffer, index=False)
            buffer.seek(0)

            self.s3_client.put_object(
                Body=buffer.getvalue(),
                Bucket=self.bucket_name,
                Key=file_key,
                ServerSideEncryption='AES256'
            )
            logging.info(f"Successfully uploaded {file_key}")
        except Exception as e:
            logging.error(f"S3 Upload failed: {e}")
            raise e
