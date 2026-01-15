import boto3
import io
import logging
import os
import json
import base64
import gspread
from google.oauth2.service_account import Credentials

class StorageService:
    def __init__(self, bucket_name, google_sheet_id=None,region='us-west-1'):
        self.bucket_name = bucket_name
        self.google_sheet_id = google_sheet_id
        
        self.s3_client = boto3.client(
            's3',
            region_name=os.getenv('AWS_REGION', region),
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )

        self.gsheet_client = self._init_google_client()

    def _init_google_client(self):
        """
        Decodes the Base64 JSON credentials and authenticates with Google.
        """
        encoded_creds = os.getenv('GOOGLE_CREDENTIALS_BASE64')
        if not encoded_creds:
            logging.warning("No Google Credentials found. GSheet export disabled.")
            return None
            
        try:
            creds_json = base64.b64decode(encoded_creds).decode('utf-8')
            creds_dict = json.loads(creds_json)
            
            scopes = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
            return gspread.authorize(creds)
        except Exception as e:
            logging.error(f"Failed to initialize Google Client: {e}")
            return None

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

    def append_to_sheet(self, dataframe):
        """
        Appends data to the configured Google Sheet (Data Only, No Header).
        """
        if not self.gsheet_client or not self.google_sheet_id:
            logging.warning("Skipping Google Sheet export (Credentials or ID missing).")
            return

        try:
            sheet = self.gsheet_client.open_by_key(self.google_sheet_id)
            worksheet = sheet.sheet1

            df_export = dataframe.copy()
            
            for col in df_export.select_dtypes(include=['datetime', 'datetimetz']).columns:
                df_export[col] = df_export[col].astype(str)

            data_to_upload = df_export.fillna('').values.tolist()    

            worksheet.append_rows(data_to_upload)
            
            logging.info(f"Successfully appended {len(data_to_upload)} rows to Google Sheet.")
        except Exception as e:
            logging.error(f"Google Sheet export failed: {e}")
