import os
import sys
import io
import pandas as pd
import yfinance as yf
import boto3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from services.storage import StorageService

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_s3_client():
    return boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-1'
    )

def move_to_archive(s3_client, bucket, key):
    """
    Moves a file from 'raw_data/' to 'raw_data/archive/'
    """
    new_key = key.replace('raw_data/', 'raw_data/archive/')
    logging.info(f"Archiving dead file: {key} -> {new_key}")
    
    try:
        s3_client.copy_object(
            Bucket=bucket,
            CopySource={'Bucket': bucket, 'Key': key},
            Key=new_key
        )
        s3_client.delete_object(Bucket=bucket, Key=key)
    except Exception as e:
        logging.error(f"Failed to archive {key}: {e}")

def load_existing_labels(bucket_name):
    """
    Downloads existing labeled data to see what we have already processed.
    Returns a set of unique signatures: "snapshot_date_ticker_strike_expiration"
    """
    s3 = get_s3_client()
    logging.info("Checking for previously labeled trades...")
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='training_data/')
    if 'Contents' not in response:
        return set()

    existing_ids = set()
    
    for obj in response['Contents']:
        key = obj['Key']
        if key.endswith('.parquet'):
            try:
                resp = s3.get_object(Bucket=bucket_name, Key=key)
                df = pd.read_parquet(
                    io.BytesIO(resp['Body'].read()), 
                    columns=['snapshot_date', 'ticker', 'strike', 'expiration_date']
                )
                
                df['expiration_date'] = pd.to_datetime(df['expiration_date'])
                df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])

                for _, row in df.iterrows():
                    sig = f"{row['snapshot_date'].strftime('%Y-%m-%d')}_{row['ticker']}_{row['strike']}_{row['expiration_date'].strftime('%Y-%m-%d')}"
                    existing_ids.add(sig)
            except Exception:
                continue
                
    logging.info(f"Found {len(existing_ids)} previously labeled trades.")
    return existing_ids

def process_and_archive_raw_data(bucket_name):
    """
    Scans 'raw_data/'. 
    - Loads all data into memory.
    - If a file is 'Dead' (all options expired), it is marked for archival.
    - Moves 'Dead' files to 'raw_data/archive/' so we don't scan them next time.
    """
    s3 = get_s3_client()
    logging.info(f"Scanning raw_data/ for processing...")
    
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix='raw_data/')
    if 'Contents' not in response:
        return pd.DataFrame()

    active_dfs = []
    
    files = [obj['Key'] for obj in response['Contents'] 
             if obj['Key'].endswith('.parquet') and 'archive/' not in obj['Key']]

    today = datetime.now()

    for key in files:
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=key)
            df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
            
            df['expiration_date'] = pd.to_datetime(df['expiration_date'])
            df['snapshot_date'] = pd.to_datetime(df['snapshot_date'])
            
            max_expiry = df['expiration_date'].max()
            
            if max_expiry < today:
                active_dfs.append(df)
                move_to_archive(s3, bucket_name, key)
            else:
                active_dfs.append(df)

        except Exception as e:
            logging.warning(f"Error processing {key}: {e}")

    if not active_dfs:
        return pd.DataFrame()

    return pd.concat(active_dfs, ignore_index=True)

def fetch_actual_prices(df):
    """
    Fetches historical closing prices for unique (Ticker, Expiration) tuples.
    Only called on rows that are confirmed EXPIRED and UNLABELED.
    """
    if df.empty:
        return df

    logging.info(f"Fetching actual prices for {len(df)} trades...")

    unique_tickers = df['ticker'].unique()
    price_map = {}

    start_date = df['expiration_date'].min() - timedelta(days=5)
    end_date = datetime.now()

    for ticker in unique_tickers:
        try:
            hist = yf.download(ticker, start=start_date, end=end_date, progress=False, ignore_tz=True)

            if isinstance(hist.columns, pd.MultiIndex):
                hist.columns = hist.columns.get_level_values(0)

            for date, row in hist.iterrows():
                date_str = date.strftime('%Y-%m-%d')
                price_map[(ticker, date_str)] = row['Close']
                
        except Exception as e:
            logging.warning(f"Could not fetch history for {ticker}: {e}")

    def get_price(row):
        lookup_date = row['expiration_date'].strftime('%Y-%m-%d')
        price = price_map.get((row['ticker'], lookup_date))
        
        if pd.isna(price):
            friday = (row['expiration_date'] - timedelta(days=1)).strftime('%Y-%m-%d')
            price = price_map.get((row['ticker'], friday))
            
        return price

    df = df.copy()
    df['final_price'] = df.apply(get_price, axis=1)
    
    labeled_df = df.dropna(subset=['final_price']).copy()
    
    return labeled_df

def calculate_outcomes(df):
    """
    Determines 'Win' or 'Loss' based on assigned vs not assigned.
    """
    df['assigned'] = df['final_price'] > df['strike']

    df['realized_value'] = df.apply(
        lambda x: (x['strike'] if x['assigned'] else x['final_price']) + (x['premium']), 
        axis=1
    )

    df['realized_return_pct'] = (df['realized_value'] - df['stock_price']) / df['stock_price']

    df['target_profitable'] = df['realized_return_pct'] > 0
    
    # High Quality = Annualized Return > 15% AND Profitable
    df['days_held'] = (df['expiration_date'] - df['snapshot_date']).dt.days
    df['days_held'] = df['days_held'].replace(0, 1) # Avoid div by zero
    
    df['realized_annual_return'] = df['realized_return_pct'] * (365 / df['days_held'])
    df['target_high_quality'] = df['realized_annual_return'] > 0.15

    return df

def run_labeling_pipeline():
    load_dotenv()
    bucket_name = os.getenv('S3_BUCKET_NAME')
    if not bucket_name:
        logging.error("S3_BUCKET_NAME not set in .env")
        return

    existing_signatures = load_existing_labels(bucket_name)

    raw_df = process_and_archive_raw_data(bucket_name)
    if raw_df.empty:
        logging.info("No raw data found to process.")
        return

    today = datetime.now()
    expired_mask = raw_df['expiration_date'] < today
    
    raw_df['signature'] = raw_df.apply(
        lambda r: f"{r['snapshot_date'].strftime('%Y-%m-%d')}_{r['ticker']}_{r['strike']}_{r['expiration_date'].strftime('%Y-%m-%d')}", 
        axis=1
    )
    
    new_mask = ~raw_df['signature'].isin(existing_signatures)
    to_process_df = raw_df[expired_mask & new_mask].copy()
    to_process_df.drop(columns=['signature'], inplace=True)

    if to_process_df.empty:
        logging.info("No NEW expired trades to label. Everything is up to date.")
        return

    logging.info(f"Found {len(to_process_df)} new expired trades to label.")

    labeled_df = fetch_actual_prices(to_process_df)
    
    if labeled_df.empty:
        logging.info("Could not fetch prices for the new data (might be delisted or API error).")
        return

    final_df = calculate_outcomes(labeled_df)
    storage = StorageService(bucket_name=bucket_name)
    today_str = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_key = f"training_data/labeled_batch_{today_str}.parquet"
    
    storage.upload_parquet(final_df, file_key)
    
    win_rate = final_df['target_profitable'].mean()
    logging.info(f"--- Batch Complete ---")
    logging.info(f"Uploaded: {file_key}")
    logging.info(f"Trades Labeled: {len(final_df)}")
    logging.info(f"Win Rate: {win_rate:.1%}")

if __name__ == "__main__":
    run_labeling_pipeline()
