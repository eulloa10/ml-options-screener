import pandas as pd
import joblib
import json
import os
import io
import boto3
import yfinance as yf
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_live_predictions():
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    model = joblib.load('ml_models/covered_call_model.joblib')
    with open('ml_models/model_config.json', 'r') as f:
        config = json.load(f)

    today_str = datetime.now().strftime('%Y-%m-%d')
    file_key = f"raw_data/{today_str}.parquet"
    
    print(f"Downloading {file_key} from S3 bucket {bucket_name}...")
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        df = pd.read_parquet(io.BytesIO(response['Body'].read()))
    except s3_client.exceptions.NoSuchKey:
        print(f"Error: No data found for {today_str}. Did you run the pull script?")
        return pd.DataFrame()

    print("Fetching live market regime data...")
    macro_start = datetime.now() - timedelta(days=14)
    
    vix = yf.download('^VIX', start=macro_start, progress=False)['Close'].iloc[-1]
    if isinstance(vix, pd.Series): vix = vix.iloc[0] 
    
    spy_hist = yf.download('SPY', start=macro_start, progress=False)['Close']
    spy_5d_ret = (spy_hist.iloc[-1] - spy_hist.iloc[-6]) / spy_hist.iloc[-6]
    if isinstance(spy_5d_ret, pd.Series): spy_5d_ret = spy_5d_ret.iloc[0]

    df['vix'] = vix
    df['spy_5d_return'] = spy_5d_ret

    # Aligning S3 production schema to Model research schema
    df['symbol'] = df['ticker']
    df['dte'] = df['days_to_expiry']

    df['distance_to_strike_pct'] = (df['strike'] - df['stock_price']) / df['stock_price']
    df['premium_yield'] = df['premium'] / df['stock_price']
    
    df['yield_to_iv_ratio'] = (df['premium_yield'] * (365 / df['dte'])) / (df['implied_volatility'] + 0.001)
    df['vol_oi_ratio'] = df['volume'] / (df['open_interest'] + 1)

    # 5. Predict
    # Ensure all features in config['features'] now exist in df
    X = df[config['features']]
    df['win_probability'] = model.predict_proba(X)[:, 1]
    
    # 6. Filter by Sweet Spot (0.65 threshold from optimization)
    return df[df['win_probability'] >= config['threshold']].sort_values('win_probability', ascending=False)

if __name__ == "__main__":
    recommendations = get_live_predictions()
    if not recommendations.empty:
        print("\n--- Model Approved Trades (AWS Source) ---")
        # Showing the top 10 trades passing the 75%+ win-rate threshold
        cols = ['symbol', 'strike', 'expiration_date', 'win_probability', 'premium_yield']
        print(recommendations[cols].head(20).to_string(index=False))
