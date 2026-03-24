import pandas as pd
import joblib
import json
import os
import io
import boto3
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

from screener.modules.transformations import transform_inference_to_db
from screener.modules.database import upsert_trade_report 

load_dotenv()

def get_live_predictions():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    bucket = os.getenv('S3_BUCKET_NAME')

    print("Downloading latest model from S3...")
    s3.download_file(bucket, 'models/covered_call_model_latest.joblib', 'temp_model.joblib')
    s3.download_file(bucket, 'models/model_config_latest.json', 'temp_config.json')

    model = joblib.load('temp_model.joblib')
    with open('temp_config.json', 'r') as f:
        config = json.load(f)

    os.remove('temp_model.joblib')
    os.remove('temp_config.json')

    today_str = datetime.now().strftime('%Y-%m-%d')
    file_key = f"raw_data/{today_str}.parquet"
    
    print(f"Fetching today's options data: {file_key}")
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
    except s3.exceptions.NoSuchKey:
        print(f"No data found for today ({today_str}) in S3. Did the scraper run?")
        return pd.DataFrame()

    # Fetch Live Macro Data
    print("Fetching live VIX and SPY data...")
    macro_data = yf.download(['^VIX', 'SPY'], period='14d', progress=False)['Close']
    current_vix = macro_data['^VIX'].iloc[-1]
    spy_series = macro_data['SPY']
    spy_5d_ret = (spy_series.iloc[-1] - spy_series.iloc[-6]) / spy_series.iloc[-6]

    # Feature Engineering
    df['vix'] = current_vix
    df['spy_5d_return'] = spy_5d_ret   
    df['symbol'] = df['ticker']
    df['dte'] = df['days_to_expiry']
    df['distance_to_strike_pct'] = (df['strike'] - df['stock_price']) / df['stock_price']
    df['premium_yield'] = df['premium'] / df['stock_price']
    df['yield_to_iv_ratio'] = (df['premium_yield'] * (365 / (df['dte'] + 0.1))) / (df['implied_volatility'] + 0.001)
    df['vol_oi_ratio'] = df['volume'] / (df['open_interest'] + 1)

    print("Running AI inference...")
    X = df[config['features']]
    df['AI_Confidence_Score'] = model.predict_proba(X)[:, 1] * 100

    picks = df[df['AI_Confidence_Score'] > 80].sort_values('AI_Confidence_Score', ascending=False)

    if picks.empty:
        print(f"No trades met the >80% threshold for {today_str}.")
        return pd.DataFrame()
    

    print("Starting ETL to Supabase...")
    db_payload = transform_inference_to_db(picks)
    upsert_trade_report(db_payload)

    FINAL_COLUMN_ORDER = [
        'company_name', 'ticker', 'contract_name',
        'expiration_date', 'last_trade_date', 'stock_price',
        'strike', 'premium', 'bid',
        'ask', 'change', 'percent_change',
        'volume', 'open_interest', 'implied_volatility',
        'delta',  'gamma', 'theta',
        'vega', 'rho', 'days_to_expiry',
        'contract_size',  'premium_return', 'annualized_return',
        'out_of_the_money', 'max_gain', 'max_loss',
        'break_even', 'risk_reward_ratio', 'return_per_day',
        'in_the_money', 'pe_ratio', 'stock_volume',
        'stock_average_volume', 'market_cap', 'stock_beta',
        'industry', 'average_analyst_rating', 'earnings_date',
        'dividend_date', 'dividend_yield',
        'vix', 'spy_5d_return', 'yield_to_iv_ratio', 
        'vol_oi_ratio', 'distance_to_strike_pct', 'AI_Confidence_Score', 
        'snapshot_date'
    ]
    
    final_report = picks[FINAL_COLUMN_ORDER].copy()
    
    cols_to_pct = ['premium_return', 'annualized_return', 'spy_5d_return', 'distance_to_strike_pct']
    for col in cols_to_pct:
        final_report[col] = (final_report[col] * 100).round(2).astype(str) + '%'
    
    final_report['AI_Confidence_Score'] = final_report['AI_Confidence_Score'].round(2)

    export_filename = f"daily_ai_report_{today_str}.csv"
    final_report.to_csv(export_filename, index=False)
    
    print(f"Uploading report to S3: ai_reports/{export_filename}")
    s3.upload_file(export_filename, bucket, f"ai_reports/{export_filename}")

    os.remove(export_filename)
    
    return final_report

if __name__ == "__main__":
    recommendations = get_live_predictions()
    if not recommendations.empty:
        print(f"✅ Inference complete. {len(recommendations)} trades identified and uploaded to S3.")
    else:
        print("No high-confidence trades found today.")
