import pandas as pd
import joblib
import json
import os
import io
import boto3
import yfinance as yf
from datetime import datetime
from dotenv import load_dotenv

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

    # Clean up temp files
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
    

    print("Fetching live VIX and SPY data...")
    # Get 14 days of history to ensure we can calculate a 5-day return
    macro_data = yf.download(['^VIX', 'SPY'], period='14d', progress=False)['Close']
    
    # Current VIX
    current_vix = macro_data['^VIX'].iloc[-1]
    
    # 5-Day SPY Return
    spy_series = macro_data['SPY']
    spy_5d_ret = (spy_series.iloc[-1] - spy_series.iloc[-6]) / spy_series.iloc[-6]

    df['vix'] = current_vix
    df['spy_5d_return'] = spy_5d_ret   

    df['symbol'] = df['ticker']
    df['dte'] = df['days_to_expiry']
    df['distance_to_strike_pct'] = (df['strike'] - df['stock_price']) / df['stock_price']
    df['premium_yield'] = df['premium'] / df['stock_price']
    df['yield_to_iv_ratio'] = (df['premium_yield'] * (365 / df['dte'])) / (df['implied_volatility'] + 0.001)
    df['vol_oi_ratio'] = df['volume'] / (df['open_interest'] + 1)

    print("Running AI inference...")
    X = df[config['features']]
    df['AI_Confidence_Score'] = model.predict_proba(X)[:, 1] * 100

    picks = df[df['AI_Confidence_Score'] > 80].sort_values('AI_Confidence_Score', ascending=False)

    if picks.empty:
        print("No trades met the >80% AI Confidence threshold today.")
        return pd.DataFrame()

    report_cols = ['symbol', 'strike', 'expiration_date', 'premium', 'premium_yield', 'AI_Confidence_Score']
    final_report = picks[report_cols].copy()
    
    final_report['AI_Confidence_Score'] = final_report['AI_Confidence_Score'].round(2)
    final_report['premium_yield'] = (final_report['premium_yield'] * 100).round(2).astype(str) + '%'

    export_filename = f"daily_ai_report_{today_str}.csv"
    # final_report.to_csv(export_filename, index=False)
    
    print(f"Uploading report to S3: daily_ai_reports/{export_filename}")
    s3.upload_file(export_filename, bucket, f"daily_ai_reports/{export_filename}")
    
    return final_report

if __name__ == "__main__":
    recommendations = get_live_predictions()
    if not recommendations.empty:
        print("\n" + "="*50)
        print("AI DAILY OPTIONS REPORT")
        print("="*50)
        print(recommendations.to_string(index=False))
        print("="*50)
        print(f"✅ Report saved as CSV and ready for review.")
