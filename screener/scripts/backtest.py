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

def get_expiry_price(ticker, exp_date_str):
    try:
        exp_date = datetime.strptime(exp_date_str, '%Y-%m-%d')
        hist = yf.download(ticker, start=exp_date, end=exp_date + timedelta(days=5), progress=False)
        if not hist.empty:
            close_val = hist['Close'].iloc[0]
            return close_val.iloc[0] if isinstance(close_val, pd.Series) else close_val
    except Exception:
        pass
    return None

def run_recent_review():
    s3 = boto3.client(
        's3',
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    bucket = os.getenv('S3_BUCKET_NAME')
    
    model = joblib.load('ml_models/covered_call_model_latest.joblib')
    with open('ml_models/model_config_latest.json', 'r') as f:
        config = json.load(f)

    response = s3.list_objects_v2(Bucket=bucket, Prefix='raw_data/')
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    
    # Filter for the last 14 days
    today = datetime.now()
    start_date = today - timedelta(days=14)
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    results = []

    for file_key in files:
        date_str = file_key.split('/')[-1].replace('.parquet', '')
        snapshot_date = datetime.strptime(date_str, '%Y-%m-%d')
        
        # Skip files older than 2 weeks
        if snapshot_date < start_date:
            continue

        obj = s3.get_object(Bucket=bucket, Key=file_key)
        df = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        if df.empty: continue

        macro_start = snapshot_date - timedelta(days=14)
        vix_hist = yf.download('^VIX', start=macro_start, end=snapshot_date + timedelta(days=1), progress=False)['Close']
        spy_hist = yf.download('SPY', start=macro_start, end=snapshot_date + timedelta(days=1), progress=False)['Close']
        if vix_hist.empty or len(spy_hist) < 6: continue
        
        vix = vix_hist.iloc[-1]
        spy_5d_ret = (spy_hist.iloc[-1] - spy_hist.iloc[-6]) / spy_hist.iloc[-6]

        df['vix'] = vix.iloc[0] if isinstance(vix, pd.Series) else vix
        df['spy_5d_return'] = spy_5d_ret.iloc[0] if isinstance(spy_5d_ret, pd.Series) else spy_5d_ret

        df['symbol'] = df['ticker']
        df['dte'] = df['days_to_expiry']
        df['distance_to_strike_pct'] = (df['strike'] - df['stock_price']) / df['stock_price']
        df['premium_yield'] = df['premium'] / df['stock_price']
        df['yield_to_iv_ratio'] = (df['premium_yield'] * (365 / df['dte'])) / (df['implied_volatility'] + 0.001)
        df['vol_oi_ratio'] = df['volume'] / (df['open_interest'] + 1)

        X = df[config['features']]
        df['win_probability'] = model.predict_proba(X)[:, 1]
        picks = df[df['win_probability'] >= config['threshold']].copy()
        
        if picks.empty: continue

    for idx, row in picks.iterrows():
                if datetime.strptime(row['expiration_date'], '%Y-%m-%d') < datetime.now():
                    actual_close = get_expiry_price(row['symbol'], row['expiration_date'])
                    
                    if actual_close is not None:
                        row['actual_close'] = actual_close
                        
                        # 1. Break-even check for a "Win"
                        break_even = row['stock_price'] - row['premium']
                        row['is_win'] = actual_close > break_even
                        
                        # 2. Net profit calculation
                        if actual_close >= row['strike']:
                            # Shares called away
                            row['net_profit'] = (row['strike'] - row['stock_price']) + row['premium']
                        else:
                            # Shares kept
                            row['net_profit'] = (actual_close - row['stock_price']) + row['premium']
                            
                        results.append(row)

    if not results:
        print("No expired trades found for the last 2 weeks.")
        return

    report = pd.DataFrame(results)
    report.to_csv("screener/backtest_results/recent_2_weeks_review.csv", index=False)

    win_rate = report['is_win'].mean()
    avg_profit = report['net_profit'].mean()
    
    print(f"\n2-Week Review ({start_date.strftime('%Y-%m-%d')} to {today.strftime('%Y-%m-%d')})")
    print("-" * 40)
    print(f"Expired Recommendations: {len(report)}")
    print(f"Realized Win Rate:       {win_rate:.2%}")
    print(f"Average Net Profit:      ${avg_profit:.2f} per share")
    print(f"Results saved to:        recent_2_weeks_review.csv")
    print("-" * 40)

if __name__ == "__main__":
    run_recent_review()
