import pandas as pd
import numpy as np
from datetime import datetime

def clean_column_names(df):
    """
    Standardizes all column names to snake_case for Postgres compatibility.
    Example: 'AI_Confidence_Score' -> 'ai_confidence_score'
    """
    df.columns = [
        c.lower()
        .replace(' ', '_')
        .replace('-', '_')
        .replace('/', '_')
        for c in df.columns
    ]
    return df

def transform_inference_to_db(df):
    """
    The main ETL transformation logic.
    Converts raw scraper/inference data into a clean SQL-ready format.
    """
    clean_df = df.copy()

    if 'snapshot_date' in clean_df.columns:
        clean_df['report_date'] = pd.to_datetime(clean_df['snapshot_date']).dt.strftime('%Y-%m-%d')
    else:
        clean_df['report_date'] = datetime.now().strftime('%Y-%m-%d')

    clean_df = clean_column_names(clean_df)

    pct_cols = [
        'percent_change', 'premium_return', 'annualized_return', 
        'spy_5d_return', 'distance_to_strike_pct'
    ]
    for col in pct_cols:
        if col in clean_df.columns:
            clean_df[col] = (
                clean_df[col]
                .astype(str)
                .str.replace('%', '', regex=False)
                .str.replace('N/A', '0', regex=False)
            )
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce').fillna(0)

    date_cols = ['expiration_date', 'earnings_date', 'last_trade_date']
    for col in date_cols:
        if col in clean_df.columns:
            clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce')
            # For columns like last_trade_date that include time, we keep the timestamp
            if col == 'last_trade_date':
                clean_df[col] = clean_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                clean_df[col] = clean_df[col].dt.strftime('%Y-%m-%d')

    # Numeric Cleaning (Handle 'N/A', 'inf', and Booleans)
    # Convert 'FALSE'/'TRUE' strings to actual Booleans for Postgres
    if 'in_the_money' in clean_df.columns:
        clean_df['in_the_money'] = clean_df['in_the_money'].astype(str).str.upper() == 'TRUE'

    # --- FIX 1: Fix the Pandas Deprecation Warning ---
    pd.set_option('future.no_silent_downcasting', True)
    
    # Replace invalid string values with Numpy's NaN
    clean_df = clean_df.replace(['N/A', 'inf', '-inf', 'None'], np.nan)
    
    # Final Mapping to SQL Schema Names
    # We rename columns to match the 'Gold' table schema in Supabase
    mapping = {
        'premium_return': 'premium_return_pct',
        'annualized_return': 'annualized_return_pct',
        'out_of_the_money': 'out_of_the_money_pct',
        'return_per_day': 'return_per_day_pct',
        'spy_5d_return': 'spy_5d_return_pct'
    }
    clean_df = clean_df.rename(columns=mapping)

    if 'ai_confidence_score' in clean_df.columns:
        clean_df['ai_confidence_score'] = pd.to_numeric(clean_df['ai_confidence_score']).round(2)


    for col in clean_df.columns:
        if pd.api.types.is_datetime64_any_dtype(clean_df[col]):
            clean_df[col] = clean_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')

    clean_df = clean_df.where(pd.notna(clean_df), None)

    db_schema_columns = [
        'report_date', 'company_name', 'ticker', 'contract_name',
        'expiration_date', 'last_trade_date', 'stock_price',
        'strike', 'premium', 'bid', 'ask', 'change', 'percent_change',
        'volume', 'open_interest', 'implied_volatility',
        'delta', 'gamma', 'theta', 'vega', 'rho', 'days_to_expiry',
        'contract_size', 'premium_return_pct', 'annualized_return_pct',
        'out_of_the_money_pct', 'max_gain', 'max_loss', 'break_even',
        'risk_reward_ratio', 'return_per_day_pct', 'in_the_money',
        'pe_ratio', 'stock_volume', 'stock_average_volume', 'market_cap',
        'stock_beta', 'industry', 'average_analyst_rating', 'earnings_date',
        'dividend_date', 'dividend_yield', 'vix', 'spy_5d_return_pct',
        'yield_to_iv_ratio', 'vol_oi_ratio', 'distance_to_strike_pct',
        'ai_confidence_score'
    ]
    
    # Keep only the columns that exist in BOTH the dataframe and our schema list
    final_cols = [c for c in db_schema_columns if c in clean_df.columns]
    clean_df = clean_df[final_cols]

    return clean_df
