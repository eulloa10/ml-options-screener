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

    # Handle Specialized Date Formats (e.g., 2026/04/30 or N/A)
    # Postgres DATE type requires YYYY-MM-DD
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

    # Replace invalid numeric strings with 0
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

    # Ensure AI score is a clean float
    if 'ai_confidence_score' in clean_df.columns:
        clean_df['ai_confidence_score'] = pd.to_numeric(clean_df['ai_confidence_score']).round(2)

    return clean_df
