import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

def get_supabase_client() -> Client:
    """
    Initializes and returns a Supabase client using the Service Role Key.
    The Service Role Key is required to bypass RLS for automated ETL tasks.
    """
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not found in environment variables.")
        
    return create_client(url, key)

def upsert_trade_report(df: pd.DataFrame, table_name: str = "daily_picks"):
    """
    Performs a PostgreSQL UPSERT on the specified table.
    If a trade (unique by ticker, strike, expiry, and report_date) exists, it updates.
    Otherwise, it inserts a new row.
    """
    if df.empty:
        print(f"⚠️ {table_name}: No data provided for upsert. Skipping.")
        return None

    supabase = get_supabase_client()

    records = df.to_dict(orient='records')

    try:
        response = supabase.table(table_name).upsert(
            records, 
            on_conflict="ticker, strike, expiration_date, report_date"
        ).execute()
        
        print(f"✅ Successfully synced {len(records)} rows to '{table_name}'.")
        return response
        
    except Exception as e:
        print(f"❌ Supabase Error during upsert to '{table_name}': {e}")
        return None

if __name__ == "__main__":
    print("Testing Supabase Connection...")
    try:
        client = get_supabase_client()
        test_query = client.table("daily_picks").select("id").limit(1).execute()
        print("✅ Connection Successful! Database is reachable.")
    except Exception as e:
        print(f"❌ Connection Failed: {e}")
