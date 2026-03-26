import streamlit as st
from supabase import create_client, Client
import pandas as pd
import datetime

st.set_page_config(
    page_title="CC Options Tracker",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed"
)

def check_password():
    """Returns `True` if the user has the correct password."""
    if st.session_state.get("password_correct", False):
        return True

    st.text_input("Enter Passcode to access the Dashboard", type="password", key="pwd_input")
    
    if st.session_state["pwd_input"]:
        if st.session_state["pwd_input"] == st.secrets["APP_PASSCODE"]:
            st.session_state["password_correct"] = True
            st.rerun() 
        else:
            st.error("❌ Incorrect Passcode")
            
    return False

if not check_password():
    st.stop()

@st.cache_resource
def init_connection() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

@st.cache_data(ttl=600) 
def load_data():
    response = supabase.table("daily_picks").select("*").order("report_date", desc=True).execute()
    if response.data:
        return pd.DataFrame(response.data)
    return pd.DataFrame()

df = load_data()

st.title("Covered Call Screener")

if df.empty:
    st.warning("No data found in the database. Run your inference pipeline first!")
else:
    # --- 1. GLOBAL DATA PREP ---
    # Convert string dates to datetime.date objects immediately to avoid TypeErrors
    df['report_date'] = pd.to_datetime(df['report_date']).dt.date
    
    # Create the display column for the entire dataset
    df['ticker_display'] = df['ticker'] + " (" + df['company_name'] + ")"

    display_cols = [
        'ticker_display', 'stock_price', 'strike', 
        'expiration_date', 'premium', 'annualized_return_pct', 
        'ai_confidence_score'
    ]
    
    my_column_config = {
        "ticker_display": st.column_config.TextColumn("Ticker", width="medium"),
        "stock_price": st.column_config.NumberColumn("Price", format="$%.2f", width="small"),
        "strike": st.column_config.NumberColumn("Strike", format="$%.2f", width="small"),
        "expiration_date": st.column_config.DateColumn("Expiry", format="MMM DD, YYYY", width="medium"),
        "premium": st.column_config.NumberColumn("Premium", format="$%.2f", width="small"),
        "annualized_return_pct": st.column_config.NumberColumn("Ann. Return", format="%.1f%%", width="small"),
        "ai_confidence_score": st.column_config.ProgressColumn(
            "Win Probability",
            help="Machine Learning Confidence",
            format="%.1f%%",
            min_value=0,
            max_value=100,
            width="medium" 
        )
    }

    # --- 2. MARKET STATUS LOGIC ---
    today_date = datetime.date.today()
    latest_db_date = df['report_date'].max()

    if latest_db_date < today_date:
        st.info(f"📅 **Market Status:** No high-confidence trades identified for {today_date.strftime('%B %d')}. Showing most recent data from {latest_db_date.strftime('%B %d')}.")
    else:
        st.success(f"✅ **Live Data:** Showing top picks for {latest_db_date.strftime('%B %d')}")

    # --- 3. TODAY'S TOP PICKS ---
    st.subheader("Today's Top Picks")
    today_df = df[df['report_date'] == latest_db_date].sort_values(by='ai_confidence_score', ascending=False)

    st.dataframe(
        today_df[display_cols],
        width='stretch',
        hide_index=True,
        column_config=my_column_config
    )

    st.divider()

    # --- 4. HISTORICAL ARCHIVE ---
    st.subheader("Historical Archive")
    
    # Get all unique dates for the dropdown
    all_available_dates = sorted(df['report_date'].unique(), reverse=True)
    
    if len(all_available_dates) == 0:
        st.info("No historical data available yet.")
    else:
        selected_date = st.selectbox(
            "Select a date to view past picks:", 
            all_available_dates,
            format_func=lambda x: x.strftime('%B %d, %Y')
        )
        
        historical_df = df[df['report_date'] == selected_date].sort_values(by='ai_confidence_score', ascending=False)
        
        st.dataframe(
            historical_df[display_cols],
            width='stretch',
            hide_index=True,
            column_config=my_column_config
        )
