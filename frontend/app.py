import streamlit as st
from supabase import create_client, Client
import pandas as pd

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
            st.rerun() # Reloads the script from the top
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

@st.cache_data(ttl=600) # Caches data for 10 minutes
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
    # Get the latest date in the database
    latest_date = df['report_date'].max()
    st.success(f"Latest Data Synced: {latest_date}")
    st.subheader("Today's Top Picks")
    today_df = df[df['report_date'] == latest_date].sort_values(by='ai_confidence_score', ascending=False)
    
    display_cols = ['ticker', 'stock_price', 'strike', 'expiration_date', 'premium', 'annualized_return_pct','ai_confidence_score']

    my_column_config = {
            "ticker": st.column_config.TextColumn("Ticker", width="small"),
            "stock_price": st.column_config.NumberColumn("Price", format="$%.2f", width="small"),
            "strike": st.column_config.NumberColumn("Strike", format="$%.2f", width="small"),
            "expiration_date": st.column_config.DateColumn("Expiry", format="MMM DD, YYYY", width="medium"),
            "premium": st.column_config.NumberColumn("Premium", format="$%.2f", width="small"),
            "annualized_return_pct": st.column_config.NumberColumn("Annualized Return", format="%.1f%%", width="small"),
            "ai_confidence_score": st.column_config.ProgressColumn(
                "Win Probability",
                help="Machine Learning Confidence",
                format="%.1f",
                min_value=0,
                max_value=100,
                width="medium" 
            )
        }

    st.dataframe(
        today_df[display_cols],
        width='stretch',
        hide_index=True,
        column_config=my_column_config
    )

    st.divider()

    st.subheader("Historical Archive")
        
    past_dates = df[df['report_date'] < latest_date]['report_date'].unique()
    
    if len(past_dates) == 0:
        st.info("No historical data available yet. Check back tomorrow after the next run.")
    else:
        # Create a dropdown to select a past date (defaults to the most recent past date)
        selected_date = st.selectbox("Select a date to view past picks:", sorted(past_dates, reverse=True))
        
        historical_df = df[df['report_date'] == selected_date].sort_values(by='ai_confidence_score', ascending=False)
        
        st.dataframe(
            historical_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config=my_column_config
    )
