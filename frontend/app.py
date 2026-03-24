import streamlit as st
from supabase import create_client, Client
import pandas as pd

def check_password():
    """Returns `True` if the user has the correct password."""
    if st.session_state.get("password_correct", False):
        return True

    # Show input for password
    st.text_input("Enter Passcode to access the AI Dashboard", type="password", key="pwd_input")
    
    if st.session_state["pwd_input"]:
        if st.session_state["pwd_input"] == st.secrets["APP_PASSCODE"]:
            st.session_state["password_correct"] = True
            st.rerun() # Reloads the script from the top
        else:
            st.error("❌ Incorrect Passcode")
            
    return False

if not check_password():
    st.stop()

# 1. Page Configuration (This gives it the App/Mobile feel)
st.set_page_config(
    page_title="CC Options Tracker",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed" # Better for mobile
)

# 2. Connect to Supabase
@st.cache_resource
def init_connection() -> Client:
    url = st.secrets["SUPABASE_URL"]
    key = st.secrets["SUPABASE_KEY"]
    return create_client(url, key)

supabase = init_connection()

# 3. Fetch Data with Caching (Crucial for Streamlit performance)
@st.cache_data(ttl=600) # Caches data for 10 minutes so it's snappy
def load_data():
    # Fetch all data, ordered by report date
    response = supabase.table("daily_picks").select("*").order("report_date", desc=True).execute()
    if response.data:
        return pd.DataFrame(response.data)
    return pd.DataFrame()

df = load_data()

# 4. Build the UI
st.title("Covered Call Screener")

if df.empty:
    st.warning("No data found in the database. Run your inference pipeline first!")
else:
    # Get the latest date in the database
    latest_date = df['report_date'].max()
    
    st.success(f"Latest Data Synced: {latest_date}")

    # --- SPRINT 3 REQUIREMENT: "Daily Top Picks" Table ---
    st.subheader("Today's Top Picks")
    
    # Filter for today's data and sort by AI confidence
    today_df = df[df['report_date'] == latest_date].sort_values(by='ai_confidence_score', ascending=False)
    
    # Display a clean, mobile-friendly table
    display_cols = ['ticker', 'stock_price', 'strike', 'expiration_date', 'premium', 'annualized_return_pct','ai_confidence_score']
    st.dataframe(
        today_df[display_cols],
        width='stretch',
        hide_index=True,
        column_config={
            "ai_confidence_score": st.column_config.ProgressColumn(
                "AI Score",
                help="Machine Learning Confidence",
                format="%.2f",
                min_value=0,
                max_value=100,
            ),
            "premium": st.column_config.NumberColumn("Premium", format="$%f")
        }
    )

    st.divider()

    # --- SPRINT 3 REQUIREMENT: "Historical Performance" Chart ---
    st.subheader("AI Confidence vs. Premium Return")
    
    # A simple scatter chart to visualize where the value is
    st.scatter_chart(
        data=df,
        x='ai_confidence_score',
        y='premium_return_pct',
        color='ticker',
        size='volume',
        width='stretch'
    )
