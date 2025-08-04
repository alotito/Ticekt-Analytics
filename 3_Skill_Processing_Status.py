# pages/3_Skill_Processing_Status.py

import streamlit as st
import pandas as pd
import sqlalchemy
import urllib
import configparser
import time
from zoneinfo import ZoneInfo # <-- ADD THIS IMPORT

st.set_page_config(page_title="Live Worker Status", page_icon="⚙️")
st.title("⚙️ Live Worker Dashboard")

# --- Database Connection ---
@st.cache_resource
def get_db_engine():
    # ... (This function remains the same) ...
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['analytics_db']
    password = urllib.parse.quote_plus(db_config['password'])
    connection_url = (
        f"mssql+pyodbc://{db_config['user']}:{password}@"
        f"{db_config['server']}/{db_config['database']}?"
        f"driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )
    return sqlalchemy.create_engine(connection_url)


@st.cache_data(ttl=5)
def load_live_status():
    """Queries the database and converts timestamps to local time."""
    in_progress_query = "SELECT WorkerID, SourceTicketNumber, LastUpdated FROM dbo.Tickets WHERE ProcessingStatusID = 1;"
    pending_query = "SELECT COUNT(*) FROM dbo.Tickets WHERE ProcessingStatusID = 0;"
    throughput_query = """
        SELECT COUNT(TicketID) AS TotalCompleted, MIN(LastUpdated) AS FirstCompletion, MAX(LastUpdated) AS LastCompletion
        FROM dbo.Tickets WHERE ProcessingStatusID = 2;
    """
    
    try:
        engine = get_db_engine()
        df_in_progress = pd.read_sql(in_progress_query, engine)
        
        # --- THIS IS THE FIX ---
        if not df_in_progress.empty:
            # Tell pandas the original time is UTC, then convert it to local time
            df_in_progress['LastUpdated'] = pd.to_datetime(df_in_progress['LastUpdated'])
            df_in_progress['LastUpdated'] = df_in_progress['LastUpdated'].dt.tz_localize('UTC').dt.tz_convert('America/New_York')
            # Format for display
            df_in_progress['LastUpdated'] = df_in_progress['LastUpdated'].dt.strftime('%Y-%m-%d %H:%M:%S')
        # --- END OF FIX ---

        tickets_per_hour = 0
        with engine.connect() as cnxn:
            result_pending = cnxn.execute(sqlalchemy.text(pending_query))
            pending_count = result_pending.scalar_one_or_none() or 0
            
            result_throughput = cnxn.execute(sqlalchemy.text(throughput_query)).first()
            if result_throughput and result_throughput.TotalCompleted > 1:
                total_completed, first_completion, last_completion = result_throughput
                if first_completion and last_completion:
                    duration_hours = (last_completion - first_completion).total_seconds() / 3600
                    if duration_hours > 0:
                        tickets_per_hour = total_completed / duration_hours
        
        # Note: df_history was removed as it wasn't being used in the final display
        return df_in_progress, pending_count, tickets_per_hour

    except Exception as e:
        st.error(f"Failed to load live data from the database: {e}")
        return pd.DataFrame(), 0, 0

# --- Display Dashboard ---
placeholder = st.empty()

while True:
    with placeholder.container():
        df_workers, pending_count, tph = load_live_status()

        col1, col2, col3 = st.columns(3)
        col1.metric("Tickets Remaining in Queue", f"{pending_count:,}")
        
        unique_workers = df_workers['WorkerID'].nunique() if not df_workers.empty else 0
        col2.metric("Active Workers", unique_workers)

        col3.metric("Avg. Tickets / Hour", f"{tph:,.0f}")

        st.subheader("Live Worker Activity")
        if not df_workers.empty:
            df_workers['SourceTicketNumber'] = df_workers['SourceTicketNumber'].astype(str)
            df_summary = df_workers.groupby('WorkerID').agg({
                'SourceTicketNumber': ', '.join,
                'LastUpdated': 'max'
            }).reset_index()
            df_summary = df_summary.rename(columns={
                'WorkerID': 'Worker ID', 'SourceTicketNumber': 'Processing Tickets', 'LastUpdated': 'Last Update Time'
            }).set_index('Worker ID')
            st.dataframe(df_summary, use_container_width=True)
        else:
            st.success("No workers are currently active.")

    time.sleep(5)