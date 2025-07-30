import streamlit as st
import pandas as pd
import sqlalchemy
import urllib
import configparser
import time

st.set_page_config(page_title="Live Worker Status", page_icon="⚙️")
st.title("⚙️ Live Worker Dashboard")

# --- Database Connection ---
@st.cache_resource
def get_db_engine():
    """Establishes a connection engine to the TicketAnalytics database using SQLAlchemy."""
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
    """Queries the database for live worker status and queue count."""
    in_progress_query = "SELECT WorkerID, SourceTicketNumber, LastUpdated FROM dbo.Tickets WHERE ProcessingStatusID = 1;"
    pending_query = "SELECT COUNT(*) FROM dbo.Tickets WHERE ProcessingStatusID = 0;"
    history_query = "SELECT TOP 10 * FROM dbo.AnalysisRuns ORDER BY RunStartTime DESC;"
    # --- ADDED: Query to calculate throughput ---
    throughput_query = """
        SELECT
            COUNT(TicketID) AS TotalCompleted,
            MIN(LastUpdated) AS FirstCompletion,
            MAX(LastUpdated) AS LastCompletion
        FROM dbo.Tickets
        WHERE ProcessingStatusID = 2;
    """
    
    try:
        engine = get_db_engine()
        
        df_history = pd.read_sql(history_query, engine)
        df_in_progress = pd.read_sql(in_progress_query, engine)
        
        tickets_per_hour = 0
        with engine.connect() as cnxn:
            # Get pending count
            result_pending = cnxn.execute(sqlalchemy.text(pending_query))
            pending_count = result_pending.scalar_one_or_none() or 0
            
            # --- ADDED: Calculate tickets per hour ---
            result_throughput = cnxn.execute(sqlalchemy.text(throughput_query)).first()
            if result_throughput and result_throughput.TotalCompleted > 1:
                total_completed, first_completion, last_completion = result_throughput
                if first_completion and last_completion:
                    duration_hours = (last_completion - first_completion).total_seconds() / 3600
                    if duration_hours > 0:
                        tickets_per_hour = total_completed / duration_hours
        
        return df_history, df_in_progress, pending_count, tickets_per_hour

    except Exception as e:
        st.error(f"Failed to load live data from the database: {e}")
        return pd.DataFrame(), pd.DataFrame(), 0, 0

# --- Display Dashboard ---
placeholder = st.empty()

while True:
    with placeholder.container():
        df_history, df_workers, pending_count, tph = load_live_status()

        # --- UPDATED: Added a third column for the new metric ---
        col1, col2, col3 = st.columns(3)
        col1.metric("Tickets Remaining in Queue", f"{pending_count:,}")
        
        unique_workers = df_workers['WorkerID'].nunique() if not df_workers.empty else 0
        col2.metric("Active Workers", unique_workers)

        col3.metric("Avg. Tickets / Hour", f"{tph:,.0f}")
        # --- END OF UPDATE ---

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

        st.subheader("Recent Run History")
        if not df_history.empty:
            st.dataframe(df_history, use_container_width=True)
        else:
            st.info("No run history found.")

    time.sleep(5)