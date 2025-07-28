# pages/3_Skill_Processing_Status.py
import streamlit as st
import pandas as pd
import pytds
import configparser

st.set_page_config(page_title="Skill Processing Status", page_icon="⚙️")
st.title("⚙️ Skill Processing Status")

# --- Database Connection ---
@st.cache_resource
def get_db_connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    db_config = config['analytics_db']
    
    server_and_port = db_config['server'].split(',')
    server = server_and_port[0]
    port = int(server_and_port[1]) if len(server_and_port) > 1 else 1433
    
    database = db_config['database']
    user = db_config['user']
    password = db_config['password']
    
    return pytds.connect(
        server=server, database=database, user=user, password=password, port=port
    )

@st.cache_data(ttl=60)
def load_status_data():
    query = "SELECT TOP 10 * FROM dbo.AnalysisRuns ORDER BY RunStartTime DESC;"
    try:
        cnxn = get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        return df
    except Exception as e:
        st.error(f"Failed to load status data from the database: {e}")
        return pd.DataFrame()

# --- Display Status ---
st.write("This page shows the status and history of the `run_analysis.py` backend script.")

if st.button("Refresh Status"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

df_status = load_status_data()

if not df_status.empty:
    latest_run = df_status.iloc[0]

    st.subheader("Latest Run")
    cols = st.columns(4)
    
    status = latest_run['Status']
    if status == 'Completed':
        cols[0].metric("Status", status, "✅")
    elif status == 'Running':
        cols[0].metric("Status", status, "⏳")
    else:
        cols[0].metric("Status", status, "❌")
        if pd.notna(latest_run['ErrorMessage']):
            st.error(f"**Error Message:** {latest_run['ErrorMessage']}")

    tickets_processed = latest_run['TicketsProcessed']
    if pd.notna(tickets_processed):
        cols[1].metric("Tickets Processed", f"{tickets_processed:.0f}")
    else:
        cols[1].metric("Tickets Processed", "0")

    run_start = latest_run['RunStartTime']
    run_end = latest_run['RunEndTime']
    if pd.notna(run_end):
        duration = run_end - run_start
        cols[2].metric("Duration (sec)", f"{duration.total_seconds():.2f}")
    else:
        cols[2].metric("Duration (sec)", "In Progress...")
    
    last_checkpoint = latest_run['LastTicketDateProcessed']
    if pd.notna(last_checkpoint):
        cols[3].metric("Checkpoint", last_checkpoint.strftime("%Y-%m-%d"))
    else:
        cols[3].metric("Checkpoint", "N/A")

    with st.expander("Current Configuration"):
        config = configparser.ConfigParser()
        config.read('config.ini')
        # --- UPDATED TO READ FROM [skills_settings] ---
        skill_config = config['skills_settings']
        st.text(f"Batch Size: {skill_config.get('batch_size')}")
        st.text(f"LLM Model: {skill_config.get('model')}")
        # --- End of update ---

    st.subheader("Recent Run History")
    st.dataframe(df_status)
else:
    st.warning("No run history found in the database. Please run the `run_backend.bat` script at least once.")