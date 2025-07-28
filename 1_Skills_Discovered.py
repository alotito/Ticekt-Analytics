# pages/1_Skill_Analytics.py
import streamlit as st
import pandas as pd
import pytds
import configparser

st.set_page_config(page_title="Skill Analytics", page_icon="🛠️")
st.title("🛠️ Skill Analytics Dashboard")

# --- Database Connection ---
@st.cache_resource
def get_db_connection():
    """Establishes a connection to the TicketAnalytics database."""
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
        server=server, database=database, user=user, password=password, port=port, as_dict=True
    )

@st.cache_data(ttl=60)
def load_top_skills():
    """Queries the database to get the most frequently discovered skills."""
    query = """
        SELECT TOP 20
            ds.DiscoveredSkillName,
            COUNT(ts.DiscoveredSkillID) AS Frequency
        FROM dbo.TicketSkills ts
        JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
        GROUP BY ds.DiscoveredSkillName
        ORDER BY Frequency DESC;
    """
    try:
        cnxn = get_db_connection()
        cursor = cnxn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cols = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=cols)
        return df
    except Exception as e:
        st.error(f"Failed to load data from the database: {e}")
        return pd.DataFrame()

# --- Display Dashboard ---
st.write("This dashboard shows analytics based on the data processed by the back-end script.")

if st.button("Refresh Data"):
    st.cache_data.clear()
    st.cache_resource.clear()
    st.rerun()

df_skills = load_top_skills()

if not df_skills.empty:
    st.subheader("Top 20 Most Frequent Skills")
    df_chart = df_skills.set_index('DiscoveredSkillName')
    st.bar_chart(df_chart)
    
    st.subheader("Raw Skill Frequency Data")
    st.dataframe(df_skills)
else:
    st.warning("No skill data found. Please run the `run_backend.bat` script to populate the database.")