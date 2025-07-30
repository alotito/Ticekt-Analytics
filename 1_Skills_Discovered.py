# pages/1_Skills_Discovered.py
import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL

# --- Page Configuration ---
st.set_page_config(page_title="Skill Analytics", page_icon="🛠️")
st.title("🛠️ Skill Analytics")

# --- Initialization ---
dal = AnalyticsDAL()

if "active_skill_tab" not in st.session_state:
    st.session_state.active_skill_tab = "Discovered Skills"

# --- Data Loading Functions ---
@st.cache_data(ttl=600)
def load_top_discovered_skills():
    """Queries the database for the most frequent raw skills."""
    return pd.DataFrame(dal.get_top_discovered_skills(count=20))

@st.cache_data(ttl=600)
def load_top_managed_skills():
    """Queries the database for the most frequent managed skill categories."""
    return pd.DataFrame(dal.get_managed_skill_occurrences())

def refresh_skill_caches():
    load_top_discovered_skills.clear()
    load_top_managed_skills.clear()

# --- UI Navigation ---
st.session_state.active_skill_tab = st.radio(
    "Select a view:",
    ["Discovered Skills", "Managed Skills"],
    horizontal=True,
    key="skill_view_radio"
)
st.markdown("---")

# =================================================================================
# VIEW 1: DISCOVERED SKILLS
# =================================================================================
if st.session_state.active_skill_tab == "Discovered Skills":
    st.subheader("Top 20 Most Frequent Discovered Skills")
    st.info("This shows the raw skills extracted by the AI, ranked by frequency.")
    
    if st.button("Refresh Data"):
        refresh_skill_caches()
    
    df_skills = load_top_discovered_skills()

    if not df_skills.empty:
        df_chart = df_skills.set_index('DiscoveredSkillName')
        st.bar_chart(df_chart)
        with st.expander("Show Raw Data"):
            st.dataframe(df_skills)
    else:
        st.warning("No skill data found. Please run the back-end analysis script to populate the database.")

# =================================================================================
# VIEW 2: MANAGED SKILLS
# =================================================================================
elif st.session_state.active_skill_tab == "Managed Skills":
    st.subheader("Managed Skill Frequency")
    st.info("This shows the total occurrences of all discovered skills, grouped by their assigned managed skill category.")

    if st.button("Refresh Data"):
        refresh_skill_caches()
        
    df_managed = load_top_managed_skills()
    
    if not df_managed.empty:
        df_chart = df_managed.set_index('ManagedSkillName')
        st.bar_chart(df_chart)
        with st.expander("Show Raw Data"):
            st.dataframe(df_managed)
    else:
        st.warning("No managed skills have been associated yet. Please use the 'Skill Management' page to categorize skills.")