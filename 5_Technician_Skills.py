import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL

# --- Page Configuration ---
st.set_page_config(page_title="Technician Managed Skills", page_icon="👨‍💻")
st.title("👨‍💻 Technician Managed Skill Analysis")

# --- Initialization ---
dal = AnalyticsDAL()

@st.cache_data(ttl=600)
def load_technicians():
    """Loads a list of all active technicians."""
    return dal.get_all_technicians()

# --- UI ---
st.write("Select a technician to see a breakdown of the **Managed Skills** found in their tickets.")

tech_data = load_technicians()

if not tech_data:
    st.warning("No technician data found in the database.")
else:
    tech_map = {tech['TechnicianName']: tech['TechnicianID'] for tech in tech_data}
    tech_names = ["-- Select a Technician --"] + list(tech_map.keys())
    
    selected_tech_name = st.selectbox(
        "Technician:",
        options=tech_names
    )

    if selected_tech_name != "-- Select a Technician --":
        selected_tech_id = tech_map[selected_tech_name]
        
        with st.spinner(f"Fetching managed skills for {selected_tech_name}..."):
            # --- UPDATED: Call the new function ---
            skill_data = dal.get_managed_skills_by_technician(selected_tech_id)

            if not skill_data:
                st.info(f"No **managed skills** have been discovered for {selected_tech_name} yet.")
            else:
                df_skills = pd.DataFrame(skill_data)
                
                st.subheader(f"Top Managed Skills for {selected_tech_name}")

                # --- UPDATED: Use ManagedSkillName for the chart ---
                df_chart = df_skills.head(20).set_index('ManagedSkillName')
                st.bar_chart(df_chart)

                with st.expander("Show Raw Data"):
                    st.dataframe(df_skills, use_container_width=True)