# pages/6_Skill_Search.py

import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL

# --- Page Configuration ---
st.set_page_config(page_title="Skill Search", page_icon="💡")
st.title("💡 Managed Skill Search")

# --- Initialization ---
dal = AnalyticsDAL()

@st.cache_data(ttl=600)
def load_all_managed_skills():
    """Loads a list of all non-exception managed skills."""
    # We get the full dictionary and extract the names on the page
    all_skills_data = dal.get_managed_skills()
    # Filter out skills marked as exceptions
    return [skill['ManagedSkillName'] for skill in all_skills_data if not skill['IsException']]

# --- UI ---
st.write("Select a **Managed Skill** to see which technicians have the most experience with it, based on ticket counts.")

all_managed_skills = load_all_managed_skills()

if not all_managed_skills:
    st.warning("No managed skills have been created in the database yet.")
else:
    skill_options = ["-- Select a Skill --"] + sorted(all_managed_skills)
    
    selected_skill = st.selectbox(
        "Search for a managed skill:",
        options=skill_options
    )

    if selected_skill != "-- Select a Skill --":
        with st.spinner(f"Finding technicians with skill: {selected_skill}..."):
            # --- UPDATED: Call the new function ---
            tech_data = dal.get_technicians_by_managed_skill(selected_skill)

            if not tech_data:
                st.info(f"No technicians are associated with the managed skill '{selected_skill}'.")
            else:
                df_techs = pd.DataFrame(tech_data)
                
                st.subheader(f"Technicians with '{selected_skill}' Skill")

                df_chart = df_techs.head(10).set_index('TechnicianName')
                st.bar_chart(df_chart)

                with st.expander("Show Raw Data"):
                    st.dataframe(df_techs, use_container_width=True)