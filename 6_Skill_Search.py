import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL

# --- Page Configuration ---
st.set_page_config(page_title="Skill Search", page_icon="💡")
st.title("💡 Skill Search")

# --- Initialization ---
dal = AnalyticsDAL()

@st.cache_data(ttl=600)
def load_all_skills():
    """Loads a list of all discovered skills."""
    return dal.get_all_discovered_skills()

# --- UI ---
st.write("Select a skill to see which technicians have the most experience with it, based on ticket counts.")

all_skills = load_all_skills()

if not all_skills:
    st.warning("No skills have been discovered in the database yet.")
else:
    # Prepend a "None" option to the list of skills
    skill_options = ["-- Select a Skill --"] + all_skills
    
    selected_skill = st.selectbox(
        "Search for a skill:",
        options=skill_options
    )

    if selected_skill != "-- Select a Skill --":
        with st.spinner(f"Finding technicians with skill: {selected_skill}..."):
            tech_data = dal.get_technicians_by_skill(selected_skill)

            if not tech_data:
                st.info(f"No technicians are associated with the skill '{selected_skill}'.")
            else:
                df_techs = pd.DataFrame(tech_data)
                
                st.subheader(f"Technicians with '{selected_skill}' Skill")

                # Display a bar chart of the top 10
                df_chart = df_techs.head(10).set_index('TechnicianName')
                st.bar_chart(df_chart)

                # Display the raw data
                with st.expander("Show Raw Data"):
                    st.dataframe(df_techs, use_container_width=True)