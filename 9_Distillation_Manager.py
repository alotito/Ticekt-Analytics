# pages/9_Distillation_Manager.py

import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL
import time

# --- Page Configuration ---
st.set_page_config(page_title="Distillation Manager", page_icon="🗂️", layout="wide")
st.title("🗂️ Distillation Manager")
st.info("Click on an action button (Edit, Merge, Delete) next to a skill to manage it in the sidebar.")


# --- Initialization & Data Loading ---
dal = AnalyticsDAL()

@st.cache_data(ttl=60)
def load_distilled_skills():
    """Fetches all distilled skills from the database."""
    return pd.DataFrame(dal.get_distilled_skills())

# --- Session State ---
if "action" not in st.session_state:
    st.session_state.action = None
if "skill_id" not in st.session_state:
    st.session_state.skill_id = None

# --- Main UI ---
distilled_skills_df = load_distilled_skills()

# --- Skill List Display ---
st.subheader("All Distilled Skills")
filter_text = st.text_input("Search skills by name...", placeholder="Type to filter...")

if filter_text:
    filtered_df = distilled_skills_df[distilled_skills_df['DistilledSkillName'].str.contains(filter_text, case=False, na=False)]
else:
    filtered_df = distilled_skills_df

if filtered_df.empty:
    st.warning("No skills match your search.")
else:
    for _, row in filtered_df.iterrows():
        with st.expander(f"**{row['DistilledSkillName']}**"):
            if row['Description']:
                st.caption(f"Description: {row['Description']}")
            else:
                st.caption("No description provided.")

            st.markdown("---")
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("✏️ Edit", key=f"edit_{row['DistilledSkillID']}", use_container_width=True):
                    st.session_state.action = "edit"
                    st.session_state.skill_id = row['DistilledSkillID']
                    st.rerun()
            
            with col2:
                if st.button("🔄 Merge", key=f"merge_{row['DistilledSkillID']}", use_container_width=True):
                    st.session_state.action = "merge"
                    st.session_state.skill_id = row['DistilledSkillID']
                    st.rerun()
            
            with col3:
                if st.button("🗑️ Delete", key=f"del_{row['DistilledSkillID']}", use_container_width=True, type="secondary"):
                    st.session_state.action = "delete"
                    st.session_state.skill_id = row['DistilledSkillID']
                    st.rerun()


# --- Sidebar Actions ---
st.sidebar.header("Skill Actions")

if not st.session_state.action:
    st.sidebar.info("Select an action (Edit, Merge, Delete) from the main list.")

if st.session_state.action and st.session_state.skill_id:
    try:
        selected_skill = distilled_skills_df[distilled_skills_df['DistilledSkillID'] == st.session_state.skill_id].iloc[0]

        if st.session_state.action == "edit":
            st.sidebar.subheader(f"Edit Skill")
            st.sidebar.info(f"**Skill:** {selected_skill['DistilledSkillName']}")
            with st.sidebar.form("edit_form"):
                new_name = st.text_input("New Name", value=selected_skill['DistilledSkillName'])
                new_desc = st.text_area("New Description", value=selected_skill['Description'] or "")
                
                submitted = st.form_submit_button("Save Changes", type="primary")
                if submitted:
                    dal.update_distilled_skill(st.session_state.skill_id, new_name, new_desc)
                    st.toast(f"Updated '{new_name}'.")
                    st.session_state.action = None
                    st.session_state.skill_id = None
                    st.cache_data.clear()
                    st.rerun()

        elif st.session_state.action == "merge":
            st.sidebar.subheader(f"Merge Skill")
            st.sidebar.info(f"**Source:** {selected_skill['DistilledSkillName']}")
            st.sidebar.warning("All child skills will be moved to the target, and the source skill will be deleted.")
            
            with st.sidebar.form("merge_form"):
                merge_target_options = distilled_skills_df[distilled_skills_df['DistilledSkillID'] != st.session_state.skill_id]
                target_id = st.selectbox(
                    "Select Target Skill:",
                    options=merge_target_options['DistilledSkillID'],
                    format_func=lambda x: merge_target_options[merge_target_options['DistilledSkillID'] == x]['DistilledSkillName'].iloc[0]
                )
                submitted = st.form_submit_button("Confirm Merge", type="primary")
                if submitted:
                    dal.merge_distilled_skill(st.session_state.skill_id, target_id)
                    st.toast(f"Merged skill successfully.")
                    st.session_state.action = None
                    st.session_state.skill_id = None
                    st.cache_data.clear()
                    st.rerun()

        elif st.session_state.action == "delete":
            st.sidebar.subheader(f"Delete Skill")
            st.sidebar.warning(f"Delete **{selected_skill['DistilledSkillName']}**?")
            st.sidebar.caption("This cannot be undone. Child skills will be un-parented.")
            
            if st.sidebar.button("Yes, permanently delete", type="primary"):
                dal.delete_distilled_skill(st.session_state.skill_id)
                st.toast(f"Deleted {selected_skill['DistilledSkillName']}.")
                st.session_state.action = None
                st.session_state.skill_id = None
                st.cache_data.clear()
                st.rerun()

        if st.sidebar.button("Cancel"):
            st.session_state.action = None
            st.session_state.skill_id = None
            st.rerun()

    except (IndexError, KeyError):
        st.sidebar.error("Selected skill not found. Resetting.")
        st.session_state.action = None
        st.session_state.skill_id = None
        st.rerun()