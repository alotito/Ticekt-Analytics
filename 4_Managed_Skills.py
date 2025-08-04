import streamlit as st
import pandas as pd
from analytics_engine.dal_analytics import AnalyticsDAL

# --- Page Configuration ---
st.set_page_config(page_title="Skill Management", page_icon="🗂️")
st.title("🗂️ Skill Management")

# --- Initialization ---
dal = AnalyticsDAL()

# Initialize session state variables
if "active_tab" not in st.session_state:
    st.session_state.active_tab = "Manage Skills"
if "skill_to_modify" not in st.session_state:
    st.session_state.skill_to_modify = "Add New"

@st.cache_data(ttl=300)
def get_managed_skills_df():
    return pd.DataFrame(dal.get_managed_skills())

@st.cache_data(ttl=60)
def get_unassociated_skills_df():
    return pd.DataFrame(dal.get_top_unassociated_skills(count=10))

managed_skills_df = get_managed_skills_df()

def refresh_all_caches():
    get_managed_skills_df.clear()
    get_unassociated_skills_df.clear()

# --- CALLBACK FUNCTION FOR THE FORM ---
def handle_save_skill():
    skill_id = st.session_state.skill_to_modify
    new_name = st.session_state.skill_form_name
    new_desc = st.session_state.skill_form_description
    is_exception = st.session_state.skill_form_is_exception # ADDED

    if not new_name:
        st.warning("Skill Name cannot be empty.")
        return

    # UPDATED: Pass the is_exception flag to the DAL methods
    if skill_id == "Add New":
        dal.add_managed_skill(new_name, new_desc, is_exception)
        st.success(f"Added new skill: {new_name}")
    else:
        dal.update_managed_skill(skill_id, new_name, new_desc, is_exception)
        st.success(f"Updated skill: {new_name}")
    
    st.session_state.skill_to_modify = "Add New"
    refresh_all_caches()

# --- UI NAVIGATION ---
st.session_state.active_tab = st.radio(
    "Navigation",
    ["Manage Skills", "Associate Discovered Skills", "Visualize Skills"],
    horizontal=True,
    label_visibility="collapsed",
    key="navigation_radio"
)
st.markdown("---")

# =================================================================================
# VIEW 1: MANAGE SKILLS (CRUD)
# =================================================================================
if st.session_state.active_tab == "Manage Skills":
    st.subheader("Add / Modify / Delete Managed Skills")
    col1, col2 = st.columns([1, 1])

    with col1:
        with st.form("skill_form"):
            st.write("**Add or Modify a Skill**")
            options_list = ["Add New"]
            if not managed_skills_df.empty:
                options_list += managed_skills_df['ManagedSkillID'].tolist()
            
            st.selectbox(
                "Select skill to modify", options=options_list,
                key="skill_to_modify",
                format_func=lambda x: "Add New" if x == "Add New" else managed_skills_df[managed_skills_df['ManagedSkillID'] == x]['ManagedSkillName'].values[0]
            )

            current_name, current_desc, current_is_exception = "", "", False
            if st.session_state.skill_to_modify != "Add New":
                skill_data = managed_skills_df[managed_skills_df['ManagedSkillID'] == st.session_state.skill_to_modify].iloc[0]
                current_name, current_desc = skill_data['ManagedSkillName'], skill_data['Description']
                current_is_exception = skill_data['IsException']
            
            st.text_input("Skill Name", value=current_name, key="skill_form_name")
            st.text_area("Description", value=current_desc, key="skill_form_description")
            # ADDED: Checkbox for the IsException flag
            st.checkbox("Mark as Exception", value=current_is_exception, key="skill_form_is_exception")

            st.form_submit_button("Save Skill", on_click=handle_save_skill)

    with col2:
        st.write("**Current Managed Skills**")
        if not managed_skills_df.empty:
            for _, row in managed_skills_df.iterrows():
                sub_col1, sub_col2, sub_col3 = st.columns([4, 1, 1])
                
                # UPDATED: Add a warning icon if the skill is an exception
                skill_display_name = f"⚠️ {row['ManagedSkillName']}" if row['IsException'] else row["ManagedSkillName"]
                sub_col1.markdown(skill_display_name, help=row["Description"] if row["Description"] else "No description.")
                
                if sub_col2.button("✏️", key=f"mod_{row['ManagedSkillID']}", help="Modify this skill"):
                    st.session_state.skill_to_modify = row['ManagedSkillID']
                    st.rerun()

                if sub_col3.button("🗑️", key=f"del_{row['ManagedSkillID']}", help="Delete this skill"):
                    dal.delete_managed_skill(row['ManagedSkillID'])
                    st.success(f"Deleted skill: {row['ManagedSkillName']}")
                    refresh_all_caches()
                    st.rerun()
        else:
            st.info("No managed skills found.")

# =================================================================================
# VIEW 2: ASSOCIATE DISCOVERED SKILLS
# =================================================================================
elif st.session_state.active_tab == "Associate Discovered Skills":
    st.subheader("Associate Discovered Skills")
    st.info("Assign a managed skill category to the top skills discovered by the LLM.")
    if managed_skills_df.empty:
        st.warning("Please add at least one Managed Skill in the 'Manage Skills' tab first.")
    else:
        df_unassociated = get_unassociated_skills_df()
        
        skill_filter = st.text_input("Filter discovered skills by name:", placeholder="e.g., email")
        if skill_filter:
            df_unassociated = df_unassociated[df_unassociated['DiscoveredSkillName'].str.contains(skill_filter, case=False, na=False)]
        
        if not df_unassociated.empty:
            df_unassociated['ManagedCategory'] = pd.Series(dtype='object')
            with st.form("association_form"):
                st.write("**Top Unassociated Skills**")
                # Filter out skills marked as exceptions from the dropdown
                non_exception_skills = managed_skills_df[managed_skills_df['IsException'] == False]
                
                edited_df = st.data_editor(
                    df_unassociated,
                    column_config={
                        "DiscoveredSkillID": None,
                        "DiscoveredSkillName": st.column_config.TextColumn("Discovered Skill", disabled=True),
                        "Frequency": st.column_config.NumberColumn("Occurrences", disabled=True),
                        "ManagedCategory": st.column_config.SelectboxColumn(
                            "Assign to Managed Skill",
                            options=sorted(list(non_exception_skills['ManagedSkillName'])),
                            required=False,
                        )
                    },
                    hide_index=True, use_container_width=True
                )
                if st.form_submit_button("Save Associations"):
                    save_count = 0
                    managed_skills_map = {row['ManagedSkillName']: row['ManagedSkillID'] for _, row in managed_skills_df.iterrows()}
                    for _, row in edited_df.iterrows():
                        if row.get('ManagedCategory') and pd.notna(row.get('ManagedCategory')):
                            managed_id = managed_skills_map[row['ManagedCategory']]
                            dal.associate_skill(row['DiscoveredSkillID'], managed_id)
                            save_count += 1
                    if save_count > 0:
                        st.success(f"Successfully saved {save_count} association(s).")
                        refresh_all_caches()
                        st.rerun()
                    else:
                        st.info("No changes were made.")
        else:
            st.success("All discovered skills have been associated!")

# =================================================================================
# VIEW 3: VISUALIZE SKILLS
# =================================================================================
elif st.session_state.active_tab == "Visualize Skills":
    st.subheader("Managed Skills Visualization")
    st.info("This chart shows the total occurrences of all discovered skills grouped under each managed skill category.")
    if st.button("Refresh Visualization"):
        refresh_all_caches()
        st.rerun()
    heatmap_data = dal.get_managed_skill_occurrences()
    if heatmap_data:
        df_heatmap = pd.DataFrame(heatmap_data)
        df_chart = df_heatmap.set_index('ManagedSkillName')
        st.bar_chart(df_chart, height=500)
    else:
        st.warning("No data available for visualization.")