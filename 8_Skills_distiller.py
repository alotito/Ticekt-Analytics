# pages/8_Skill_Distiller.py

import streamlit as st
import pandas as pd
import configparser
import json
import time
import math
import os
from datetime import datetime
from analytics_engine.dal_analytics import AnalyticsDAL
from analytics_engine.llm_interface import OllamaInterface
from analytics_engine.utils import parse_meta_analysis_output

# --- Page Configuration ---
st.set_page_config(page_title="Skill Distiller", page_icon="🧪")
st.title("🧪 Skill Distiller")

# --- Initialization ---
dal = AnalyticsDAL()
config = configparser.ConfigParser()
config.read('config.ini')
settings = config['skills_settings']

# --- Distiller Specific Settings ---
DISTILLER_MODEL = settings.get('DistillerModel', 'llama3:instruct')
DISTILLER_PROMPT_PATH = settings.get('DistillerPromptPath', 'prompts/skill_distiller_prompt.txt')
DISTILLER_BATCH_SIZE = settings.getint('DistillerBatchSize', 1000)
DISTILLER_DELAY = settings.getint('DistillerDelay', 1)

distiller_llm = OllamaInterface(model_name=DISTILLER_MODEL, prompt_template_path=DISTILLER_PROMPT_PATH)

# --- Session State Management ---
if "distiller_active_tab" not in st.session_state:
    st.session_state.distiller_active_tab = "AI-Powered Distillation"
if "distiller_to_modify" not in st.session_state:
    st.session_state.distiller_to_modify = "Add New"
if 'distiller_continuous_run_in_progress' not in st.session_state:
    st.session_state.distiller_continuous_run_in_progress = False
if 'distiller_failure_log_path' not in st.session_state:
    st.session_state.distiller_failure_log_path = None

# --- Data Loading & Caching ---
@st.cache_data(ttl=300)
def get_distilled_skills_df():
    return pd.DataFrame(dal.get_distilled_skills())

@st.cache_data(ttl=60)
def get_unassociated_managed_skills_df(count=20):
    # This function is not yet in the DAL, so we'll simulate it for now
    # by fetching managed skills and filtering. A dedicated DAL function would be better.
    all_managed = pd.DataFrame(dal.get_managed_skills())
    # Assuming the DAL function get_managed_skills returns 'DistilledSkillID'
    unassociated = all_managed[all_managed['DistilledSkillID'].isnull()]
    return unassociated.head(count)


def refresh_distiller_caches():
    get_distilled_skills_df.clear()
    get_unassociated_managed_skills_df.clear()

# --- UI Navigation ---
st.session_state.distiller_active_tab = st.radio(
    "Navigation",
    ["AI-Powered Distillation", "Manage Distilled Skills"],
    horizontal=True,
    label_visibility="collapsed"
)
st.markdown("---")


# =================================================================================
# VIEW 1: AI-POWERED DISTILLATION
# =================================================================================
if st.session_state.distiller_active_tab == "AI-Powered Distillation":
    st.subheader("🤖 AI-Powered Distillation")
    st.info("Use an LLM to automatically group `Managed Skills` into broader `Distilled Skill` categories.")

    def process_one_distillation_batch():
        result_info = { "status": "UNKNOWN", "llm_response": "", "error_detail": "" }
        managed_skills_batch = dal.get_unassociated_managed_skills_batch(DISTILLER_BATCH_SIZE)
        if not managed_skills_batch:
            result_info["status"] = "DONE"
            return result_info
        try:
            prompt_template = distiller_llm.prompt_template
            skills_json_string = json.dumps(managed_skills_batch)
            final_prompt = prompt_template.format(managed_skills_list=skills_json_string)
            
            llm_response = distiller_llm.get_skill_analysis(final_prompt)
            result_info["llm_response"] = llm_response

            parsed_data = parse_meta_analysis_output(llm_response)
            if not parsed_data:
                raise ValueError("LLM response could not be parsed into valid JSON.")

            sql_to_run = dal.generate_distillation_sql(parsed_data)
            if not sql_to_run:
                result_info["status"] = "NO_SQL_GENERATED"
                return result_info
            
            dal.execute_distillation_sql(sql_to_run)
            result_info["status"] = "SUCCESS"
        except Exception as e:
            result_info["status"] = "BATCH_ERROR"
            result_info["error_detail"] = str(e)
        return result_info

    def start_distiller_continuous_run():
        st.session_state.distiller_continuous_run_in_progress = True
        now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        st.session_state.distiller_failure_log_path = os.path.join(log_dir, f"DistillerBadBatches-{now}.log")

    def reset_distiller_page():
        st.session_state.distiller_continuous_run_in_progress = False
        st.session_state.distiller_failure_log_path = None
        st.cache_data.clear()

    if st.session_state.distiller_continuous_run_in_progress:
        total_unassociated = dal.get_unassociated_managed_skill_count()
        if total_unassociated == 0:
            st.success("No unassociated managed skills found to process!")
            st.button("✅ Done", on_click=reset_distiller_page)
        else:
            total_batches = math.ceil(total_unassociated / DISTILLER_BATCH_SIZE)
            progress_bar = st.progress(0, text=f"Preparing to process {total_batches} batches...")
            success_count, failure_count = 0, 0
            for i in range(total_batches):
                current_batch_num = i + 1
                progress_val = current_batch_num / total_batches
                progress_text = f"Processing batch {current_batch_num}/{total_batches}... (Success: {success_count}, Failed: {failure_count})"
                progress_bar.progress(progress_val, text=progress_text)
                result = process_one_distillation_batch()
                if result["status"] == "SUCCESS":
                    success_count += 1
                elif result["status"] == "DONE":
                    st.success("All unassociated managed skills have been processed.")
                    break
                else:
                    failure_count += 1
                    with open(st.session_state.distiller_failure_log_path, "a") as f:
                        f.write(f"--- FAILED BATCH #{current_batch_num} ---\n")
                        f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                        f.write(f"Status: {result['status']}\n")
                        f.write(f"Error Detail: {result.get('error_detail', 'N/A')}\n")
                        f.write("LLM Raw Response:\n")
                        f.write(result.get('llm_response', 'No response captured.') + "\n\n")
                time.sleep(DISTILLER_DELAY)
            st.success(f"Run finished. Succeeded: {success_count} | Failed: {failure_count}")
            if failure_count > 0:
                st.warning(f"Failed batches were logged to: `{st.session_state.distiller_failure_log_path}`")
            st.button("✅ Done", on_click=reset_distiller_page)
    else:
        unassociated_count = dal.get_unassociated_managed_skill_count()
        st.metric("Managed Skills Ready for Distillation", f"{unassociated_count:,}")
        st.button("Run Distillation Continuously", on_click=start_distiller_continuous_run, use_container_width=True, type="primary")


# =================================================================================
# VIEW 2: MANAGE DISTILLED SKILLS (CRUD)
# =================================================================================
elif st.session_state.distiller_active_tab == "Manage Distilled Skills":
    st.subheader("Add / Modify Distilled Skills")
    
    def handle_save_distilled_skill():
        skill_id = st.session_state.distiller_to_modify
        new_name = st.session_state.dist_skill_form_name
        new_desc = st.session_state.dist_skill_form_description
        if not new_name:
            st.warning("Distilled Skill Name cannot be empty.")
            return
        if skill_id == "Add New":
            dal.add_distilled_skill(new_name, new_desc)
            st.success(f"Added new distilled skill: {new_name}")
        else:
            dal.update_distilled_skill(skill_id, new_name, new_desc)
            st.success(f"Updated distilled skill: {new_name}")
        st.session_state.distiller_to_modify = "Add New"
        refresh_distiller_caches()

    col1, col2 = st.columns([1, 1])
    distilled_skills_df = get_distilled_skills_df()

    with col1:
        with st.form("distilled_skill_form"):
            st.write("**Add or Modify a Skill**")
            options_list = ["Add New"]
            if not distilled_skills_df.empty:
                options_list += distilled_skills_df['DistilledSkillID'].tolist()
            
            st.selectbox(
                "Select skill to modify", options=options_list,
                key="distiller_to_modify",
                format_func=lambda x: "Add New" if x == "Add New" else distilled_skills_df[distilled_skills_df['DistilledSkillID'] == x]['DistilledSkillName'].values[0]
            )

            current_name, current_desc = "", ""
            if st.session_state.distiller_to_modify != "Add New":
                skill_data = distilled_skills_df[distilled_skills_df['DistilledSkillID'] == st.session_state.distiller_to_modify].iloc[0]
                current_name = skill_data['DistilledSkillName']
                current_desc = skill_data['Description'] or ""
            
            st.text_input("Skill Name", value=current_name, key="dist_skill_form_name")
            st.text_area("Description", value=current_desc, key="dist_skill_form_description")
            st.form_submit_button("Save Skill", on_click=handle_save_distilled_skill)

    with col2:
        st.write("**Current Distilled Skills**")
        if not distilled_skills_df.empty:
            for _, row in distilled_skills_df.iterrows():
                sub_col1, sub_col2, sub_col3 = st.columns([4, 1, 1])
                sub_col1.markdown(f"**{row['DistilledSkillName']}**", help=row["Description"] if row["Description"] else "No description.")
                
                if sub_col2.button("✏️", key=f"mod_dist_{row['DistilledSkillID']}", help="Modify this skill"):
                    st.session_state.distiller_to_modify = row['DistilledSkillID']
                    st.rerun()

                if sub_col3.button("🗑️", key=f"del_dist_{row['DistilledSkillID']}", help="Delete this skill"):
                    dal.delete_distilled_skill(row['DistilledSkillID'])
                    st.success(f"Deleted skill: {row['DistilledSkillName']}")
                    refresh_distiller_caches()
                    st.rerun()
        else:
            st.info("No distilled skills found.")