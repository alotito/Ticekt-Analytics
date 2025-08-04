# pages/7_Skills_Meta_Analysis.py

import streamlit as st
import configparser
import time
import pandas as pd
import json
import os
from datetime import datetime
import math
from analytics_engine.dal_analytics import AnalyticsDAL
from analytics_engine.llm_interface import OllamaInterface
from analytics_engine.utils import parse_meta_analysis_output

# --- Page Configuration ---
st.set_page_config(page_title="Skills Meta Analysis", page_icon="🤖")
st.title("🤖 Skills Meta Analysis")

# --- Initialization ---
dal = AnalyticsDAL()
config = configparser.ConfigParser()
config.read('config.ini')
settings = config['skills_settings']
BATCH_SIZE = settings.getint('MetaAnalysisBatchSize', 200)
MODEL = settings.get('MetaAnalysisModel', 'llama3:instruct')
PROMPT_PATH = settings.get('MetaAnalysisPromptPath', 'prompts/skills_meta_analysis_prompt.txt')
DELAY = settings.getint('MetaAnalysisDelay', 2)
llm = OllamaInterface(model_name=MODEL, prompt_template_path=PROMPT_PATH)

# --- Session State Management ---
if 'analysis_results' not in st.session_state:
    st.session_state.analysis_results = None
if 'auto_run_in_progress' not in st.session_state:
    st.session_state.auto_run_in_progress = False
if 'continuous_run_in_progress' not in st.session_state:
    st.session_state.continuous_run_in_progress = False
if 'batches_to_run' not in st.session_state:
    st.session_state.batches_to_run = 0
if 'failure_log_path' not in st.session_state:
    st.session_state.failure_log_path = None

# --- Data Fetching ---
@st.cache_data(ttl=10)
def get_metric_counts():
    unassociated = dal.get_unassociated_skill_count()
    associated = dal.get_associated_skill_count()
    managed = dal.get_managed_skill_count()
    return unassociated, associated, managed

# --- Core Processing & Callback Functions ---
def process_one_batch():
    """Processes one batch and commits changes if successful."""
    result_info = { "status": "UNKNOWN", "llm_response": "", "error_detail": "" }
    skills_batch = dal.get_unassociated_skills_batch(BATCH_SIZE)
    if not skills_batch:
        result_info["status"] = "DONE"
        return result_info

    try:
        prompt_template = llm.prompt_template
        skills_json_string = json.dumps(skills_batch)
        final_prompt = prompt_template.format(skills_list=skills_json_string)
        
        llm_response = llm.get_skill_analysis(final_prompt)
        result_info["llm_response"] = llm_response

        parsed_data = parse_meta_analysis_output(llm_response)
        if not parsed_data:
            raise ValueError("LLM response could not be parsed into a valid JSON array.")

        sql_to_run = dal.generate_meta_analysis_sql(parsed_data)
        if not sql_to_run:
            result_info["status"] = "NO_SQL_GENERATED"
            return result_info
            
        dal.execute_meta_analysis_sql(sql_to_run)
        result_info["status"] = "SUCCESS"

    except Exception as e:
        result_info["status"] = "BATCH_ERROR"
        result_info["error_detail"] = str(e)

    return result_info

def start_auto_run():
    st.session_state.batches_to_run = st.session_state.batch_count_input
    st.session_state.auto_run_in_progress = True
    st.session_state.continuous_run_in_progress = False

def start_continuous_run():
    st.session_state.continuous_run_in_progress = True
    st.session_state.auto_run_in_progress = False
    now = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    st.session_state.failure_log_path = os.path.join(log_dir, f"BadBatches-{now}.log")

def reset_to_main_page():
    st.session_state.auto_run_in_progress = False
    st.session_state.continuous_run_in_progress = False
    st.session_state.analysis_results = None
    st.session_state.failure_log_path = None
    st.cache_data.clear()

# --- UI ---
st.write("This tool uses an LLM to automatically group and normalize unassociated discovered skills.")
st.divider()

# --- UI STATE 4: CONTINUOUS RUN ---
if st.session_state.continuous_run_in_progress:
    st.subheader("Continuous Automated Run in Progress...")
    
    total_unassociated = dal.get_unassociated_skill_count()
    if total_unassociated == 0:
        st.success("No unassociated skills found to process!")
        st.button("✅ Done", on_click=reset_to_main_page)
    else:
        total_batches = math.ceil(total_unassociated / BATCH_SIZE)
        progress_bar = st.progress(0, text=f"Preparing to process {total_batches} batches...")
        
        success_count = 0
        failure_count = 0
        
        for i in range(total_batches):
            current_batch_num = i + 1
            progress_val = current_batch_num / total_batches
            progress_text = f"Processing batch {current_batch_num} of {total_batches}... (Success: {success_count}, Failed: {failure_count})"
            progress_bar.progress(progress_val, text=progress_text)
            
            result = process_one_batch()

            if result["status"] == "SUCCESS":
                success_count += 1
            elif result["status"] == "DONE":
                st.success("All unassociated skills have been processed.")
                break
            else: # Any failure state
                failure_count += 1
                with open(st.session_state.failure_log_path, "a") as f:
                    f.write(f"--- FAILED BATCH #{current_batch_num} ---\n")
                    f.write(f"Timestamp: {datetime.now().isoformat()}\n")
                    f.write(f"Status: {result['status']}\n")
                    f.write(f"Error Detail: {result.get('error_detail', 'N/A')}\n")
                    f.write("LLM Raw Response:\n")
                    f.write(result.get('llm_response', 'No response captured.') + "\n\n")

            time.sleep(DELAY)

        final_message = f"Continuous run finished. Succeeded: {success_count} | Failed/Skipped: {failure_count}"
        st.success(final_message)
        if failure_count > 0:
            st.warning(f"Failed batches were logged to: `{st.session_state.failure_log_path}`")
        
        st.button("✅ Done - Return to Main Page", on_click=reset_to_main_page)

# --- UI STATE 3: AUTOMATED RUN IN PROGRESS ---
elif st.session_state.auto_run_in_progress:
    st.subheader("Automated Run in Progress...")
    progress_bar = st.progress(0, text="Starting auto-analysis...")
    
    batch_results = []
    
    for i in range(st.session_state.batches_to_run):
        progress_text = f"Processing batch {i + 1} of {st.session_state.batches_to_run}..."
        progress_bar.progress((i + 1) / st.session_state.batches_to_run, text=progress_text)
        
        result = process_one_batch()
        result['batch_number'] = i + 1
        batch_results.append(result)

        if result["status"] == "DONE":
            break 
        time.sleep(DELAY)

    success_count = sum(1 for r in batch_results if r['status'] == 'SUCCESS')
    failure_count = len(batch_results) - success_count
    st.success(f"Auto-analysis finished. Succeeded: {success_count} | Failed/Skipped: {failure_count}")

    failed_batches = [r for r in batch_results if r['status'] not in ["SUCCESS", "DONE", "NO_SQL_GENERATED"]]
    
    if failed_batches:
        with st.expander("🔬 Failed Batch Log", expanded=True):
            st.warning(f"Displaying details for {len(failed_batches)} failed batch(es).")
            for result in failed_batches:
                st.markdown(f"--- **Batch {result['batch_number']}** ---")
                st.write(f"**Status**: {result['status']}")
                st.code(result.get("llm_response", "No response from LLM."), language="json")
    else:
        st.info("✅ No batches failed during the run.")

    st.button("✅ Done - Return to Main Page", on_click=reset_to_main_page)

# --- UI STATE 2: MANUAL REVIEW ---
elif st.session_state.analysis_results:
    st.subheader("Step 2: Review and Confirm Analysis")
    st.info("Review the LLM's proposed skill groups below. Click 'Confirm and Save' to commit these changes to the database.")
    st.write("**Parsed LLM Response:**")
    st.json(st.session_state.analysis_results, expanded=True)
    if st.button("✅ Confirm and Save", type="primary"):
        with st.spinner("Saving changes..."):
            sql_to_run = dal.generate_meta_analysis_sql(st.session_state.analysis_results)
            if sql_to_run:
                dal.execute_meta_analysis_sql(sql_to_run)
                st.success("Analysis saved successfully!")
                time.sleep(1)
                reset_to_main_page()
                st.rerun()
            else:
                st.warning("Analysis did not result in any SQL commands to execute.")
    if st.button("Discard Batch"):
        st.session_state.analysis_results = None
        st.rerun()

# --- UI STATE 1: MAIN PAGE ---
else:
    st.subheader("Current State")
    unassociated_count, associated_count, managed_count = get_metric_counts()
    c1, c2, c3 = st.columns(3)
    c1.metric("Unassociated Skills", f"{unassociated_count:,}")
    c2.metric("Associated Skills", f"{associated_count:,}")
    c3.metric("Total Managed Skills", f"{managed_count:,}")
    st.divider()

    st.subheader("Processing Options")
    col1, col2, col3 = st.columns(3, gap="large")

    with col1:
        st.markdown("##### Manual Mode")
        st.write("Analyze a single batch and manually review the LLM's output before saving.")
        if st.button("Analyze One Batch for Review", use_container_width=True):
            with st.spinner("LLM is analyzing the skill batch..."):
                skills_batch = dal.get_unassociated_skills_batch(BATCH_SIZE)
                if not skills_batch:
                    st.success("No more unassociated skills to process!")
                else:
                    prompt_template = llm.prompt_template
                    skills_json_string = json.dumps(skills_batch)
                    final_prompt = prompt_template.format(skills_list=skills_json_string)
                    llm_response = llm.get_skill_analysis(final_prompt)
                    parsed_data = parse_meta_analysis_output(llm_response)
                    if parsed_data:
                        st.session_state.analysis_results = parsed_data
                        st.rerun()
                    else:
                        st.error("Failed to parse a valid response from the LLM.")
                        st.json(llm_response)
    
    with col2:
        st.markdown("##### Fixed Batch Mode")
        st.write("Run analysis automatically for a set number of batches with no manual review.")
        st.number_input(
            "Number of batches to run:",
            min_value=1, max_value=500, value=5, step=1,
            key="batch_count_input"
        )
        st.button("Run Set Number of Batches", on_click=start_auto_run, use_container_width=True)

    with col3:
        st.markdown("##### Continuous Mode")
        st.write("Continuously run analysis until all skills are processed. Failed batches will be logged.")
        st.button("Run Continuously Until Done", on_click=start_continuous_run, use_container_width=True)

    st.divider()