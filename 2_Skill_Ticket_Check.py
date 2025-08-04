# pages/2_Skill_Ticket_Check.py
import streamlit as st
import configparser
import asyncio
import json
import pytds
from typing import List, Optional

# The user has provided the full content of these files, so they are cited.
from analytics_engine.dal_cw import ConnectWiseDAL #
from analytics_engine.llm_interface import OllamaInterface #
from analytics_engine.utils import parse_llm_output

# In pages/2_Skill_Ticket_Check.py

def fetch_and_analyze_ticket(ticket_number, config_path):
    """
    Contains the blocking I/O calls (database, LLM).
    """
    dal = ConnectWiseDAL(config_path)
    ticket = dal.get_ticket_by_number(ticket_number)
    
    if not ticket:
        return None, "Ticket not found.", []

    config = configparser.ConfigParser()
    config.read(config_path)
    skill_config = config['skills_settings']
    
    llm = OllamaInterface(
        model_name=skill_config['model'],
        prompt_template_path=skill_config['prompt_path']
    )

    # --- THIS IS THE FIX ---
    # We now format the prompt on the page before sending it to the LLM.
    prompt_template = llm.prompt_template
    final_prompt = prompt_template.format(ticket_text=ticket.full_text)
    raw_output = llm.get_skill_analysis(final_prompt)
    # --- END OF FIX ---
    
    parsed_skills = parse_llm_output(raw_output)
    
    return ticket, raw_output, parsed_skills

async def main():
    st.set_page_config(page_title="Skill Ticket Check", page_icon="🔎")
    st.title("🔎 Skill Ticket Check")
    
    ticket_number = st.text_input("Enter ConnectWise Ticket Number:", placeholder="e.g., 2249790")
    analyze_button = st.button("Analyze Ticket")

    if analyze_button and ticket_number:
        with st.spinner(f"Processing Ticket #{ticket_number}..."):
            try:
                loop = asyncio.get_running_loop()
                ticket, raw_llm_output, skills = await loop.run_in_executor(
                    None, fetch_and_analyze_ticket, ticket_number, 'config.ini'
                )

                if ticket:
                    st.success(f"Successfully fetched Ticket #{ticket.ticket_id}")
                    st.subheader("Analysis Results")
                    st.text_input("Technician:", value=ticket.technician_name, disabled=True)
                    
                    st.write("**Parsed Skills:**")
                    if skills:
                        st.write(" ".join(f"`{skill}`" for skill in skills))
                    else:
                        st.warning("No skills were successfully parsed from the output.")

                    with st.expander("Show Raw LLM Output"):
                        st.code(raw_llm_output, language="json")
                else:
                    st.error(f"Could not find a ticket with number: {ticket_number}")

            except Exception as e:
                st.error(f"An error occurred during the process: {e}")

if __name__ == "__main__":
    asyncio.run(main())