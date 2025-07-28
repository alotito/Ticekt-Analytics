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

def parse_llm_output(raw_output: str) -> List[str]:
    """
    Tries to parse the JSON string from the LLM.
    Handles malformed JSON and missing keys gracefully.
    """
    try:
        # The model sometimes wraps its JSON in markdown. Let's remove it.
        clean_output = raw_output.strip().replace("```json", "").replace("```", "").strip()
        if not clean_output:
            return []
            
        data = json.loads(clean_output)
        
        # Safely get the 'skills' key. Returns an empty list if the key is missing.
        skills = data.get('skills', [])
        
        return skills if isinstance(skills, list) else []
            
    except json.JSONDecodeError:
        print(f"Warning: Failed to decode JSON from LLM output: {raw_output}")
        return []
    except Exception as e:
        print(f"An unexpected error occurred during parsing: {e}")
        return []


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
    raw_output = llm.get_skill_analysis(ticket.full_text)
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