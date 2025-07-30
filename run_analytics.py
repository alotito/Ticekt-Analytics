# analytics_engine/run_analysis.py
import os
import time
import socket
import configparser

# The user has provided the full content of these files, so they are cited.
from .dal_analytics import AnalyticsDAL #
from .dal_cw import ConnectWiseDAL #
from .llm_interface import OllamaInterface #
from pages.a2_Skill_Ticket_Check import parse_llm_output #

def main():
    # --- Setup ---
    config = configparser.ConfigParser()
    config.read('config.ini')
    settings = config['skills_settings']
    
    worker_id = f"{socket.gethostname()}-{os.getpid()}"
    batch_size = int(settings['batch_size'])
    delay = int(settings['delay_between_batches_seconds'])

    print(f"✅ Worker {worker_id} started. Batch size: {batch_size}")

    # Instantiate all necessary data and interface layers
    analytics_dal = AnalyticsDAL()
    cw_dal = ConnectWiseDAL()
    llm = OllamaInterface(
        model_name=settings['model'],
        prompt_template_path=settings['prompt_path']
    )

    # --- Main Loop ---
    while True:
        try:
            print(f"Worker {worker_id}: Attempting to claim a batch of {batch_size} tickets...")
            ticket_batch = analytics_dal.claim_ticket_batch(worker_id, batch_size)

            if not ticket_batch:
                print(f"Worker {worker_id}: No pending tickets found. Shutting down.")
                break

            print(f"Worker {worker_id}: Successfully claimed {len(ticket_batch)} tickets.")

            # Process the claimed batch
            for ticket_data in ticket_batch:
                ticket_pk = ticket_data['TicketID']
                source_ticket_number = ticket_data['SourceTicketNumber']

                try:
                    print(f"Worker {worker_id}: Processing Ticket #{source_ticket_number} (PK: {ticket_pk})...")
                    
                    # 1. Fetch full ticket details from the source ConnectWise database
                    ticket_details = cw_dal.get_ticket_by_number(source_ticket_number)
                    if not ticket_details or not ticket_details.full_text:
                        raise ValueError("Ticket not found in source or has no text content.")

                    # 2. Get skill analysis from LLM
                    raw_output = llm.get_skill_analysis(ticket_details.full_text)
                    skills = parse_llm_output(raw_output)

                    # 3. Save the extracted skills to the analytics database
                    if skills:
                        analytics_dal.save_skills_for_ticket(ticket_pk, skills)
                    
                    # 4. Mark ticket as complete
                    analytics_dal.update_ticket_status(ticket_pk, status_id=2) # 2 = Complete
                    print(f"   -> Worker {worker_id}: Successfully processed Ticket #{source_ticket_number}.")

                except Exception as e:
                    print(f"   !! Worker {worker_id}: FAILED to process Ticket #{source_ticket_number}. Error: {e}")
                    analytics_dal.update_ticket_status(ticket_pk, status_id=3) # 3 = Error
            
            print(f"Worker {worker_id}: Batch finished. Waiting for {delay} seconds.")
            time.sleep(delay)

        except Exception as e:
            print(f"🚨 Worker {worker_id}: A critical error occurred in the main loop: {e}")
            time.sleep(15) # Wait before retrying to prevent fast failure loops

if __name__ == "__main__":
    main()