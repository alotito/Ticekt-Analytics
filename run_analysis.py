import os
import time
import socket
import configparser
import random  # ADDED
import logging
import traceback
from datetime import datetime
from .dal_analytics import AnalyticsDAL
from .dal_cw import ConnectWiseDAL
from .llm_interface import OllamaInterface
from .utils import parse_llm_output

# --- ADDED: A list of names for the workers ---
WORKER_NAMES = [
    "GabbysHenchamn", "BertsMinion", "Galileo", "Curie", "Copernicus", "Kepler",
    "Faraday", "Maxwell", "Bohr", "Heisenberg", "Feynman", "Turing"
]

def setup_logger(worker_id: str):
    """Sets up a unique logger for this worker that writes to a file."""
    if not os.path.exists('logs'):
        os.makedirs('logs')
    logger = logging.getLogger(worker_id)
    logger.setLevel(logging.INFO)
    log_path = os.path.join('logs', f"{worker_id}.log")
    handler = logging.FileHandler(log_path, mode='w')
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)
    return logger

def main():
    # --- UPDATED: Generate a new random worker name ---
    worker_name = random.choice(WORKER_NAMES)
    worker_id = f"{worker_name}-{os.getpid()}"
    # --- END OF UPDATE ---
    
    logger = setup_logger(worker_id)

    try:
        config = configparser.ConfigParser()
        config.read('config.ini')
        settings = config['skills_settings']
        
        batch_size = int(settings['batch_size'])
        delay = int(settings['delay_between_batches_seconds'])

        logger.info(f"Worker started. Batch size: {batch_size}")

        analytics_dal = AnalyticsDAL()
        cw_dal = ConnectWiseDAL()
        llm = OllamaInterface(
            model_name=settings['model'],
            prompt_template_path=settings['prompt_path']
        )

        while True:
            time.sleep(random.uniform(0, 1))
            
            logger.info(f"Attempting to claim a batch of {batch_size} tickets...")
            ticket_batch = analytics_dal.claim_ticket_batch(worker_id, batch_size)

            if not ticket_batch:
                logger.info("No pending tickets found. Shutting down.")
                break
            
            logger.info(f"Claim successful. Processing {len(ticket_batch)} tickets.")
            for ticket_data in ticket_batch:
                ticket_pk = ticket_data['TicketID']
                source_ticket_number = ticket_data['SourceTicketNumber']
                
                try:
                    logger.info(f"Processing Ticket #{source_ticket_number}. Fetching from source DB...")
                    ticket_details = cw_dal.get_ticket_by_number(str(source_ticket_number))
                    if not ticket_details or not ticket_details.full_text:
                        raise ValueError("Ticket not found in source or has no text content.")

                    logger.info(f"Ticket #{source_ticket_number}. Sending to LLM...")
                    raw_output = llm.get_skill_analysis(ticket_details.full_text)
                    
                    logger.info(f"Ticket #{source_ticket_number}. Parsing LLM response...")
                    skills = parse_llm_output(raw_output)

                    if skills:
                        logger.info(f"Ticket #{source_ticket_number}. Saving {len(skills)} skills...")
                        analytics_dal.save_skills_for_ticket(ticket_pk, skills)
                    
                    analytics_dal.update_ticket_status(ticket_pk, status_id=2)
                    logger.info(f"Successfully processed Ticket #{source_ticket_number}.")

                except Exception as e:
                    error_trace = traceback.format_exc()
                    logger.error(f"FAILED to process Ticket #{source_ticket_number}. Error: {e}\n{error_trace}")
                    analytics_dal.update_ticket_status(ticket_pk, status_id=3)
            
            logger.info(f"Batch finished. Waiting for {delay} seconds.")
            time.sleep(delay)

    except Exception as e:
        error_trace = traceback.format_exc()
        logger.critical(f"CRITICAL ERROR in main loop: {e}\n{error_trace}")
        time.sleep(15)

if __name__ == "__main__":
    main()