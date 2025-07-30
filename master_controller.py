import time
import subprocess
import configparser
import sys
from datetime import datetime
from populate_tickets import PopulationController
from analytics_engine.dal_analytics import AnalyticsDAL

def log(message: str):
    """Prints a message with a timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} | {message}")

def main():
    """
    Main controller loop to manage the entire ETL process automatically.
    """
    log("--- Master Controller Started ---")
    config = configparser.ConfigParser()
    config.read('config.ini')
    worker_count = int(config['skills_settings']['worker_count'])
    
    analytics_dal = AnalyticsDAL()

    log("Running startup cleanup to reset any stuck tickets...")
    reset_count = analytics_dal.reset_stuck_tickets()
    log(f"Cleanup complete. Reset {reset_count} stuck tickets to 'Pending'.")

    population_controller = PopulationController()
    
    processes = []

    try:
        while True:
            log("--- Starting New Cycle ---")
            
            log("Step 1: Checking for new tickets from source...")
            population_controller.run_population()
            
            pending_count = analytics_dal.get_pending_ticket_count()
            
            if pending_count > 0:
                log(f"Step 2: {pending_count} tickets are pending. Starting {worker_count} workers...")
                
                processes.clear()
                command = [sys.executable, "-m", "analytics_engine.run_analysis"]
                
                for i in range(worker_count):
                    proc = subprocess.Popen(
                        command, 
                        stdout=subprocess.PIPE, 
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    processes.append(proc)
                
                log("Waiting for all workers to complete...")

                # --- CORRECTED LOGIC: Wait for all processes concurrently ---
                # This collects output from each worker as it finishes.
                worker_outputs = [p.communicate() for p in processes]
                
                log("All workers have finished. Consolidating output...")

                # Now, loop through the collected results and print them.
                for i, (stdout, stderr) in enumerate(worker_outputs):
                    log(f"--- Output from Worker {i+1} (PID: {processes[i].pid}) ---")
                    if stdout:
                        print("[STDOUT]:")
                        print(stdout.strip())
                    if stderr:
                        print("[STDERR - ERROR!]:")
                        print(stderr.strip())
                    log(f"--- End of Output for Worker {i+1} ---")
                # --- END OF CORRECTION ---
                
                log("All tasks for this cycle are complete. Restarting...")
            
            else:
                sleep_duration_hr = 1
                log(f"No pending tickets found. Sleeping for {sleep_duration_hr} hour...")
                time.sleep(sleep_duration_hr * 3600)
    
    except KeyboardInterrupt:
        log("\n--- Ctrl+C detected. Initiating graceful shutdown. ---")
        
        if processes:
            log(f"Terminating {len(processes)} active worker processes...")
            for p in processes:
                p.terminate()
            log("Worker processes terminated.")

        log("Resetting any stuck tickets in the queue...")
        reset_count = analytics_dal.reset_stuck_tickets()
        log(f"Cleanup complete. Reset {reset_count} tickets.")
        log("--- Shutdown complete. ---")

if __name__ == "__main__":
    main()