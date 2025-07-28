# analytics_engine/run_analysis.py (Database Checkpointing Version)
import pyodbc
import configparser
import base64
import json
import os
from datetime import datetime
from typing import Optional, List

from .dal_cw import ConnectWiseDAL #
from .llm_interface import OllamaInterface #
from .models import StandardTicket #

class AnalysisRunner:
    def __init__(self, config_path: str = 'config.ini'): #
        config = configparser.ConfigParser()
        config.read(config_path)
        skill_config = config['skills_settings']
        self.dal = ConnectWiseDAL(config_path)
        self.llm = OllamaInterface(
            model_name=skill_config['model'],
            prompt_template_path=skill_config['prompt_path']
        )
        self.batch_size = skill_config.getint('batch_size')
        self.max_token_threshold = skill_config.getint('max_token_threshold')
        db_config = config['analytics_db']
        password = base64.b64decode(db_config['password_b64']).decode('utf-8')
        self.dest_conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={db_config['server']};DATABASE={db_config['database']};"
            f"UID={db_config['user']};PWD={{{password}}};TrustServerCertificate=yes;"
        )

    def _get_last_processed_id(self, cursor: pyodbc.Cursor) -> int:
        """Reads the last processed ticket ID from the database."""
        cursor.execute("SELECT TOP 1 LastProcessedTicketID FROM dbo.ProcessingCheckpoint;")
        result = cursor.fetchone()
        return result[0] if result else 0

    def _update_last_processed_id(self, cursor: pyodbc.Cursor, ticket_id: int):
        """Updates the last processed ticket ID in the database."""
        cursor.execute("UPDATE dbo.ProcessingCheckpoint SET LastProcessedTicketID = ?;", ticket_id)

    def _get_or_create_id(self, cursor: pyodbc.Cursor, table_name: str, column_name: str, value: str) -> Optional[int]:
        if not value or not value.strip(): return None
        cursor.execute(f"SELECT {table_name[:-1]}ID FROM dbo.{table_name} WHERE {column_name} = ?", value)
        row = cursor.fetchone()
        if row: return row[0]
        sql = f"INSERT INTO dbo.{table_name} ({column_name}) OUTPUT INSERTED.{table_name[:-1]}ID VALUES (?);"
        new_id = cursor.execute(sql, value).fetchone()[0]
        return new_id

    def _parse_llm_output(self, raw_output: str) -> List[str]:
        try:
            clean_output = raw_output.strip().replace("```json", "").replace("```", "").strip()
            if not clean_output: return []
            data = json.loads(clean_output)
            skills = data.get('skills', [])
            return skills if isinstance(skills, list) else []
        except: return []

    def run(self):
        start_time = datetime.now()
        cnxn_dest = None
        run_id = None
        total_tickets_processed = 0
        try:
            cnxn_dest = pyodbc.connect(self.dest_conn_str, autocommit=False)
            cursor = cnxn_dest.cursor()
            cursor.execute("INSERT INTO dbo.AnalysisRuns (RunStartTime, Status) VALUES (?, 'Running');", start_time)
            run_id = cursor.execute("SELECT SCOPE_IDENTITY();").fetchone()[0]
            cnxn_dest.commit()
            
            last_id = self._get_last_processed_id(cursor)

            while True:
                if os.path.exists('stop.txt'):
                    print("\nStop file detected. Shutting down gracefully...")
                    os.remove('stop.txt')
                    end_time = datetime.now()
                    cursor.execute("UPDATE dbo.AnalysisRuns SET RunEndTime = ?, Status = 'Stopped by User', TicketsProcessed = ? WHERE RunID = ?;",
                                   end_time, total_tickets_processed, run_id)
                    cnxn_dest.commit()
                    break

                print(f"\nFetching next batch of {self.batch_size} tickets after ID {last_id}...")
                ticket_batch = self.dal.get_ticket_batch(last_id, self.batch_size)

                if not ticket_batch:
                    print("No more tickets found to process.")
                    break

                for ticket in ticket_batch:
                    try:
                        print(f"  -> Processing Ticket #{ticket.ticket_id}...")
                        # ... (Ticket processing logic) ...
                        raw_output = self.llm.get_skill_analysis(ticket.full_text)
                        discovered_skills = self._parse_llm_output(raw_output)
                        if not discovered_skills:
                            self._update_last_processed_id(cursor, ticket.ticket_id)
                            cnxn_dest.commit()
                            last_id = ticket.ticket_id
                            continue
                        
                        technician_id = self._get_or_create_id(cursor, 'Technicians', 'TechnicianName', ticket.technician_name)
                        if technician_id is None:
                            self._update_last_processed_id(cursor, ticket.ticket_id)
                            cnxn_dest.commit()
                            last_id = ticket.ticket_id
                            continue

                        source_system_id = 1
                        sql_insert_ticket = "INSERT INTO dbo.Tickets (SourceTicketNumber, SourceSystemID, TechnicianID, DateClosed) OUTPUT INSERTED.TicketID VALUES (?, ?, ?, ?);"
                        new_ticket_id = cursor.execute(sql_insert_ticket, ticket.ticket_id, source_system_id, technician_id, ticket.date_closed).fetchone()[0]
                        
                        for skill_name in discovered_skills:
                            discovered_skill_id = self._get_or_create_id(cursor, 'DiscoveredSkills', 'DiscoveredSkillName', skill_name)
                            if discovered_skill_id:
                                cursor.execute("INSERT INTO dbo.TicketSkills (TicketID, DiscoveredSkillID) VALUES (?, ?);", new_ticket_id, discovered_skill_id)
                        
                        self._update_last_processed_id(cursor, ticket.ticket_id)
                        cnxn_dest.commit()
                        total_tickets_processed += 1
                        print(f"   -> Successfully saved results for Ticket #{ticket.ticket_id}.")
                    except Exception as e_ticket:
                        print(f"   !! FAILED to process Ticket #{ticket.ticket_id}. ROLLING BACK. Error: {e_ticket}")
                        cnxn_dest.rollback()
                    
                    last_id = ticket.ticket_id
            
            # ... (Final run logging) ...
        
        except Exception as e_main:
            print(f" !! A CRITICAL ERROR occurred during the run: {e_main}")
            # ... (Error logging) ...
        finally:
            if cnxn_dest:
                cnxn_dest.close()

if __name__ == '__main__':
    runner = AnalysisRunner()
    runner.run()