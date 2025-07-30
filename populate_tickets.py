# populate_tickets.py
import configparser
import pytds
from analytics_engine.dal_cw import ConnectWiseDAL
from analytics_engine.dal_analytics import AnalyticsDAL

class PopulationController:
    def __init__(self, config_path='config.ini'):
        self.cw_dal = ConnectWiseDAL(config_path)
        self.analytics_dal = AnalyticsDAL(config_path)
        
        config = configparser.ConfigParser()
        config.read(config_path)
        db_config = config['analytics_db']
        
        server_and_port = db_config['server'].split(',')
        self.server = server_and_port[0]
        self.port = int(server_and_port[1]) if len(server_and_port) > 1 else 1433
        self.database = db_config['database']
        self.user = db_config['user']
        self.password = db_config['password']

    def _get_db_connection(self):
        return pytds.connect(
            server=self.server, database=self.database, user=self.user, 
            password=self.password, port=self.port, autocommit=False
        )

    def get_last_checkpoint(self) -> int:
        with self._get_db_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute("SELECT TOP 1 LastProcessedTicketID FROM dbo.ProcessingCheckpoint")
            result = cursor.fetchone()
            return result[0] if result else 0

    def update_checkpoint(self, new_checkpoint_id: int):
        with self._get_db_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute("UPDATE dbo.ProcessingCheckpoint SET LastProcessedTicketID = %s", (new_checkpoint_id,))
            if cursor.rowcount == 0:
                cursor.execute("INSERT INTO dbo.ProcessingCheckpoint (LastProcessedTicketID) VALUES (%s)", (new_checkpoint_id,))
            cnxn.commit()

# Replace the run_population method in populate_tickets.py
    def run_population(self):
        print("--- Running Ticket Population ---")
        last_id = self.get_last_checkpoint()
        print(f"Current checkpoint: Last ticket ID was {last_id}.")

        print("Fetching new tickets from source...")
        new_tickets = self.cw_dal.get_ticket_batch(last_id, 1000)

        if not new_tickets:
            print("No new tickets found.")
            return 0

        print(f"Found {len(new_tickets)} new tickets to insert.")
        
        latest_ticket_id = last_id
        newly_inserted_count = 0
        with self._get_db_connection() as cnxn:
            cursor = cnxn.cursor()
            insert_sql = """
                INSERT INTO dbo.Tickets (SourceTicketNumber, SourceSystemID, TechnicianID, DateClosed)
                VALUES (%s, %s, %s, %s)
            """
            for ticket in new_tickets:
                try:
                    # Use strip() to handle potential whitespace-only names
                    technician_name = ticket.technician_name.strip() if ticket.technician_name else ""

                    if not technician_name:
                        # --- THIS IS THE NEW, MORE DETAILED DEBUG PRINT ---
                        print(f"   !! Skipping Ticket #{ticket.ticket_id} (Technician name from source was blank or whitespace)")
                        if ticket.ticket_id > latest_ticket_id:
                           latest_ticket_id = ticket.ticket_id
                        continue
                    
                    technician_id = self.analytics_dal.get_or_create_technician(technician_name)
                    
                    cursor.execute(insert_sql, (ticket.ticket_id, 1, technician_id, ticket.date_closed))
                    
                    newly_inserted_count += 1
                    if ticket.ticket_id > latest_ticket_id:
                        latest_ticket_id = ticket.ticket_id
                except Exception as e:
                    print(f"Skipping potential duplicate ticket #{ticket.ticket_id}. Error: {e}")
                    if ticket.ticket_id > latest_ticket_id:
                        latest_ticket_id = ticket.ticket_id
            
            cnxn.commit()

        if latest_ticket_id > last_id:
            self.update_checkpoint(latest_ticket_id)
            print(f"Population successful. New checkpoint is {latest_ticket_id}.")
        
        return newly_inserted_count
        if latest_ticket_id > last_id:
            self.update_checkpoint(latest_ticket_id)
            print(f"Population successful. New checkpoint is {latest_ticket_id}.")
        
        return newly_inserted_count
        
if __name__ == "__main__":
    controller = PopulationController()
    new_count = controller.run_population()
    print(f"\nProcess complete. Added {new_count} new tickets to the queue.")