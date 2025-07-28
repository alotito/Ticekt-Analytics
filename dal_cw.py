# analytics_engine/dal_cw.py
# Data Access Layer for retrieving ticket data from the source ConnectWise database.

import pyodbc
import configparser
from typing import List, Optional
from datetime import datetime
from .models import StandardTicket

class ConnectWiseDAL:
    """
    Handles all database communication with the source ConnectWise database using pyodbc.
    """
    def __init__(self, config_path: str = 'config.ini'):
        config = configparser.ConfigParser()
        config.read(config_path)

        db_config = config['connectwise_db']
        self.server = db_config['server']
        self.database = db_config['DatabaseName']
        self.user = db_config['user']
        self.password = db_config['password']
        
        self.conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"UID={self.user};"
            f"PWD={{{self.password}}};"
            f"TrustServerCertificate=yes;"
        )

    def get_closed_tickets_since(self, last_run_date: datetime) -> List[StandardTicket]:
        tickets = []
        sql_query = """
            SELECT
                ticketnbr, summary, status_description, company_name,
                contact_name, detail_description, resolution, date_closed,
                CONCAT(ticket_owner_first_name, ' ', ticket_owner_last_name) AS TechnicianFullName
            FROM v_rpt_service
            WHERE date_closed > ? 
              AND status_description LIKE '%Close%'
              AND ticket_owner_last_name IS NOT NULL -- ADDED
            ORDER BY date_closed;
        """
        cnxn = None
        try:
            cnxn = pyodbc.connect(self.conn_str)
            cursor = cnxn.cursor()
            cursor.execute(sql_query, last_run_date)
            rows = cursor.fetchall()
            for row in rows:
                ticket = StandardTicket(
                    ticket_id=row.ticketnbr, summary=row.summary, status=row.status_description,
                    client_name=row.company_name, 
                    technician_name=row.TechnicianFullName,
                    initial_description=row.detail_description, resolution_notes=row.resolution,
                    date_closed=row.date_closed
                )
                tickets.append(ticket)
        except pyodbc.Error as ex:
            print(f"Database query failed in get_closed_tickets_since. Error: {ex}")
        finally:
            if cnxn:
                cnxn.close()
        return tickets

    def get_ticket_by_number(self, ticket_number: str) -> Optional[StandardTicket]:
        sql_query = """
            SELECT TOP 1
                ticketnbr, summary, status_description, company_name,
                contact_name, detail_description, resolution, date_closed,
                CONCAT(ticket_owner_first_name, ' ', ticket_owner_last_name) AS TechnicianFullName
            FROM v_rpt_service
            WHERE CAST(ticketnbr AS NVARCHAR(50)) = ?
              AND ticket_owner_last_name IS NOT NULL; -- ADDED
        """
        cnxn = None
        try:
            cnxn = pyodbc.connect(self.conn_str)
            cursor = cnxn.cursor()
            cursor.execute(sql_query, ticket_number)
            row = cursor.fetchone()
            if row:
                ticket = StandardTicket(
                    ticket_id=row.ticketnbr, summary=row.summary, status=row.status_description,
                    client_name=row.company_name, 
                    technician_name=row.TechnicianFullName,
                    initial_description=row.detail_description, resolution_notes=row.resolution,
                    date_closed=row.date_closed
                )
                return ticket
            return None
        except pyodbc.Error as ex:
            print(f"Database query failed for ticket {ticket_number}. Error: {ex}")
            raise
        finally:
            if cnxn:
                cnxn.close()
                
    def get_ticket_batch(self, last_processed_id: int, batch_size: int) -> List[StandardTicket]:
        tickets = []
        sql_query = """
            SELECT
                ticketnbr, summary, status_description, company_name,
                contact_name, detail_description, resolution, date_closed,
                CONCAT(ticket_owner_first_name, ' ', ticket_owner_last_name) AS TechnicianFullName
            FROM v_rpt_service
            WHERE ticketnbr > ?
              AND ticket_owner_last_name IS NOT NULL -- ADDED
            ORDER BY ticketnbr
            OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY;
        """
        cnxn = None
        try:
            cnxn = pyodbc.connect(self.conn_str)
            cursor = cnxn.cursor()
            cursor.execute(sql_query, last_processed_id, batch_size)
            rows = cursor.fetchall()
            for row in rows:
                ticket = StandardTicket(
                    ticket_id=row.ticketnbr, summary=row.summary, status=row.status_description,
                    client_name=row.company_name, technician_name=row.TechnicianFullName,
                    initial_description=row.detail_description, resolution_notes=row.resolution,
                    date_closed=row.date_closed
                )
                tickets.append(ticket)
        except pyodbc.Error as ex:
            print(f"Database query failed in get_ticket_batch. Error: {ex}")
        finally:
            if cnxn:
                cnxn.close()
        return tickets