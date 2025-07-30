# analytics_engine/dal_analytics.py
import pytds
import configparser
from typing import List, Dict

class AnalyticsDAL:
    """Handles communication with the destination TicketAnalytics database."""
    def __init__(self, config_path: str = 'config.ini'):
        config = configparser.ConfigParser()
        config.read(config_path)
        db_config = config['analytics_db']
        
        server_and_port = db_config['server'].split(',')
        self.server = server_and_port[0]
        self.port = int(server_and_port[1]) if len(server_and_port) > 1 else 1433
        self.database = db_config['database']
        self.user = db_config['user']
        self.password = db_config['password']

    def _get_connection(self):
        """Establishes and returns a new database connection."""
        return pytds.connect(
            server=self.server, port=self.port, database=self.database,
            user=self.user, password=self.password,
            autocommit=False  # UPDATED: Disable autocommit for manual control
        )

    def claim_ticket_batch(self, worker_id: str, batch_size: int) -> List[Dict]:
        """Calls the stored procedure to atomically claim a batch of tickets."""
        tickets = []
        sql = "EXEC dbo.sp_ClaimTicketBatch @WorkerID=%s, @BatchSize=%s"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (worker_id, batch_size))
            rows = cursor.fetchall()
            cols = [desc[0] for desc in cursor.description]
            for row in rows:
                tickets.append(dict(zip(cols, row)))
            cnxn.commit() # ADDED: Explicitly commit the UPDATE
        return tickets

    def update_ticket_status(self, ticket_id: int, status_id: int):
        """Updates the status of a single ticket after processing."""
        sql = "UPDATE dbo.Tickets SET ProcessingStatusID = %s, LastUpdated = GETUTCDATE() WHERE TicketID = %s"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (status_id, ticket_id))
            cnxn.commit() # ADDED: Explicitly commit the UPDATE

    def save_skills_for_ticket(self, ticket_pk: int, skills: List[str]):
        """Saves a list of skills and associates them with a ticket."""
        sql = "EXEC dbo.sp_LinkSkillToTicket @TicketID=%s, @SkillName=%s"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            for skill in skills:
                if skill:
                    cursor.execute(sql, (ticket_pk, skill))
            cnxn.commit() # ADDED: Explicitly commit all skill links

    def get_pending_ticket_count(self) -> int:
        """Returns the number of tickets with a 'Pending' status."""
        sql = "SELECT COUNT(*) FROM dbo.Tickets WHERE ProcessingStatusID = 0;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]

    def get_managed_skills(self) -> List[Dict]:
        """Fetches all managed skills, ordered by name."""
        sql = "SELECT ManagedSkillID, ManagedSkillName, Description FROM dbo.ManagedSkills ORDER BY ManagedSkillName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    def add_managed_skill(self, name: str, description: str):
        """Adds a new managed skill to the database."""
        sql = "INSERT INTO dbo.ManagedSkills (ManagedSkillName, Description) VALUES (%s, %s);"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (name, description))
            cnxn.commit() # ADDED: Explicitly commit the INSERT

    def update_managed_skill(self, skill_id: int, new_name: str, new_description: str):
        """Updates an existing managed skill."""
        sql = "UPDATE dbo.ManagedSkills SET ManagedSkillName = %s, Description = %s WHERE ManagedSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (new_name, new_description, skill_id))
            cnxn.commit() # ADDED: Explicitly commit the UPDATE

    def delete_managed_skill(self, skill_id: int):
        """Deletes a managed skill."""
        sql = "DELETE FROM dbo.ManagedSkills WHERE ManagedSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (skill_id,))
            cnxn.commit() # ADDED: Explicitly commit the DELETE

    def get_top_unassociated_skills(self, count: int = 10) -> List[Dict]:
        """Gets the most frequent skills that are not yet associated with a managed skill."""
        sql = """
            SELECT TOP (%s)
                ds.DiscoveredSkillID,
                ds.DiscoveredSkillName,
                COUNT(ts.TicketID) as Frequency
            FROM dbo.DiscoveredSkills ds
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            WHERE ds.ManagedSkillID IS NULL
            GROUP BY ds.DiscoveredSkillID, ds.DiscoveredSkillName
            ORDER BY Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (count,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    def associate_skill(self, discovered_skill_id: int, managed_skill_id: int):
        """Associates a discovered skill with a managed skill."""
        sql = "UPDATE dbo.DiscoveredSkills SET ManagedSkillID = %s WHERE DiscoveredSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (managed_skill_id, discovered_skill_id))
            cnxn.commit() # ADDED: Explicitly commit the UPDATE
            
    def get_managed_skill_occurrences(self) -> List[Dict]:
        """Gets the total occurrence count for each managed skill."""
        sql = """
            SELECT
                ms.ManagedSkillName,
                COUNT(ts.TicketID) as TotalOccurrences
            FROM dbo.ManagedSkills ms
            JOIN dbo.DiscoveredSkills ds ON ms.ManagedSkillID = ds.ManagedSkillID
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            GROUP BY ms.ManagedSkillName
            ORDER BY TotalOccurrences DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    # In analytics_engine/dal_analytics.py, add these new methods to the AnalyticsDAL class:

    def get_all_technicians(self) -> List[Dict]:
        """Fetches all active technicians from the database."""
        sql = "SELECT TechnicianID, TechnicianName FROM dbo.Technicians WHERE IsActive = 1 ORDER BY TechnicianName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    def get_skills_by_technician(self, technician_id: int) -> List[Dict]:
        """Gets a frequency list of all skills associated with a given technician's tickets."""
        sql = """
            SELECT
                ds.DiscoveredSkillName,
                COUNT(ds.DiscoveredSkillID) AS Frequency
            FROM dbo.Tickets t
            JOIN dbo.TicketSkills ts ON t.TicketID = ts.TicketID
            JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
            WHERE
                t.TechnicianID = %s
            GROUP BY
                ds.DiscoveredSkillName
            ORDER BY
                Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (technician_id,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    # In analytics_engine/dal_analytics.py, add this method to the AnalyticsDAL class:

    # In analytics_engine/dal_analytics.py

    def get_or_create_technician(self, technician_name: str) -> int:
        """
        Gets the ID for a technician by name by calling a thread-safe stored procedure.
        """
        if not technician_name or not technician_name.strip():
            technician_name = "Unassigned"

        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            # Call the stored procedure
            cursor.execute("EXEC dbo.sp_GetOrCreateTechnician @TechnicianName=%s", (technician_name,))
            technician_id = cursor.fetchone()[0]
            cnxn.commit()
            return technician_id
        
    def get_or_create_technician_Deprecated(self, technician_name: str) -> int:
        """Gets the ID for a technician by name, creating a new record if it doesn't exist."""
        # Use a default name if the source technician is null or empty
        if not technician_name or not technician_name.strip():
            technician_name = "Unassigned"

        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            # Check if technician exists
            cursor.execute("SELECT TechnicianID FROM dbo.Technicians WHERE TechnicianName = %s", (technician_name,))
            result = cursor.fetchone()
            
            if result:
                # If they exist, return their ID
                return result[0]
            else:
                # If not, insert them and get their new ID
                cursor.execute("INSERT INTO dbo.Technicians (TechnicianName) VALUES (%s)", (technician_name,))
                # Use SCOPE_IDENTITY() to get the ID that was just created
                cursor.execute("SELECT SCOPE_IDENTITY() AS ID;")
                new_id = cursor.fetchone()[0]
                cnxn.commit()
                return new_id
    # In analytics_engine/dal_analytics.py, add these methods:

    def get_all_discovered_skills(self) -> List[str]:
        """Fetches a simple list of all unique discovered skill names."""
        sql = "SELECT DISTINCT DiscoveredSkillName FROM dbo.DiscoveredSkills ORDER BY DiscoveredSkillName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            # Return a simple list of strings
            return [row[0] for row in cursor.fetchall()]

    def get_technicians_by_skill(self, skill_name: str) -> List[Dict]:
        """Gets a frequency list of technicians associated with a given skill."""
        sql = """
            SELECT
                tech.TechnicianName,
                COUNT(t.TicketID) AS TicketCount
            FROM dbo.DiscoveredSkills ds
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            JOIN dbo.Tickets t ON ts.TicketID = t.TicketID
            JOIN dbo.Technicians tech ON t.TechnicianID = tech.TechnicianID
            WHERE
                ds.DiscoveredSkillName = %s
            GROUP BY
                tech.TechnicianName
            ORDER BY
                TicketCount DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (skill_name,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
        
    # In analytics_engine/dal_analytics.py, add this method:

    def get_managed_skills_by_technician(self, technician_id: int) -> List[Dict]:
        """
        Gets a frequency list of all MANAGED skills associated with a given technician's tickets.
        """
        sql = """
            SELECT
                ms.ManagedSkillName,
                COUNT(ms.ManagedSkillID) AS Frequency
            FROM dbo.Tickets t
            JOIN dbo.TicketSkills ts ON t.TicketID = ts.TicketID
            JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
            JOIN dbo.ManagedSkills ms ON ds.ManagedSkillID = ms.ManagedSkillID
            WHERE
                t.TechnicianID = %s
            GROUP BY
                ms.ManagedSkillName
            ORDER BY
                Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (technician_id,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    
    # In analytics_engine/dal_analytics.py, add this method:

    def search_unassociated_skills(self, search_term: str) -> List[Dict]:
        """Searches the entire table for unassociated skills matching a search term."""
        # Add wildcards for a 'contains' search
        search_pattern = f"%{search_term}%"
        sql = """
            SELECT
                ds.DiscoveredSkillID,
                ds.DiscoveredSkillName,
                COUNT(ts.TicketID) as Frequency
            FROM dbo.DiscoveredSkills ds
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            WHERE
                ds.ManagedSkillID IS NULL
                AND ds.DiscoveredSkillName LIKE %s
            GROUP BY
                ds.DiscoveredSkillID, ds.DiscoveredSkillName
            ORDER BY
                Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (search_pattern,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    # Add this method to the AnalyticsDAL class in analytics_engine/dal_analytics.py

    def get_managed_skill_occurrences(self) -> List[Dict]:
        """
        Calculates the frequency of each managed skill category based on the
        associations in the TicketSkills table.
        """
        sql = """
            SELECT
                ms.ManagedSkillName,
                COUNT(ts.DiscoveredSkillID) AS Frequency
            FROM dbo.TicketSkills ts
            JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
            JOIN dbo.ManagedSkills ms ON ds.ManagedSkillID = ms.ManagedSkillID
            GROUP BY ms.ManagedSkillName
            ORDER BY Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    # Add this method to the AnalyticsDAL class in analytics_engine/dal_analytics.py

    def get_top_discovered_skills(self, count: int = 20) -> List[Dict]:
        """
        Calculates the frequency of the top N most discovered skills.
        """
        # pytds uses %s for parameters
        sql = """
            SELECT TOP (%s)
                ds.DiscoveredSkillName,
                COUNT(ts.DiscoveredSkillID) AS Frequency
            FROM dbo.TicketSkills ts
            JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
            GROUP BY ds.DiscoveredSkillName
            ORDER BY Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (count,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]
    # In analytics_engine/dal_analytics.py, add this method:

    def reset_stuck_tickets(self) -> int:
        """Resets any tickets stuck in a 'Claimed' (ProcessingStatusID=1) state back to 'Pending'."""
        sql = "UPDATE dbo.Tickets SET ProcessingStatusID = 0, WorkerID = NULL WHERE ProcessingStatusID = 1;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            affected_rows = cursor.rowcount if cursor.rowcount is not None else 0
            cnxn.commit()
            return affected_rows