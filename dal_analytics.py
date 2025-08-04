# analytics_engine/dal_analytics.py

import pytds
import configparser
from typing import List, Dict
from datetime import datetime

class AnalyticsDAL:
    """Handles all communication with the destination TicketAnalytics database."""
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
            autocommit=True
        )

    # --- Methods for Backend Processing (master_controller.py, run_analysis.py) ---

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
        return tickets

    def update_ticket_status(self, ticket_id: int, status_id: int):
        """Updates the status of a single ticket after processing."""
        sql = "UPDATE dbo.Tickets SET ProcessingStatusID = %s, LastUpdated = GETUTCDATE() WHERE TicketID = %s"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (status_id, ticket_id))

    def save_skills_for_ticket(self, ticket_pk: int, skills: List[str]):
        """Saves a list of skills and associates them with a ticket by calling a stored procedure."""
        sql = "EXEC dbo.sp_LinkSkillToTicket @TicketID=%s, @SkillName=%s"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            for skill in skills:
                if skill:
                    cursor.execute(sql, (ticket_pk, skill))

    def get_pending_ticket_count(self) -> int:
        """Returns the number of tickets with a 'Pending' status."""
        sql = "SELECT COUNT(*) FROM dbo.Tickets WHERE ProcessingStatusID = 0;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            result = cursor.fetchone()
            return result[0] if result else 0
            
    def get_or_create_technician(self, technician_name: str) -> int:
        """Gets the ID for a technician by name by calling a thread-safe stored procedure."""
        if not technician_name or not technician_name.strip():
            technician_name = "Unassigned"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute("EXEC dbo.sp_GetOrCreateTechnician @TechnicianName=%s", (technician_name,))
            technician_id = cursor.fetchone()[0]
        return technician_id

    def reset_stuck_tickets(self) -> int:
        """Resets any tickets stuck in a 'Claimed' state back to 'Pending'."""
        sql = "UPDATE dbo.Tickets SET ProcessingStatusID = 0, WorkerID = NULL WHERE ProcessingStatusID = 1;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.rowcount if cursor.rowcount is not None else 0

    # --- Methods for Streamlit Pages ---

    def get_top_discovered_skills(self, count: int = 20) -> List[Dict]:
        """Calculates the frequency of the top N most discovered skills."""
        sql = """
            SELECT TOP (%s)
                ds.DiscoveredSkillName,
                COUNT(ts.TicketID) AS Frequency
            FROM dbo.DiscoveredSkills ds
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            GROUP BY ds.DiscoveredSkillName
            ORDER BY Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (count,))
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

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
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_managed_skills(self) -> List[Dict]:
        """Fetches all managed skills, ordered by name."""
        sql = "SELECT ManagedSkillID, ManagedSkillName, Description, IsException, DistilledSkillID FROM dbo.ManagedSkills ORDER BY ManagedSkillName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def add_managed_skill(self, name: str, description: str, is_exception: bool = False):
        """Adds a new managed skill to the database."""
        sql = "INSERT INTO dbo.ManagedSkills (ManagedSkillName, Description, IsException) VALUES (%s, %s, %s);"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (name, description, is_exception))

    def update_managed_skill(self, skill_id: int, new_name: str, new_description: str, is_exception: bool = False):
        """Updates an existing managed skill."""
        sql = "UPDATE dbo.ManagedSkills SET ManagedSkillName = %s, Description = %s, IsException = %s WHERE ManagedSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (new_name, new_description, is_exception, skill_id))

    def delete_managed_skill(self, skill_id: int):
        """Deletes a managed skill."""
        sql = "DELETE FROM dbo.ManagedSkills WHERE ManagedSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (skill_id,))

    def get_top_unassociated_skills(self, count: int = 10) -> List[Dict]:
        """Gets the most frequent skills that are not yet associated with a managed skill."""
        sql = """
            SELECT TOP (%s)
                ds.DiscoveredSkillID, ds.DiscoveredSkillName, COUNT(ts.TicketID) as Frequency
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
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def associate_skill(self, discovered_skill_id: int, managed_skill_id: int):
        """Associates a discovered skill with a managed skill."""
        sql = "UPDATE dbo.DiscoveredSkills SET ManagedSkillID = %s WHERE DiscoveredSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (managed_skill_id, discovered_skill_id))
            
    def get_all_technicians(self) -> List[Dict]:
        """Fetches all active technicians from the database."""
        sql = "SELECT TechnicianID, TechnicianName FROM dbo.Technicians WHERE IsActive = 1 ORDER BY TechnicianName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]
    
    def get_managed_skills_by_technician(self, technician_id: int) -> List[Dict]:
        """Gets a frequency list of all MANAGED skills associated with a given technician's tickets."""
        sql = """
            SELECT
                ms.ManagedSkillName, COUNT(ms.ManagedSkillID) AS Frequency
            FROM dbo.Tickets t
            JOIN dbo.TicketSkills ts ON t.TicketID = ts.TicketID
            JOIN dbo.DiscoveredSkills ds ON ts.DiscoveredSkillID = ds.DiscoveredSkillID
            JOIN dbo.ManagedSkills ms ON ds.ManagedSkillID = ms.ManagedSkillID
            WHERE t.TechnicianID = %s
            GROUP BY ms.ManagedSkillName
            ORDER BY Frequency DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (technician_id,))
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def get_technicians_by_managed_skill(self, managed_skill_name: str) -> List[Dict]:
        """Gets a frequency list of technicians associated with a given managed skill."""
        sql = """
            SELECT
                tech.TechnicianName,
                COUNT(DISTINCT t.TicketID) AS TicketCount
            FROM dbo.ManagedSkills ms
            JOIN dbo.DiscoveredSkills ds ON ms.ManagedSkillID = ds.ManagedSkillID
            JOIN dbo.TicketSkills ts ON ds.DiscoveredSkillID = ts.DiscoveredSkillID
            JOIN dbo.Tickets t ON ts.TicketID = t.TicketID
            JOIN dbo.Technicians tech ON t.TechnicianID = tech.TechnicianID
            WHERE
                ms.ManagedSkillName = %s
            GROUP BY
                tech.TechnicianName
            ORDER BY
                TicketCount DESC;
        """
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (managed_skill_name,))
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row)) for row in rows]

    # --- Methods for Meta-Analysis Page ---
    
    def generate_meta_analysis_sql(self, analysis_result: List[Dict]) -> List[tuple]:
        """Generates SQL commands, preventing duplicate INSERTs for the same canonical name within a single batch."""
        sql_commands = []
        newly_added_skills = set()
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            for group in analysis_result:
                canonical_name = group.get('canonical_name')
                original_skills = group.get('original_skills', [])
                if not canonical_name or not original_skills: continue
                managed_skill_id = None
                if canonical_name in newly_added_skills:
                    managed_skill_id = canonical_name
                else:
                    cursor.execute("SELECT ManagedSkillID FROM dbo.ManagedSkills WHERE ManagedSkillName = %s", (canonical_name,))
                    result = cursor.fetchone()
                    if not result:
                        managed_skill_id = canonical_name
                        insert_sql = "INSERT INTO dbo.ManagedSkills (ManagedSkillName, Description, IsException) VALUES (%s, %s, %s);"
                        insert_params = (canonical_name, f"Auto-generated for '{canonical_name}'", False)
                        sql_commands.append((insert_sql, insert_params))
                        newly_added_skills.add(canonical_name)
                    else:
                        managed_skill_id = result[0]
                if managed_skill_id:
                    update_sql = "UPDATE dbo.DiscoveredSkills SET ManagedSkillID = %s WHERE DiscoveredSkillName = %s;"
                    for skill_name in original_skills:
                        update_params = (managed_skill_id, skill_name)
                        sql_commands.append((update_sql, update_params))
        return sql_commands

    def execute_meta_analysis_sql(self, sql_commands: List[tuple]):
        """Executes a list of (sql, params) tuples to update skills."""
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            temp_id_map = {}
            for sql, params in sql_commands:
                if "INSERT INTO dbo.ManagedSkills" in sql:
                    cursor.execute(sql, params)
                    cursor.execute("SELECT SCOPE_IDENTITY();")
                    new_id = cursor.fetchone()[0]
                    placeholder_name = params[0]
                    temp_id_map[placeholder_name] = new_id
                elif "UPDATE dbo.DiscoveredSkills" in sql:
                    managed_id, skill_name = params
                    actual_id = temp_id_map.get(managed_id, managed_id)
                    if actual_id:
                        cursor.execute(sql, (actual_id, skill_name))

    def get_unassociated_skills_batch(self, batch_size: int) -> List[str]:
        """Fetches a batch of unassociated discovered skill names."""
        sql = "SELECT TOP (%s) DiscoveredSkillName FROM dbo.DiscoveredSkills WHERE ManagedSkillID IS NULL ORDER BY DiscoveredSkillID;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (batch_size,))
            return [row[0] for row in cursor.fetchall()]
             
    def get_unassociated_skill_count(self) -> int:
        """Gets the total count of unassociated skills."""
        sql = "SELECT COUNT(*) FROM dbo.DiscoveredSkills WHERE ManagedSkillID IS NULL;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]

    def get_associated_skill_count(self) -> int:
        """Gets the total count of discovered skills that have been associated."""
        sql = "SELECT COUNT(*) FROM dbo.DiscoveredSkills WHERE ManagedSkillID IS NOT NULL;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]

    def get_managed_skill_count(self) -> int:
        """Gets the total count of managed skills."""
        sql = "SELECT COUNT(*) FROM dbo.ManagedSkills;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]

    # --- Methods for Skill Distiller ---

    def get_distilled_skills(self) -> List[Dict]:
        """Fetches all distilled skills, ordered by name."""
        sql = "SELECT DistilledSkillID, DistilledSkillName, Description FROM dbo.DistilledSkills ORDER BY DistilledSkillName;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            cols = [desc[0] for desc in cursor.description]
            return [dict(zip(cols, row)) for row in cursor.fetchall()]

    def add_distilled_skill(self, name: str, description: str):
        """Adds a new distilled skill."""
        sql = "INSERT INTO dbo.DistilledSkills (DistilledSkillName, Description) VALUES (%s, %s);"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (name, description))

    def update_distilled_skill(self, skill_id: int, new_name: str, new_description: str):
        """Updates an existing distilled skill."""
        sql = "UPDATE dbo.DistilledSkills SET DistilledSkillName = %s, Description = %s WHERE DistilledSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (new_name, new_description, skill_id))

    def delete_distilled_skill(self, skill_id: int):
        """Deletes a distilled skill."""
        sql = "DELETE FROM dbo.DistilledSkills WHERE DistilledSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (skill_id,))

    def get_unassociated_managed_skills_batch(self, batch_size: int) -> List[str]:
        """Fetches a batch of managed skill names that are not yet associated with a distilled skill."""
        sql = "SELECT TOP (%s) ManagedSkillName FROM dbo.ManagedSkills WHERE DistilledSkillID IS NULL ORDER BY ManagedSkillID;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (batch_size,))
            return [row[0] for row in cursor.fetchall()]

    def get_unassociated_managed_skill_count(self) -> int:
        """Gets the total count of managed skills not yet associated with a distilled skill."""
        sql = "SELECT COUNT(*) FROM dbo.ManagedSkills WHERE DistilledSkillID IS NULL;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql)
            return cursor.fetchone()[0]
    
    def generate_distillation_sql(self, analysis_result: List[Dict]) -> List[tuple]:
        """Generates SQL commands for the distiller process without executing them."""
        sql_commands = []
        newly_added_distilled = set()
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            for group in analysis_result:
                distilled_name = group.get('distilled_name')
                original_managed_skills = group.get('original_managed_skills', [])
                if not distilled_name or not original_managed_skills: continue
                
                distilled_skill_id = None
                if distilled_name in newly_added_distilled:
                    distilled_skill_id = distilled_name
                else:
                    cursor.execute("SELECT DistilledSkillID FROM dbo.DistilledSkills WHERE DistilledSkillName = %s", (distilled_name,))
                    result = cursor.fetchone()
                    if not result:
                        distilled_skill_id = distilled_name
                        insert_sql = "INSERT INTO dbo.DistilledSkills (DistilledSkillName, Description) VALUES (%s, %s);"
                        insert_params = (distilled_name, f"Auto-generated for '{distilled_name}'")
                        sql_commands.append((insert_sql, insert_params))
                        newly_added_distilled.add(distilled_name)
                    else:
                        distilled_skill_id = result[0]
                
                if distilled_skill_id:
                    update_sql = "UPDATE dbo.ManagedSkills SET DistilledSkillID = %s WHERE ManagedSkillName = %s;"
                    for managed_skill in original_managed_skills:
                        update_params = (distilled_skill_id, managed_skill)
                        sql_commands.append((update_sql, update_params))
        return sql_commands

    def execute_distillation_sql(self, sql_commands: List[tuple]):
        """Executes a list of (sql, params) tuples to update managed skills with distilled skill IDs."""
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            temp_id_map = {}
            for sql, params in sql_commands:
                if "INSERT INTO dbo.DistilledSkills" in sql:
                    cursor.execute(sql, params)
                    cursor.execute("SELECT SCOPE_IDENTITY();")
                    new_id = cursor.fetchone()[0]
                    placeholder_name = params[0]
                    temp_id_map[placeholder_name] = new_id
                elif "UPDATE dbo.ManagedSkills" in sql:
                    distilled_id, managed_name = params
                    actual_id = temp_id_map.get(distilled_id, distilled_id)
                    if actual_id:
                        cursor.execute(sql, (actual_id, managed_name))

    # Add this function to the AnalyticsDAL class in dal_analytics.py
    def associate_managed_skill(self, managed_skill_id: int, distilled_skill_id: int):
        """Associates a managed skill with a distilled skill."""
        sql = "UPDATE dbo.ManagedSkills SET DistilledSkillID = %s WHERE ManagedSkillID = %s;"
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(sql, (distilled_skill_id, managed_skill_id))

    # Add this function to the AnalyticsDAL class in dal_analytics.py

    def merge_distilled_skill(self, source_skill_id: int, target_skill_id: int):
        """
        Reassigns all children of the source skill to the target skill,
        then deletes the source skill.
        """
        # Re-parent all associated ManagedSkills
        reparent_sql = "UPDATE dbo.ManagedSkills SET DistilledSkillID = %s WHERE DistilledSkillID = %s;"
        # Delete the now-empty source skill
        delete_sql = "DELETE FROM dbo.DistilledSkills WHERE DistilledSkillID = %s;"
        
        with self._get_connection() as cnxn:
            cursor = cnxn.cursor()
            cursor.execute(reparent_sql, (target_skill_id, source_skill_id))
            cursor.execute(delete_sql, (source_skill_id,))