# analytics_engine/models.py
# Defines the standard data classes (models) for the application.

from dataclasses import dataclass
from datetime import datetime

@dataclass
class StandardTicket:
    """
    A standardized data class representing a single service ticket.
    This class acts as the "contract" between the Data Access Layer
    and the main processing logic, ensuring data consistency.
    """
    ticket_id: int
    summary: str
    status: str
    client_name: str
    technician_name: str
    initial_description: str
    resolution_notes: str
    date_closed: datetime

    @property
    def full_text(self) -> str:
        """
        A helper property that combines all relevant text fields into a
        single block of text, ready to be sent to the LLM for analysis.
        """
        # Combine the text fields, ensuring None values are handled gracefully.
        parts = [
            self.summary or "",
            self.initial_description or "",
            self.resolution_notes or ""
        ]
        return "\n---\n".join(parts)