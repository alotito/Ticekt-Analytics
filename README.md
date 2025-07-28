Of course. Based on our entire development process, here is a comprehensive `README.md` file for the project. This can serve as the main documentation on GitHub.

-----

# Help Desk Ticket Analytics Portal

> A Python-based application designed to analyze IT service tickets using a local Large Language Model (LLM). It extracts technical skills from ticket resolutions, stores them in a SQL Server database, and provides a web-based interface for analysis.

This project was built to turn unstructured ticket data into actionable insights for training, resource management, and identifying subject matter experts.

## ✨ Features

  * **Automated Skill Extraction:** Uses a local LLM via Ollama to read tickets and identify the technical skills required for resolution.
  * **Batch Processing:** Efficiently processes a large history of tickets in manageable, memory-safe batches.
  * **Database Checkpointing:** A robust checkpointing system in the database ensures that processing can be stopped and restarted without duplicating work.
  * **Graceful Shutdown:** The back-end script can be stopped cleanly between batches by creating a `stop.txt` file.
  * **Interactive Web App:** A Streamlit-based web application provides:
      * A dashboard to visualize the most common skills found.
      * A status page to monitor the back-end processing runs.
      * A debug tool to test the analysis on a single ticket in real-time.
  * **Modular & Abstracted Design:**
      * A Data Access Layer (`dal_cw.py`) isolates the source database, making it easy to switch ticketing systems in the future.
      * An LLM Interface (`llm_interface.py`) isolates the AI model, allowing for easy switching of models or providers.
  * **Configurable:** All settings, including database credentials, model names, and processing parameters, are managed in a central `config.ini` file.

## ⚙️ Tech Stack

  * **Backend:** Python 3.13
  * **AI:** Ollama with `llama3:instruct` model
  * **Database:** Microsoft SQL Server 2025
  * **Web Frontend:** Streamlit
  * **Key Python Libraries:** `pytds`, `requests`, `pandas`, `streamlit`

## 📂 Project Structure

```text
C:\Ticket_Studies\
|
+-- app.py                   # Main Streamlit landing page
+-- config.ini               # All configuration
+-- requirements.txt         # Python package dependencies
+-- run_backend.bat          # Script to run the backend processor
+-- start_app.bat            # Script to start the Streamlit UI
+-- stop.txt                 # (Create this file to stop the backend)
|
+-- database/
|   +-- schema.sql           # T-SQL script to create all tables
|
+-- pages/
|   +-- 1_Skills_Discovered.py
|   +-- 2_Skill_Ticket_Check.py
|   +-- 3_Skill_Processing_Status.py
|
+-- prompts/
|   +-- skills_discovery_prompt.txt
|
+-- analytics_engine/
    +-- __init__.py
    +-- run_analysis.py      # Main backend processing script
    +-- models.py            # Defines the StandardTicket data class
    +-- llm_interface.py     # AI abstraction module
    +-- dal_cw.py            # Data Access Layer for ConnectWise
```

## 🚀 Getting Started

### Prerequisites

  * Python 3.13 or higher
  * Microsoft SQL Server (2017 or newer)
  * Ollama installed and running
  * Git (for cloning)

### Installation Steps

1.  **Clone the repository:**

    ```sh
    git clone <your-repo-url>
    cd <your-repo-folder>
    ```

2.  **Set up the Database:**

      * On your SQL Server, create a new database (e.g., `TicketAnalytics`).
      * Run the script in `database/schema.sql` to create all the necessary tables.
      * Run the T-SQL scripts to create the `TheTicketReader` login on your source database and `TheAnalyst` login on your `TicketAnalytics` database, ensuring they have the correct permissions.

3.  **Set up the AI Model:**

      * Pull the required model from Ollama:
        ```sh
        ollama pull llama3:instruct
        ```

4.  **Set up the Python Environment:**

      * Create and activate a virtual environment:
        ```sh
        python -m venv .venv
        .\.venv\Scripts\activate.bat
        ```
      * Install the required packages:
        ```sh
        pip install -r requirements.txt
        ```

## 🔧 Configuration

All application settings are controlled by the **`config.ini`** file.

#### `[connectwise_db]`

Connection details for your source ticketing system database.

  * `server`: Hostname of the database server.
  * `DatabaseName`: Name of the source database.
  * `user`: Read-only user for the source database (e.g., `TheTicketReader`).
  * `password`: Password for the read-only user.

#### `[analytics_db]`

Connection details for your destination `TicketAnalytics` database.

  * `server`: Hostname and port of the destination server (e.g., `thelibrary,49957`).
  * `database`: Name of the destination database (e.g., `TicketAnalytics`).
  * `user`: Read/write user for the destination database (e.g., `TheAnalyst`).
  * `password`: Plain-text password for the read/write user (used by the web app).
  * `password_b64`: The same password, but Base64 encoded (used by the back-end script).

#### `[skills_settings]`

Parameters for the skills analysis back-end process.

  * `model`: The name of the Ollama model to use (e.g., `llama3:instruct`).
  * `prompt_path`: The file path to the prompt template.
  * `batch_size`: The number of tickets to process in each batch.
  * `max_token_threshold`: Tickets with an estimated token count above this will be skipped.

## ▶️ How to Run

1.  **Run the Back-End Processor:**

      * To start processing tickets, double-click the **`run_backend.bat`** file.
      * A terminal window will open and show the script's progress. It will process tickets in batches until it reaches the end or is stopped.

2.  **Run the Web App:**

      * To view the dashboards, double-click the **`start_app.bat`** file.
      * This will open a new terminal and launch the Streamlit app in your web browser.

3.  **Stop the Back-End Processor Gracefully:**

      * While the `run_backend.bat` script is running, create an empty file named **`stop.txt`** in the root directory (`C:\Ticket_Studies`).
      * The script will finish its current batch, detect the file, update its status, and shut down cleanly.
