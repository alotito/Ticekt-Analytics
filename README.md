Certainly. Here is a comprehensive `README.md` for your Ticket Analytics system.

***
# 📊 Ticket Analytics Portal

This project is an ETL pipeline and data analytics dashboard that uses a local Large Language Model (LLM) to analyze IT service tickets. It extracts technical skills from unstructured ticket text, stores the structured results in a SQL Server database, and presents insights through an interactive web application built with Streamlit.

## ✨ Features

* **Automated ETL Pipeline**: A continuously running controller manages the process of fetching, analyzing, and storing ticket data.
* **LLM-Powered Skill Extraction**: Utilizes a local Ollama server with a Llama 3 model to identify technical skills from ticket notes.
* **Parallel Processing**: A multi-process worker architecture processes batches of tickets concurrently for high throughput.
* **Interactive Dashboards**: A suite of web-based tools for visualizing data and managing skills.
    * View top discovered and managed skills.
    * Monitor live worker status and queue metrics.
    * Analyze skill profiles for individual technicians.
* **Skill Management**: A comprehensive UI to curate, categorize, and visualize discovered skills into a master "Managed Skills" list.
* **Deployment Ready**: Includes configuration for deploying the Streamlit frontend behind an IIS reverse proxy.

---
## 🏗️ System Architecture
The application operates in a continuous, automated cycle managed by a central controller.

1.  **Population**: The **Master Controller** (`master_controller.py`) starts a cycle by calling the **Population** module (`populate_tickets.py`). This module connects to the source ConnectWise database, finds new tickets since the last check, and inserts them into a queue in the local `TicketAnalytics` database with a "Pending" status.
2.  **Processing**: The **Master Controller** then launches multiple **Worker** processes (`run_analysis.py`). These workers run in parallel.
3.  **Claim & Analyze**: Each worker claims a unique batch of "Pending" tickets from the queue, which prevents duplication of work. For each ticket, the worker calls the **Ollama LLM** (`llm_interface.py`) to extract skills.
4.  **Store**: The worker saves the extracted skills to the `TicketAnalytics` database and marks the ticket as "Complete."
5.  **Visualize**: A separate **Streamlit** process (`app.py` and `pages/`) runs the web server. Users interact with the dashboards, which query the `TicketAnalytics` database to display real-time and historical data.

---
## 📦 Modules & Components

### Core Backend
* **`master_controller.py`**: The main entry point for the backend. It orchestrates the entire ETL pipeline in a continuous loop, launches workers, captures their logs, and handles graceful shutdown.
* **`populate_tickets.py`**: Responsible for fetching new ticket data from the source database (ConnectWise) and populating the local `dbo.Tickets` table, which serves as the work queue. It uses the `dbo.ProcessingCheckpoint` table to avoid re-importing tickets.
* **`config.ini`**: The central configuration file for all database credentials, LLM model settings, and performance tuning (e.g., `worker_count`, `population_batch_size`).
* **`Schema.sql`**: The master SQL script containing the full Data Definition Language (DDL) to create the `TicketAnalytics` database, its tables, indexes, users, permissions, and stored procedures.

### `analytics_engine/` (Python Library)
* **`run_analysis.py`**: The script defining a single worker process. Its main loop consists of claiming a batch of tickets, fetching full details, calling the LLM, saving the results, and updating the ticket's status.
* **`dal_analytics.py`**: The Data Access Layer for the `TicketAnalytics` database. It contains all methods for reading from and writing to the local SQL Server database (e.g., claiming batches, saving skills, getting technicians).
* **`dal_cw.py`**: The Data Access Layer for the source ConnectWise database. It handles connections and queries to fetch raw ticket data.
* **`llm_interface.py`**: A dedicated module for communicating with the Ollama LLM API. It formats the prompt and handles the HTTP request to get the skill analysis.
* **`utils.py`**: Contains shared helper functions, most importantly the robust `parse_llm_output` function that cleans and validates the LLM's JSON response.
* **`models.py`**: Defines the `StandardTicket` dataclass, providing a consistent data structure for ticket information throughout the application.

### `pages/` (Streamlit UI)
* **`app.py`**: The main landing page for the Streamlit web application.
* **`1_Skills_Discovered.py`**: A dashboard that visualizes the top 20 most frequently occurring skills across all tickets.
* **`2_Skill_Ticket_Check.py`**: An interactive tool for performing an on-demand skill analysis of a single ticket by its number.
* **`3_Skill_Processing_Status.py`**: A live dashboard that shows real-time metrics, including the number of tickets in the queue, the list of active workers, and the average throughput.
* **`4_Managed_Skills.py`**: A comprehensive management console to view, create, edit, and delete curated "Managed Skills," associate discovered skills with them, and visualize the results.
* **`5_Technician_Skills.py`**: A dashboard to view the top skills (either discovered or managed) associated with a specific technician.
* **`6_Skill_Search.py`**: An interactive tool to find which technicians are most frequently associated with a specific skill.

---
## 🚀 Usage

1.  **Start Ollama Server**: Run the batch file (or command) to start the Ollama server with the desired parallelism.
2.  **Start Master Controller**: Open a new terminal, activate the virtual environment, and run `python master_controller.py`. This will start the continuous backend processing.
3.  **Start Web App**: Open a third terminal, activate the virtual environment, and run `streamlit run app.py`.
4.  **Access the UI**: Open your web browser and navigate to `http://localhost:8501`.
