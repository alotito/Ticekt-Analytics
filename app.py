# app.py
import streamlit as st

st.set_page_config(
    page_title="Ticket Analytics Home", # UPDATED
    page_icon="📊",
    layout="wide"
)

st.title("📊 Welcome to the Ticket Analytics Portal") # UPDATED

st.write(
    "This system uses a local Large Language Model (LLM) to analyze service ticket data "
    "and provide actionable insights."
)
st.info("Select an analysis module from the navigation sidebar on the left to begin.")