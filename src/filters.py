# src/filters.py
from __future__ import annotations
import streamlit as st
from datetime import date

def visit_filters():
    with st.expander("Filters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            visit_id = st.text_input("Visit ID", placeholder="e.g., 123456")
        with col2:
            patient_id = st.text_input("Patient ID / Iqama (optional)")
        with col3:
            contractor = st.text_input("Payer / Contractor (optional)")

        col4, col5 = st.columns(2)
        with col4:
            start = st.date_input("Start date", value=None)
        with col5:
            end = st.date_input("End date", value=None)

        classification = st.multiselect(
            "Visit Classification", ["Inpatient", "Outpatient", "Emergency", "Ambulatory"]
        )

    return {
        "visit_id": visit_id.strip(),
        "patient_id": patient_id.strip(),
        "contractor": contractor.strip(),
        "start": start if isinstance(start, date) else None,
        "end": end if isinstance(end, date) else None,
        "classification": classification,
    }