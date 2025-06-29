import streamlit as st
import pandas as pd
from pathlib import Path

# Set up base paths and logging configuration
BASE_DIR = Path(__file__).resolve().parent


st.set_page_config(page_title="Claims Predictions Performance Analysis", layout="centered")
if st.button('Get Analysis'):
    data = pd.read_csv(BASE_DIR / "etl_analysis.csv", parse_dates=['Date'])

    total_services = data['Services'].sum()
    total_visits = data['Visits'].sum()
    total_rejected = data['Rejected'].sum()
    total_time = data['Time'].sum()
    total_approved = total_services - total_rejected

    # Display KPIs
    st.subheader("Key Metrics")

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Services", total_services)
    col2.metric("Total Visits", total_visits)
    col3.metric("Total Time (sec)", total_time.round(2))

    col4, col5, col6 = st.columns(3)
    col4.metric("Avg Time / Service", (total_time / total_services).round(2))
    col5.metric("Avg Time / Visit", (total_time / total_visits).round(2))
    col6.empty()  # placeholder to balance the layout

    # Line charts for services, visits, time over time
    line_data = data[['Date', 'Services', 'Visits', 'Time']].set_index('Date')
    st.line_chart(line_data, use_container_width=True)

    # Bar charts for approved/rejected
    st.subheader("Predictions")
    bar_df = pd.DataFrame({
        'Prediction': ['Approved', 'Rejected'],
        'Count': [total_approved, total_rejected]
    })
    st.bar_chart(bar_df.set_index('Prediction'))

    # Descriptive statistics
    st.subheader("Descriptive Statistics")
    st.write(data[['Services', 'Visits', 'Time', 'Rejected']].describe())