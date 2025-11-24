#Streamlit UI helper functions
# src/components.py
import streamlit as st  
from typing import Union, List, Tuple

#Shows multiple KPIs in evenly spaced columns
def kpi_row(items: List[Tuple[str, Union[str, int, float]]]):
    cols = st.columns(len(items))
    for col, (label, value) in zip(cols, items):
        with col:
            st.metric(label, value)

#Shows a stylized info message with an icon using Markdown
def info_box(text: str, icon: str = "ℹ️"):
    st.markdown(f"> {icon} **{text}**")
