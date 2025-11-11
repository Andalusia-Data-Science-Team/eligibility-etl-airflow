# src/db.py
from __future__ import annotations
import json
from pathlib import Path
import pandas as pd
import sqlalchemy as sa
import streamlit as st

# add near the bottom of src/db.py
@st.cache_data(ttl=120, show_spinner=False)  # 2 min cache
def cached_read(engine_key: str, query: str, params: dict | None = None):
    # Only pass params if it's not None and not empty
    if params:
        return read_sql(engine_key, query, params)
    else:
        return read_sql(engine_key, query)

HERE = Path(__file__).resolve()
ROOT = HERE.parents[1]  # repo root (parent of src)
PASSCODE_PATH = ROOT / "passcode.json"

@st.cache_resource(show_spinner=False)
def get_engines():
    """Return dict of SQLAlchemy engines. Prefer Streamlit secrets; fallback to passcode.json."""
    cfg = None

    # 1) Try Streamlit secrets (prod)
    try:
        cfg = st.secrets["DB_NAMES"]  # will raise if no secrets.toml or key missing
    except Exception:
        # 2) Fallback to passcode.json (dev)
        if not PASSCODE_PATH.exists():
            raise FileNotFoundError(
                "No DB config found. Either add `.streamlit/secrets.toml` with [DB_NAMES] "
                "or provide passcode.json at the repo root."
            )
        with PASSCODE_PATH.open("r", encoding="utf-8") as f:
            cfg = json.load(f)["DB_NAMES"]

    def mk_engine(pc: dict) -> sa.Engine:
        # Build a reliable ODBC connect string
        driver = pc.get("driver", "{ODBC Driver 17 for SQL Server}")
        server = pc["Server"]                  # add ",1433" if your server uses a non-default port
        database = pc["Database"]
        uid = pc["UID"]
        pwd = pc["PWD"]

        odbc = (
            f"DRIVER={driver};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={uid};PWD={pwd};"
            "Encrypt=no;"                 # change to 'yes;' if your server requires encryption
            "TrustServerCertificate=yes;" # helps avoid CA issues if Encrypt=yes
            "MARS_Connection=Yes;"
            "Connection Timeout=15;"
        )
        url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": odbc})
        return sa.create_engine(url, pool_pre_ping=True, pool_recycle=1800)

    return {
        "BI": mk_engine(cfg["BI"]),
        "AI": mk_engine(cfg["AI"]),
        "LIVE": mk_engine(cfg["LIVE"]),
        # Prefer 'Replica' entry, then 'AHJ_DOT-CARE', else fall back to LIVE
        "REPLICA": mk_engine(cfg.get("Replica", cfg.get("AHJ_DOT-CARE", cfg["LIVE"]))),
    }

def read_sql(engine_key: str, query: str, params: dict | None = None) -> pd.DataFrame:
    eng = get_engines()[engine_key]
    # Only pass params if it's not None and not empty
    if params and any(f":{k}" in query for k in params.keys()):
        # Use SQLAlchemy text() to properly handle named parameters
        from sqlalchemy.sql import text
        return pd.read_sql(text(query), eng, params=params)
    elif params:
        # For positional parameters
        return pd.read_sql(query, eng, params=tuple(params.values()))
    else:
        # No parameters
        return pd.read_sql(query, eng)