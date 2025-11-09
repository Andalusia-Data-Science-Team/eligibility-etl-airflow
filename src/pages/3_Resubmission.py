# pages/3_Resubmission.py
from __future__ import annotations

import base64
from pathlib import Path
from datetime import datetime
from typing import Optional, List

import pandas as pd
import streamlit as st

# --- path bootstrap: ensure project root (parent of "src") on sys.path ---
import sys
_THIS = Path(__file__).resolve()
candidates: List[Path] = []
if _THIS.parent.name == "pages" and _THIS.parent.parent.name == "src":
    candidates += [_THIS.parents[3], _THIS.parents[2], _THIS.parents[1]]
else:
    candidates += [_THIS.parents[2], _THIS.parents[1], _THIS.parents[0]]
for p in candidates:
    if p and str(p) not in sys.path:
        sys.path.append(str(p))
# --- end bootstrap ---

from src.db import read_sql, cached_read
from src.components import kpi_row, info_box
from src.export import render_docx, render_pdf
from src.resubmission_engine import transform_loop  # used by Manual/Live tabs only

# ---------------- Page Setup ----------------
st.set_page_config(page_title="Resubmission", page_icon="💊", layout="wide")

# ---------- Locate logo ----------
def _resolve_logo() -> Optional[Path]:
    here = Path(__file__).resolve().parent
    for p in [
        here / "assets" / "your_company_logo.png",
        here.parent / "assets" / "your_company_logo.png",
        here.parents[2] / "assets" / "your_company_logo.png",
        Path.cwd() / "assets" / "your_company_logo.png",
    ]:
        if p.exists():
            return p
    return None

def _to_data_uri(p: Path) -> str:
    return "data:image/png;base64," + base64.b64encode(p.read_bytes()).decode("utf-8")

_LOGO_PATH = _resolve_logo()
_LOGO_URI = _to_data_uri(_LOGO_PATH) if _LOGO_PATH else ""

st.markdown("""
<style>
.header-logo img {
    max-width: 170px;
    height: auto;
    mix-blend-mode: multiply;     /* makes white blend with background */
    background-color: transparent !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Palette / CSS (same look as Predictions) ----------
PALETTE = {
    "bg": "#f8fafc",
    "paper": "#f8f5f2",
    "brand": "#7c4c24",
    "brand_hover": "#9a6231",
    "muted": "#64748b",
}

st.markdown(
    f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{
  background: {PALETTE['bg']};
}}
.block-container {{ max-width: 1280px; margin: 0 auto; }}

/* Header */
.header-box {{
  display:flex; align-items:center; justify-content:space-between;
  background:{PALETTE['paper']};
  border:1px solid rgba(0,0,0,0.06);
  border-radius:16px;
  box-shadow:0 2px 6px rgba(0,0,0,0.06);
  padding:1.1rem 1.4rem;
  margin-bottom:.75rem;
}}
.header-title h1 {{
  margin:0; color:{PALETTE['brand']}; font-weight:800; font-size:2rem;
}}
.header-title p {{
  margin:.35rem 0 0; color:{PALETTE['muted']}; font-size:.95rem;
}}
.header-logo img {{ max-width:170px; height:auto; display:block; }}

/* Tabs – remove Streamlit red underline, use brand brown */
.stTabs [data-baseweb="tab-list"] {{
  gap: 8px; border-bottom: 1px solid rgba(0,0,0,.08);
}}
.stTabs [data-baseweb="tab"] {{
  position: relative;
  height: 46px;
  padding: 10px 16px;
  border-radius: 999px 999px 0 0;
  background: #fff;
  border:1px solid rgba(0,0,0,.06);
  border-bottom: none;
  color: {PALETTE['brand']};
}}
.stTabs [data-baseweb="tab"]::after {{ content:""; position:absolute; left:14px; right:14px; bottom:-1px; height:3px; background:transparent; border-radius:2px; }}
.stTabs [data-baseweb="tab-highlight"] {{ background-color: transparent !important; border-bottom:none !important; }}
.stTabs [data-baseweb="tab"][aria-selected="true"]::after {{ background:{PALETTE['brand']}!important; }}
.stTabs [data-baseweb="tab"][aria-selected="true"] {{ box-shadow:0 -4px 12px rgba(0,0,0,.08); border-color:{PALETTE['brand']}; color:{PALETTE['brand']}; }}

/* Buttons */
.stButton > button {{
  background:{PALETTE['brand']}!important;
  color:#fff!important;
  border-radius:12px!important;
  font-weight:700!important;
  border:1px solid {PALETTE['brand']}!important;
}}
.stButton > button:hover {{
  background:{PALETTE['brand_hover']}!important;
  border-color:{PALETTE['brand_hover']}!important;
}}

/* Cards */
.surface-card {{
  background:#fff;
  border:1px solid rgba(0,0,0,.06);
  border-radius:14px;
  box-shadow:0 2px 6px rgba(0,0,0,.04);
  padding:.9rem 1.1rem;
  margin-bottom:.5rem;
}}

/* Footer */
.footer {{ text-align:center; color:#94a3b8; margin-top:1.2rem; }}
</style>
""",
    unsafe_allow_html=True,
)

# ---------- Header ----------
st.markdown(
    f"""
<div class="header-box">
  <div class="header-title">
    <h1>Resubmission Copilot</h1>
  </div>
  <div class="header-logo">
    {"<img src='" + _LOGO_URI + "' alt='Andalusia Health Logo'/>" if _LOGO_URI else ""}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

# ---------- Paths ----------
HERE = Path(__file__).resolve()
ROOT = HERE.parents[2] if (HERE.parent.name == "pages" and HERE.parents[1].name == "src") else HERE.parents[1]
SQL_DIR = ROOT / "sql"

# ---------- Helpers ----------
class UiLogger:
    def info(self, msg): st.info(str(msg))
    def warning(self, msg): st.warning(str(msg))
    def debug(self, msg): st.caption(str(msg))
ui_logger = UiLogger()

def fetch_single_visit_from_replica(visit_id: str) -> pd.DataFrame:
    base = (SQL_DIR / "resubmission.sql").read_text()
    # add WHERE VisitID = :vid if missing
    import re as _re
    sql = base.strip().rstrip(";")
    visit_col = "REQ.VisitID" if _re.search(r"\bREQ\.VisitID\b", sql, flags=_re.I) else "VisitID"
    if _re.search(r"\bWHERE\b", sql, flags=_re.I):
        sql += f"\nAND {visit_col} = :vid"
    else:
        sql += f"\nWHERE {visit_col} = :vid"
    return read_sql("REPLICA", sql, {"vid": str(visit_id)})

def run_llm_and_show(df: pd.DataFrame, visit_id: str):
    with st.spinner(f"Generating justifications for Visit {visit_id}…"):
        result = transform_loop(df, ui_logger)
    if result is None or result.empty:
        info_box("LLM produced no rows."); return
    st.subheader("Justification Result")
    st.dataframe(result, use_container_width=True)
    c1, c2 = st.columns(2)
    with c1:
        st.download_button(
            "⬇ Download PDF",
            data=render_pdf(result, str(visit_id)),
            file_name=f"resubmission_{visit_id}.pdf",
            mime="application/pdf",
        )


# ---------- TABS ----------
tab_bi, tab_manual, tab_live = st.tabs(
    [" BI rejection", "Get Justifications", " Live source"]
)

# === TAB 1: BI rejected — show existing BI justifications, no LLM ===
with tab_bi:
    st.caption("Load BI output and view/export the existing justification for a specific VisitID.")
    c1, c2 = st.columns([1.6, 1])
    with c1:
        bi_visit_filter = st.text_input("Filter by Visit ID (optional)")
    with c2:
        limit_rows = st.number_input("Max rows", min_value=100, max_value=100000, value=100)
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("Load", key="bi-load"):
        q = f"""
            SELECT TOP {int(limit_rows)}
                VisitID, VisitServiceID, Service_Name, Reason, Justification,
                ContractorEnName, VisitStartDate, VisitClassificationEnName
            FROM dbo.Resubmission_Copilot
            WHERE 1=1
        """
        params = {}
        if bi_visit_filter.strip():
            q += " AND VisitID = :v"
            params["v"] = bi_visit_filter.strip()
        q += " ORDER BY VisitServiceID"
        try:
            df_bi = cached_read("BI", q, params)
            st.session_state["_bi_df"] = df_bi
            if df_bi.empty:
                info_box("No rows matched your filters.")
            else:
                kpi_row([("Rows", len(df_bi)), ("Visits", df_bi["VisitID"].astype(str).nunique())])
                st.dataframe(df_bi, use_container_width=True)
        except Exception as e:
            st.error(f"Could not reach BI: {e}")

    df_bi = st.session_state.get("_bi_df")

                    

# === TAB 2: Manual single visit (REPLICA + LLM) ===
# === TAB 2: Manual single visit — prefer BI if available; else REPLICA+LLM ===
with tab_manual:
    st.caption(
        "Enter a VisitID. If it exists in the loaded BI table, we'll show the existing BI justification. "
        "Otherwise we’ll fetch from REPLICA and run the LLM for that one visit."
    )
    vcol, chkcol = st.columns([2, 1])
    with vcol:
        manual_vid = st.text_input("Visit ID (required)", placeholder="e.g., 488746")
    with chkcol:
        show_src = st.checkbox("Show fetched source (if using REPLICA)", value=False)
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("Get result for this visit", key="manual_btn"):
        vid = manual_vid.strip()
        if not vid:
            st.error("Please enter a Visit ID.")
        else:
            # 1) Try BI first (if the user loaded BI on tab 1 this session)
            bi_df = st.session_state.get("_bi_df")
            if (
                isinstance(bi_df, pd.DataFrame)
                and not bi_df.empty
                and "VisitID" in bi_df.columns
                and vid in bi_df["VisitID"].astype(str).values
            ):
                st.success(f"Found Visit {vid} in BI history. Showing existing justification.")
                sub = bi_df[bi_df["VisitID"].astype(str) == vid]
                # Show the existing BI justification
                show_cols = [c for c in ["VisitID", "VisitServiceID", "Service_Name", "Reason", "Justification"] if c in sub.columns]
                st.dataframe(sub[show_cols], use_container_width=True)

             
            else:
                # 2) Not in BI → fall back to REPLICA + LLM on the single visit
                try:
                    src = fetch_single_visit_from_replica(vid)
                    if not src.empty and "VisitID" in src.columns:
                        src = src[src["VisitID"].astype(str) == vid]  # hard guard, single-visit only
                    if src.empty:
                        info_box("No rows found on REPLICA for that Visit.")
                    else:
                        if show_src:
                            st.dataframe(src, use_container_width=True)
                        run_llm_and_show(src, vid)
                except Exception as e:
                    st.error(f"Replica error: {e}")

# === TAB 3: Live window (REPLICA + LLM) ===
with tab_live:
    st.caption("Load the REPLICA window and choose a VisitID to resubmit.")
    if st.button("Load REPLICA window", key="live_load_btn"):
        try:
            base_sql = (SQL_DIR / "resubmission.sql").read_text().strip().rstrip(";")
            live_df = read_sql("REPLICA", base_sql, {})
            st.session_state["_resub_live_df"] = live_df
            if live_df.empty:
                info_box("No live rows in the current window.")
            else:
                kpi_row([
                    ("Rows", len(live_df)),
                    ("Visits", live_df["VisitID"].astype(str).nunique() if "VisitID" in live_df.columns else 0),
                ])
                st.dataframe(live_df.head(200), use_container_width=True)
        except Exception as e:
            st.error(f"Failed to load REPLICA window: {e}")

    live_df = st.session_state.get("_resub_live_df")
    if isinstance(live_df, pd.DataFrame) and not live_df.empty:
        st.markdown("---")
        st.subheader("Run on a live VisitID")

        # Enter any VisitID (free text)
        free_live_vid = st.text_input("Enter Visit ID from the live window", key="live_free_vid")
        if st.button("Create Justification", key="live_free_btn"):
            if not free_live_vid.strip():
                st.error("Please enter a Visit ID.")
            else:
                try:
                    src = fetch_single_visit_from_replica(free_live_vid.strip())
                    if not src.empty and "VisitID" in src.columns:
                        src = src[src["VisitID"].astype(str) == free_live_vid.strip()]
                    if src.empty:
                        info_box("No rows found on REPLICA for that Visit.")
                    else:
                        run_llm_and_show(src, free_live_vid.strip())
                except Exception as e:
                    st.error(f"Run failed: {e}")

        
# ---------- Footer ----------
st.markdown(
    f'<p class="footer">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)