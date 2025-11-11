# pages/3_Resubmission.py
from __future__ import annotations
import base64
from pathlib import Path
from datetime import datetime
from typing import Optional, List
import re
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
# --- SQL safety helpers ---

def _normalize_cte(sql_text: str) -> str:
    """Ensure queries that begin with WITH are prefixed by a semicolon."""
    s = sql_text.lstrip()
    if re.match(r"^WITH\b", s, flags=re.I):
        # add ; before the first non-space WHEN the query starts with WITH
        leading = sql_text[:len(sql_text) - len(s)]
        return leading + ";" + s
    return sql_text

def _sanitize_sql(sql_text: str) -> str:
    """Fix common authoring artifacts that break SQL Server."""
    s = sql_text

    # 1) Remove accidental 'XX ' markers (seen before I.ResponseReason)
    s = re.sub(r"\bXX\s+(?=[A-Za-z_]\w*\.)", "", s)

    # 2) Replace Unicode em/en dashes with normal hyphen (don’t create comments)
    s = s.replace("—", "-").replace("–", "-")

    # 3) Normalize Windows smart quotes if present
    s = s.replace("’", "'").replace("“", '"').replace("”", '"')

    # 4) Keep CTEs safe
    s = _normalize_cte(s)

    return s

class UiLogger:
    def info(self, msg): st.info(str(msg))
    def warning(self, msg): st.warning(str(msg))
    def debug(self, msg): st.caption(str(msg))
ui_logger = UiLogger()

def fetch_single_visit_from_replica(visit_id: str) -> pd.DataFrame:
    raw = (SQL_DIR / "resubmission.sql").read_text(encoding="utf-8")
    sql = _sanitize_sql(raw)  # ✅ sanitize + CTE-safe

    # add WHERE VisitID = :vid if missing
    visit_col = "REQ.VisitID" if re.search(r"\bREQ\.VisitID\b", sql, flags=re.I) else "VisitID"
    if re.search(r"\bWHERE\b", sql, flags=re.I):
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

    if st.button("Load", key="bi-load"):
        q = f"""
            SELECT TOP {int(limit_rows)}
                VisitID, VisitServiceID, Service_Name, Reason, Justification,
                ContractorEnName, VisitStartDate, VisitClassificationEnName, ResponseReasonCode
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

            # ---- Merge Reason + ResponseReasonCode BEFORE displaying ----
            REASON_MAP = {
                "MN-1-1": "Service is not clinically justified based on clinical practice guideline, without additional supporting diagnosis",
                "AD-1-4": "Diagnosis is inconsistent with service/procedure",
                "AD-3-5": "Diagnosis is inconsistent with patient's age",
            }

            if not df_bi.empty:
                def _merge_reason(row):
                    code = (str(row.get("ResponseReasonCode", "")).strip() or "")
                    code_text = REASON_MAP.get(code, code) if code else ""
                    reason = (str(row.get("Reason", "")).strip() or "")

                    parts = []
                    if code_text:
                        parts.append(code_text)
                    if reason and reason.lower() not in code_text.lower():
                        parts.append(reason)
                    return " — ".join(p for p in parts if p) or reason or code_text

                df_bi["Reason"] = df_bi.apply(_merge_reason, axis=1)
                # Drop the original code column
                if "ResponseReasonCode" in df_bi.columns:
                    df_bi = df_bi.drop(columns=["ResponseReasonCode"])

                # Optional: put Reason right after Service_Name
                desired_order = [
                    "VisitID", "VisitServiceID", "Service_Name", "Reason","ContractorEnName", "VisitStartDate",
                    "VisitClassificationEnName","Justification"
                ]
                # keep any extras at the end
                cols = [c for c in desired_order if c in df_bi.columns] + \
                       [c for c in df_bi.columns if c not in desired_order]
                df_bi = df_bi[cols]

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
            # 1) Try BI first (if loaded in this session)
            bi_df = st.session_state.get("_bi_df")
            if (
                isinstance(bi_df, pd.DataFrame)
                and not bi_df.empty
                and "VisitID" in bi_df.columns
                and vid in bi_df["VisitID"].astype(str).values
            ):
                st.success(f"Found Visit {vid} in BI history. Showing existing justification.")
                sub = bi_df[bi_df["VisitID"].astype(str) == vid]
                show_cols = [c for c in ["VisitID", "VisitServiceID", "Service_Name", "Reason", "Justification"] if c in sub.columns]
                st.dataframe(sub[show_cols], use_container_width=True)

            else:
                # 2) Not in BI → REPLICA + LLM (and keep the REPLICA table layout)
                try:
                    src = fetch_single_visit_from_replica(vid)
                    if not src.empty and "VisitID" in src.columns:
                        src = src[src["VisitID"].astype(str) == vid]

                    if src.empty:
                        info_box("No rows found on REPLICA for that Visit.")
                    else:
                        if show_src:
                            st.dataframe(src, use_container_width=True)

                        # --- Run LLM on the same rows ---
                        with st.spinner(f"Generating justifications for Visit {vid}…"):
                            result = transform_loop(src, ui_logger)

                        # --- Start from the REPLICA table EXACTLY as-is ---
                        merged = src.copy()
                        base_cols = list(src.columns)  # preserve exact order

                        # Small helper to find first matching column name
                        def _pick(df, names):
                            for n in names:
                                if n in df.columns:
                                    return n
                            return None

                        # --- Attach LLM Justification by VisitServiceID/Service_id if present ---
                        if isinstance(result, pd.DataFrame) and not result.empty:
                            key_src = _pick(src,    ["VisitServiceID", "Service_id", "ItemId", "service_id"])
                            key_res = _pick(result, ["VisitServiceID", "Service_id", "ItemId", "service_id"])
                            just_col = _pick(result, ["Justification", "justification"])
                            if key_src and key_res and just_col:
                                right = (
                                    result[[key_res, just_col]]
                                    .rename(columns={key_res: "__key__", just_col: "Justification"})
                                    .drop_duplicates()
                                )
                                merged["__key__"] = merged[key_src]
                                merged = merged.merge(right, on="__key__", how="left")
                                merged.drop(columns="__key__", inplace=True, errors="ignore")
                            else:
                                merged["Justification"] = ""
                        else:
                            merged["Justification"] = ""

                        # --- Build user-friendly Reason from ResponseReasonCode + Reason (same mapping as Live tab) ---
                        REASON_MAP = {
                            "MN-1-1": "Service is not clinically justified based on clinical practice guideline, without additional supporting diagnosis",
                            "AD-1-4": "Diagnosis is inconsistent with service/procedure",
                            "AD-3-5": "Diagnosis is inconsistent with patient's age",
                        }
                        def _merge_reason(row):
                            code = str(row.get("ResponseReasonCode", "") or "").strip()
                            reason = str(row.get("Reason", "") or "").strip()
                            mapped = REASON_MAP.get(code, code) if code else ""
                            parts = []
                            if mapped:
                                parts.append(mapped)
                            if reason and reason.lower() not in mapped.lower():
                                parts.append(reason)
                            return " — ".join(parts) if parts else ""
                        if "Reason" in merged.columns or "ResponseReasonCode" in merged.columns:
                            merged["Reason"] = merged.apply(_merge_reason, axis=1)
                            merged.drop(columns=["ResponseReasonCode"], inplace=True, errors="ignore")

                        # --- Final columns: EXACTLY Replica columns + Justification LAST ---
                        extras = [c for c in merged.columns if c not in base_cols and c != "Justification"]
                        final_cols = base_cols + extras + (["Justification"] if "Justification" in merged.columns else [])
                        merged = merged.reindex(columns=final_cols)

                        # --- Show & Download ---
                        st.subheader("Justification Result")
                        st.dataframe(merged, use_container_width=True)

                        c1, _ = st.columns(2)
                        with c1:
                            st.download_button(
                                "⬇ Download PDF",
                                data=render_pdf(merged, str(vid)),
                                file_name=f"resubmission_{vid}.pdf",
                                mime="application/pdf",
                            )
                except Exception as e:
                    st.error(f"Replica error: {e}")

# === TAB 3: Live window (REPLICA + LLM) ===
with tab_live:
    st.caption("Load the REPLICA window and choose a VisitID to resubmit.")

    REASON_MAP = {
        "MN-1-1": "Service is not clinically justified based on clinical practice guideline, without additional supporting diagnosis",
        "AD-1-4": "Diagnosis is inconsistent with service/procedure",
        "AD-3-5": "Diagnosis is inconsistent with patient's age",
    }

    if st.button("Load REPLICA window", key="live_load_btn"):
        try:
            raw_sql = (SQL_DIR / "resubmission.sql").read_text(encoding="utf-8")
            base_sql = _sanitize_sql(raw_sql)
            live_df = read_sql("REPLICA", base_sql, {})

            if isinstance(live_df, pd.DataFrame) and not live_df.empty:
                # Merge Reason and ResponseReasonCode
                def _merge_reason(row):
                    code = str(row.get("ResponseReasonCode", "") or "").strip()
                    reason = str(row.get("Reason", "") or "").strip()
                    mapped = REASON_MAP.get(code, code) if code else ""
                    parts = []
                    if mapped:
                        parts.append(mapped)
                    if reason and reason.lower() not in mapped.lower():
                        parts.append(reason)
                    return " — ".join(parts) if parts else ""
                live_df["Reason"] = live_df.apply(_merge_reason, axis=1)
                live_df.drop(columns=["ResponseReasonCode"], inplace=True, errors="ignore")

            st.session_state["_resub_live_df"] = live_df

            if live_df.empty:
                info_box("No live rows in the current window.")
            else:
                kpi_row([
                    ("Rows", len(live_df)),
                    ("Visits", live_df["VisitID"].astype(str).nunique()
                     if "VisitID" in live_df.columns else 0),
                ])
                st.dataframe(live_df.head(200), use_container_width=True)

        except Exception as e:
            st.error(f"Failed to load REPLICA window: {e}")

    # --- Run LLM for a selected VisitID ---
    live_df = st.session_state.get("_resub_live_df")
    if isinstance(live_df, pd.DataFrame) and not live_df.empty:
        st.markdown("---")
        st.subheader("Run on a live VisitID")

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
                        with st.spinner(f"Generating justifications for Visit {free_live_vid}…"):
                            result = transform_loop(src, ui_logger)

                        merged = src.copy()

                        # Merge Justifications if result exists
                        if result is not None and not result.empty:
                            def _pick(df, names):
                                for n in names:
                                    if n in df.columns:
                                        return n
                                return None
                            key_src = _pick(src, ["VisitServiceID", "Service_id", "ItemId"])
                            key_res = _pick(result, ["VisitServiceID", "Service_id", "ItemId"])
                            if key_src and key_res:
                                just_col = _pick(result, ["Justification", "justification"])
                                if just_col:
                                    right = result.rename(
                                        columns={key_res: "__key__", just_col: "Justification"}
                                    )[["__key__", "Justification"]].drop_duplicates()
                                    merged["__key__"] = merged[key_src]
                                    merged = merged.merge(right, on="__key__", how="left")
                                    merged.drop(columns="__key__", inplace=True, errors="ignore")
                            else:
                                merged["Justification"] = ""
                        else:
                            merged["Justification"] = ""

                        # Merge Reason safely
                        def _merge_reason(row):
                            code = str(row.get("ResponseReasonCode", "") or "").strip()
                            reason = str(row.get("Reason", "") or "").strip()
                            mapped = REASON_MAP.get(code, code) if code else ""
                            parts = []
                            if mapped:
                                parts.append(mapped)
                            if reason and reason.lower() not in mapped.lower():
                                parts.append(reason)
                            return " — ".join(parts) if parts else ""

                        merged["Reason"] = merged.apply(_merge_reason, axis=1)
                        merged.drop(columns=["ResponseReasonCode"], inplace=True, errors="ignore")

                        # Preserve Replica column order
                        base_cols = [c for c in src.columns if c in merged.columns]
                        extras = [c for c in merged.columns if c not in base_cols and c != "Justification"]
                        final_cols = base_cols + extras + (["Justification"] if "Justification" in merged.columns else [])
                        merged = merged[final_cols]

                        # Display
                        st.subheader("Justification Result")
                        st.dataframe(merged, use_container_width=True)

                        c1, _ = st.columns(2)
                        with c1:
                            st.download_button(
                                "⬇ Download PDF",
                                data=render_pdf(merged, str(free_live_vid)),
                                file_name=f"resubmission_{free_live_vid}.pdf",
                                mime="application/pdf",
                            )

                except Exception as e:
                    st.error(f"Run failed: {e}")

# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)