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

# ---------- ICD10 loader (shared) ----------
from functools import lru_cache

# put the CSV under your repo's /data if you like; keep a fallback to /mnt/data
ICD10_PATHS = [
    Path(__file__).resolve().parents[2] / "data" / "idc10_disease_full_data.csv",
    Path("/mnt/data/idc10_disease_full_data.csv"),
]

@lru_cache(maxsize=1)
def load_icd10_maps():
    """Return (code->name, name->code) using the full ICD10 CSV."""
    import pandas as _pd
    df = None
    errs = []
    for p in ICD10_PATHS:
        try:
            if p.exists():
                df = _pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8", on_bad_lines="skip")
                break
        except Exception as e:
            errs.append(f"{p}: {e}")
    if df is None or df.empty:
        st.warning("ICD10 table not found/empty. Free-text will be used.")
        return {}, {}

    # CSV columns: diseaseCode, diseaseDescription, (…)
    code_col = "diseaseCode" if "diseaseCode" in df.columns else df.columns[0]
    name_col = "diseaseDescription" if "diseaseDescription" in df.columns else df.columns[1]

    df[code_col] = df[code_col].astype(str).str.strip().str.upper()
    df[name_col] = df[name_col].astype(str).str.strip()

    # main maps
    code_to_name = {c: n for c, n in zip(df[code_col], df[name_col]) if c}
    # make the name lookup more forgiving
    def _norm_name(s: str) -> str:
        return " ".join(str(s).lower().split())

    name_to_code = {}
    for c, n in code_to_name.items():
        name_to_code.setdefault(_norm_name(n), c)
    # also add a few alias forms without punctuation
    import re as _re
    for n, c in list(name_to_code.items()):
        alias = _re.sub(r"[^a-z0-9 ]+", "", n)
        if alias and alias not in name_to_code:
            name_to_code[alias] = c

    return code_to_name, name_to_code

def resolve_icd10_pair(icd10_input: str, dx_input: str):
    """
    Given either/both of (icd10 code, diagnosis name) return a consistent (code, name).
    Prefers explicit ICD10 if both provided.
    """
    code_to_name, name_to_code = load_icd10_maps()

    icd10 = (icd10_input or "").strip().upper()
    dx    = (dx_input  or "").strip()

    if icd10 and icd10 in code_to_name:
        return icd10, code_to_name[icd10]

    if dx:
        key = " ".join(dx.lower().split())
        # try exact/normalized name
        if key in name_to_code:
            c = name_to_code[key]
            return c, code_to_name.get(c, dx)
        # try a loose contains match on descriptions (fallback)
        match = next((c for c, name in code_to_name.items() if key and key in name.lower()), None)
        if match:
            return match, code_to_name[match]

    # if we get here: unknown; pass through raw user text
    return icd10, dx

# ===== Manual-entry helpers (diagnosis <-> ICD10 mapping) =====
ICD10_MAP = {
    "D50.9": "Iron deficiency anemia, unspecified",
    "G43.9": "Migraine, unspecified",
    "J06.9": "Acute upper respiratory infection, unspecified",
    "E11.9": "Type 2 diabetes mellitus without complications",
    "I10":   "Essential (primary) hypertension",
}
DX_TO_ICD = {v.lower(): k for k, v in ICD10_MAP.items()}

# Initialize state keys for sync
for _k, _v in {
    "dx_name": "",
    "icd10": "",
    "_last_filled": "",
}.items():
    if _k not in st.session_state:
        st.session_state[_k] = _v

def _on_dx_change():
    """Auto-fill ICD10 when Diagnosis Name matches known mapping"""
    name = (st.session_state.get("dx_name") or "").strip().lower()
    code = DX_TO_ICD.get(name)
    if code:
        st.session_state.icd10 = code
        st.session_state._last_filled = "dx"

def _on_icd_change():
    """Auto-fill Diagnosis Name when ICD10 matches known mapping"""
    code = (st.session_state.get("icd10") or "").strip().upper()
    name = ICD10_MAP.get(code)
    if name:
        st.session_state.dx_name = name
        st.session_state._last_filled = "icd"

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
    [" Rejection", "Get Justifications", " Live source"]
)

# === TAB 1: BI rejected — show existing BI justifications, no LLM ===
with tab_bi:
    st.caption("Load output and view/export the existing justification for a specific VisitID.")
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

# === TAB 3: Live source (manual entry; no DB) ===
with tab_live:
    st.caption("Enter the required fields manually and create a justification.")

    # Rejection reasons: show description in dropdown, keep code internally
    REASONS = {
        "MN-1-1": "Service is not clinically justified based on clinical practice guideline, without additional supporting diagnosis",
        "AD-1-4": "Diagnosis is inconsistent with service/procedure",
        "AD-3-5": "Diagnosis is inconsistent with patient's age",
    }
    reason_labels = ["(choose…)", *[REASONS[k] for k in ["MN-1-1","AD-1-4","AD-3-5"]], "Other (free text)"]

    with st.form("live_manual_resub"):
        c1, c2 = st.columns(2)

        with c1:
            service_name = st.text_input("Service Name (required)", placeholder="e.g., Ferritin")
            dx_name      = st.text_input("Diagnosis Name (required)", key="dx_name_resub", placeholder="e.g., Iron deficiency anemia")
            chief_complaint = st.text_input("Chief Complaint (required)", placeholder="e.g., Fatigue and pallor")
            qty          = st.number_input("Quantity", min_value=1, value=1, step=1)
            age          = st.number_input("Age", min_value=0, value=30, step=1)

        with c2:
            icd10        = st.text_input("ICD10 Code (required)", key="icd10_resub", placeholder="e.g., D50.9")
            symptoms     = st.text_input("Symptoms (optional)", placeholder="comma-separated…")
            reason_choice = st.selectbox("Rejection Reason (description)", options=reason_labels, index=0)
            free_reason  = st.text_input("Reason (optional if code chosen)", placeholder="If you picked a code, this can be empty")

        run_just = st.form_submit_button("Run Justification")

    if run_just:
        # Map chosen description -> code (if any)
        selected_code = ""
        if reason_choice and reason_choice != "(choose…)" and reason_choice != "Other (free text)":
            # reverse lookup
            for code, desc in REASONS.items():
                if desc == reason_choice:
                    selected_code = code
                    break

        # Resolve ICD10 <-> Diagnosis
        icd10_final, dx_final = resolve_icd10_pair(icd10, dx_name)

        # Validate
        missing = []
        if not service_name.strip(): missing.append("Service Name")
        if not dx_final.strip():     missing.append("Diagnosis Name / ICD10")
        if not icd10_final.strip():  missing.append("ICD10")
        if not chief_complaint.strip(): missing.append("Chief Complaint")
        if age is None: missing.append("Age")
        if qty is None: missing.append("Quantity")
        if not selected_code and not free_reason.strip():
            missing.append("Rejection Reason (pick a code or type a reason)")

        if missing:
            st.error("Please fill: " + ", ".join(missing))
        else:
            now = datetime.now()
            reason_text = REASONS.get(selected_code, "").strip()
            # If user typed something, append it (or use it if no code)
            if free_reason.strip():
                reason_merged = f"{reason_text} — {free_reason.strip()}" if reason_text else free_reason.strip()
            else:
                reason_merged = reason_text

            # Build one-row DF for transform_loop()
            row = {
                "RequestTransactionID": f"MAN-{now.strftime('%Y%m%d%H%M%S')}",
                "VisitID":              f"MAN-{now.strftime('%Y%m%d')}",
                "VisitStartDate":       now,
                "UpdatedDate":          now,
                "StatementId":          0,
                "Gender":               "Unknown",
                "Age":                  int(age),
                "ContractorEnName":     "Manual",
                "VisitClassificationEnName": "Manual Entry",
                "Sequence":             1,
                "Service_id":           0,
                "Service_Name":         service_name.strip(),
                "Status":               "Rejected",
                "Note":                 "",
                "Reason":               reason_merged,
                "ResponseReasonCode":   selected_code or "Manual",
                "VisitServiceID":       f"VS-{now.strftime('%H%M%S%f')}",
                "Diagnosis":            dx_final,
                "ICD10":                icd10_final,
                "ProblemNote":          "",
                "Chief_Complaint":      chief_complaint.strip(),
                "Symptoms":             symptoms.strip(),
            }
            src = pd.DataFrame([row])

            with st.spinner("Generating justification for the manual entry…"):
                result = transform_loop(src, ui_logger)

            if result is None or result.empty:
                info_box("LLM produced no rows.")
            else:
                # Keep the same columns as our src, add Justification at the end
                merged = src.copy()
                # attach Justification by VisitServiceID if present
                key_src = "VisitServiceID" if "VisitServiceID" in merged.columns else None
                key_res = next((k for k in ["VisitServiceID","Service_id","ItemId","service_id"] if k in result.columns), None)
                just_col = next((j for j in ["Justification","justification"] if j in result.columns), None)
                if key_src and key_res and just_col:
                    r = result[[key_res, just_col]].rename(columns={key_res: "__k__", just_col: "Justification"}).drop_duplicates()
                    merged["__k__"] = merged[key_src]
                    merged = merged.merge(r, on="__k__", how="left").drop(columns="__k__", errors="ignore")
                else:
                    merged["Justification"] = ""

                base_cols = list(src.columns)
                extras = [c for c in merged.columns if c not in base_cols and c != "Justification"]
                final_cols = base_cols + extras + (["Justification"] if "Justification" in merged.columns else [])
                merged = merged.reindex(columns=final_cols)

                st.subheader("Justification Result")
                st.dataframe(merged, use_container_width=True)

                c1, _ = st.columns(2)
                with c1:
                    st.download_button(
                        "⬇ Download PDF",
                        data=render_pdf(merged, merged.loc[0, "VisitID"]),
                        file_name=f"resubmission_{merged.loc[0, 'VisitID']}.pdf",
                        mime="application/pdf",
                    )
                    
# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)