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
                df = _pd.read_csv(
                    p,
                    dtype=str,
                    keep_default_na=False,
                    encoding="utf-8",
                    on_bad_lines="skip",
                )
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

    # 1) If valid ICD10 is given, trust it
    if icd10 and icd10 in code_to_name:
        return icd10, code_to_name[icd10]

    # 2) Try to resolve from diagnosis name
    if dx:
        key = " ".join(dx.lower().split())

        # exact / normalized match
        if key in name_to_code:
            c = name_to_code[key]
            return c, code_to_name.get(c, dx)

        # loose "contains" match
        match = next(
            (c for c, name in code_to_name.items() if key and key in name.lower()),
            None,
        )
        if match:
            return match, code_to_name[match]

    # 3) Fallback: unknown, return what the user typed
    return icd10, dx


# --- Callbacks specifically for the Manual Entry tab (ICD10 <-> Diagnosis sync) ---

# Make sure the resub keys exist in session_state
for _k in ("dx_name_resub", "icd10_resub"):
    if _k not in st.session_state:
        st.session_state[_k] = ""

def _on_dx_resub():
    """When Diagnosis Name changes, auto-fill ICD10 using the full ICD10 table."""
    name = (st.session_state.get("dx_name_resub") or "").strip()
    if not name:
        return
    code, resolved_name = resolve_icd10_pair("", name)
    if code:
        st.session_state["icd10_resub"] = code
    if resolved_name:
        st.session_state["dx_name_resub"] = resolved_name

def _on_icd_resub():
    """When ICD10 changes, auto-fill Diagnosis Name using the full ICD10 table."""
    code = (st.session_state.get("icd10_resub") or "").strip()
    if not code:
        return
    resolved_code, name = resolve_icd10_pair(code, "")
    if resolved_code:
        st.session_state["icd10_resub"] = resolved_code
    if name:
        st.session_state["dx_name_resub"] = name

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
tab_manual_entry, tab_csv_validate = st.tabs(
    [" Manual Entry", " Validate on CSV"]
)

with tab_manual_entry:
    st.caption("Enter the required fields manually and create a justification.")

    # Rejection reasons: show description in dropdown, keep code internally
    REASONS = {
        "MN-1-1": "Service is not clinically justified based on clinical practice guideline, without additional supporting diagnosis",
        "AD-1-4": "Diagnosis is inconsistent with service/procedure",
        "AD-3-5": "Diagnosis is inconsistent with patient's age",
    }
    reason_labels = ["(choose…)", *[REASONS[k] for k in ["MN-1-1", "AD-1-4", "AD-3-5"]]]

    # --- Inputs ---
    c1, c2 = st.columns(2)

    with c1:
        service_name = st.text_input("Service Name (required)", placeholder="e.g., Ferritin")
        dx_name = st.text_input(
            "Diagnosis Name (required)",
            key="dx_name_resub",
            placeholder="e.g., Iron deficiency anemia",
            on_change=_on_dx_resub,
        )
        chief_complaint = st.text_input("Chief Complaint (required)", placeholder="e.g., Fatigue and pallor")
        age = st.number_input("Age", min_value=0, value=30, step=1)

    with c2:
        icd10 = st.text_input(
            "ICD10 Code (required)",
            key="icd10_resub",
            placeholder="e.g., D50.9",
            on_change=_on_icd_resub,
        )
        symptoms = st.text_input("Symptoms (optional)", placeholder="comma-separated…")
        reason_choice = st.selectbox("Rejection Reason (description)",options=reason_labels, index=0)

    # Submit button
    run_just = st.button("Run Justification", key="run_manual_just")

    if run_just:
        # Read the *current* mapped values from session_state
        dx_input = st.session_state.get("dx_name_resub", dx_name)
        icd10_input = st.session_state.get("icd10_resub", icd10)

        # Final ICD10 consistency
        icd10_final, dx_final = resolve_icd10_pair(icd10_input, dx_input)

        # --- Validate required fields ---
        missing = []
        if not service_name.strip():
            missing.append("Service Name")
        if not dx_final.strip():
            missing.append("Diagnosis Name / ICD10")
        if not icd10_final.strip():
            missing.append("ICD10")
        if not chief_complaint.strip():
            missing.append("Chief Complaint")
        if age is None:
            missing.append("Age")

        # Map chosen description -> code
        selected_code = ""
        reason_merged = ""

        if reason_choice == "(choose…)":
            missing.append("Rejection Reason")

        elif reason_choice in REASONS.values():
            # They selected a known code
            for code, desc in REASONS.items():
                if desc == reason_choice:
                    selected_code = code
                    reason_merged = desc
                    break

        elif reason_choice == "Other (free text)":
            missing.append("Other reason is not supported now (free text removed — choose an official reason)")

        if missing:
            st.error("Please fill: " + ", ".join(missing))
        else:
            now = datetime.now()

            # ---------- INTERNAL full row for LLM ----------
            llm_row = {
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
                "VisitServiceID":       int(now.strftime("%H%M%S%f")),
                "Diagnosis":            dx_final,
                "ICD10":                icd10_final,
                "ProblemNote":          "",
                "Chief_Complaint":      chief_complaint.strip(),
                "Symptoms":             symptoms.strip(),
            }

            src_llm = pd.DataFrame([llm_row])

            # ---------- Run LLM ----------
            try:
                with st.spinner("Generating justification for the manual entry…"):
                    result = transform_loop(src_llm, ui_logger)
            except Exception as e:
                st.error(f"LLM pipeline failed: {e}")
                result = None

            if result is None or result.empty:
                info_box("LLM produced no rows.")
            else:
                needed_cols = [
                    "Age",
                    "Service_Name",
                    "Status",
                    "Reason",
                    "Diagnosis",
                    "ICD10",
                    "Chief_Complaint",
                    "Symptoms",
                ]
                display_cols = [c for c in needed_cols if c in result.columns]
                if "Justification" in result.columns:
                    display_cols.append("Justification")

                merged = result[display_cols].copy()

                st.subheader("Justification Result")
                st.dataframe(merged, width="stretch")

                c1, _ = st.columns(2)
                with c1:
                    st.download_button(
                        "⬇ Download PDF",
                        data=render_pdf(merged, "MANUAL"),
                        file_name="resubmission_manual.pdf",
                        mime="application/pdf",
                    )


# === TAB 2: Validate on CSV (upload CSV → pick VisitID → CSV + LLM) ===
with tab_csv_validate:

    uploaded_file = st.file_uploader(
        "Upload CSV (first column should contain VisitID or have a VisitID column)",
        type=["csv"],
        key="csv_validate_uploader",
    )

    df_ids = pd.DataFrame()

    if uploaded_file is not None:
        try:
            # Load full CSV – this is now the ONLY source for LLM
            df_ids = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
        except Exception as e:
            st.error(f"Could not read CSV: {e}")
            df_ids = pd.DataFrame()

    if df_ids.empty:
        info_box("Upload a CSV above to begin.")
    else:
        # Decide which column to treat as VisitID
        visit_col = "VisitID" if "VisitID" in df_ids.columns else df_ids.columns[0]
        visit_choices = sorted(df_ids[visit_col].astype(str).unique().tolist())

        pick_col, opt_col = st.columns([2, 1])
        with pick_col:
            sel_visit = st.selectbox(
                f"Select Visit ID from '{visit_col}' column",
                options=visit_choices,
                key="csv_visit_select",
            )
        
        st.markdown('</div>', unsafe_allow_html=True)

        if st.button("Run Validation", key="csv_run_validation_btn"):
            vid = sel_visit.strip()
            if not vid:
                st.error("Please select a Visit ID.")
            else:
                # 1) Extract visit rows from the uploaded CSV itself
                src = df_ids[df_ids[visit_col].astype(str) == vid].copy()

                if src.empty:
                    info_box("No rows found in CSV for that Visit.")
                else:
                    # IMPORTANT: normalize VisitServiceID dtype to avoid int64/object merge error
                    if "VisitServiceID" in src.columns:
                        # Coerce to numeric so LLM/output side (usually numeric) matches
                        src["VisitServiceID"] = pd.to_numeric(src["VisitServiceID"], errors="coerce")


                    # 2) LLM processing
                    try:
                        with st.spinner(f"Generating justifications for Visit {vid}…"):
                            result = transform_loop(src, ui_logger)
                    except Exception as e:
                        st.error(f"LLM pipeline failed: {e}")
                        result = None

                    # 3) Merge LLM output back onto the CSV rows
                    merged = src.copy()
                    base_cols = list(src.columns)  # preserve original CSV order

                    def _pick(df, names):
                        for n in names:
                            if n in df.columns:
                                return n
                        return None

                    # Attach Justification by VisitServiceID / Service_id / ItemId if present
                    if isinstance(result, pd.DataFrame) and not result.empty:
                        key_src = _pick(src, ["VisitServiceID", "Service_id", "ItemId", "service_id"])
                        key_res = _pick(result, ["VisitServiceID", "Service_id", "ItemId", "service_id"])
                        just_col = _pick(result, ["Justification", "justification"])

                        if key_src and key_res and just_col:
                            right = (
                                result[[key_res, just_col]]
                                .rename(columns={key_res: "__key__", just_col: "Justification"})
                                .drop_duplicates()
                            )

                            # Make sure merge key is comparable
                            merged["__key__"] = merged[key_src].astype(str)
                            right["__key__"] = right["__key__"].astype(str)

                            merged = merged.merge(right, on="__key__", how="left")
                            merged.drop(columns="__key__", inplace=True, errors="ignore")
                        else:
                            merged["Justification"] = ""
                    else:
                        merged["Justification"] = ""

                    # 4) Merge Reason + ResponseReasonCode → human-friendly Reason
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

                    # 5) Keep original CSV column order; append Justification last
                    extras = [c for c in merged.columns if c not in base_cols and c != "Justification"]
                    final_cols = base_cols + extras + (["Justification"] if "Justification" in merged.columns else [])
                    merged = merged.reindex(columns=final_cols)

                    # 6) Show + Download
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

# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)
