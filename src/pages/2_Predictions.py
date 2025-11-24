# pages/2_Predictions.py
from __future__ import annotations
import base64
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Tuple, List
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

from src.components import kpi_row, info_box
from src.AHJ_Claims import make_preds
from src.icd10_utils import load_icd10_maps, resolve_icd10_pair

# ---------- small helpers ----------
def looks_numeric(text: str) -> bool:
    """True if the string is only digits (optionally with one decimal point)."""
    s = (text or "").strip()
    return bool(s) and bool(re.fullmatch(r"[0-9]+(\.[0-9]+)?", s))


def resolve_icd10_multi(icd_input: str, dx_input: str) -> Tuple[str, str]:
    """
    Handle multiple ICD10 codes / diagnoses separated by commas.
    Uses resolve_icd10_pair() per element and then joins back.
    Example:
      icd_input = 'E11,E78.2,D50,D51'
      dx_input  = ''  (or matching count of names)
    """
    icd_input = icd_input or ""
    dx_input = dx_input or ""

    # Split codes on comma or whitespace
    raw_codes = [c.strip().upper() for c in re.split(r"[,\s]+", icd_input) if c.strip()]
    # Split names on comma only (to avoid breaking words too much)
    raw_names = [n.strip() for n in dx_input.split(",") if n.strip()]

    if not raw_codes and not raw_names:
        return "", ""

    n = max(len(raw_codes), len(raw_names), 1)
    out_codes: List[str] = []
    out_names: List[str] = []

    for i in range(n):
        c = raw_codes[i] if i < len(raw_codes) else ""
        d = raw_names[i] if i < len(raw_names) else ""
        c_res, d_res = resolve_icd10_pair(c, d)

        # Prefer resolved values, fall back to what user typed
        final_c = c_res or c
        final_d = d_res or d

        if final_c:
            out_codes.append(final_c)
        if final_d:
            out_names.append(final_d)

    # Join back as comma-separated lists
    codes_str = ",".join(dict.fromkeys(out_codes))  # dict.fromkeys to keep order but drop duplicates
    names_str = ", ".join(out_names)

    return codes_str, names_str


# ================= Streamlit Page =================
# ---------------- Page Setup ----------------
st.set_page_config(page_title="Predictions", page_icon="💊", layout="wide")

# ---------- Locate logo ----------
def _resolve_logo() -> Path:
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
    b64 = base64.b64encode(p.read_bytes()).decode("utf-8")
    return f"data:image/png;base64,{b64}"

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

# ---------- Palette ----------
PALETTE = {
    "bg": "#f8fafc",
    "paper": "#f8f5f2",
    "brand": "#7c4c24",      # Andalusia brown
    "brand_hover": "#9a6231",
    "muted": "#64748b",
}

# ---------- CSS (tabs underline = brand brown) ----------
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

/* Tabs – base look */
.stTabs [data-baseweb="tab-list"] {{
  gap: 8px; border-bottom: 1px solid rgba(0,0,0,.08);
}}
.stTabs [data-baseweb="tab"] {{
  position: relative;           /* needed for our custom underline */
  height: 46px;
  padding: 10px 16px;
  border-radius: 999px 999px 0 0;
  background: #fff;
  border:1px solid rgba(0,0,0,0.06);
  border-bottom: none;
  color: {PALETTE['brand']};
}}

/* Remove any default red underline and replace with brand brown */
.stTabs [data-baseweb="tab"]::after {{
  content: "";
  position: absolute;
  left: 14px;
  right: 14px;
  bottom: -1px;
  height: 3px;
  background: transparent;
  border-radius: 2px;
}}

/* Remove Streamlit's default red underline completely */
.stTabs [data-baseweb="tab-highlight"] {{
    background-color: transparent !important;
    border-bottom: none !important;
}}

.stTabs [data-baseweb="tab"][aria-selected="true"]::after {{
  background: {PALETTE['brand']} !important;   /* <-- brown underline */
}}
.stTabs [data-baseweb="tab"][aria-selected="true"] {{
  box-shadow: 0 -4px 12px rgba(0,0,0,.08);
  border-color: {PALETTE['brand']};
  color: {PALETTE['brand']};
}}

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
  box-shadow:0 2px 6px rgba(0,0,0,0.04);
  padding: .9rem 1.1rem;
  margin-bottom: .5rem;
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
    <h1>Medical Predictions</h1>
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
DATA_DIR = ROOT / "data"

# ---------- TABS (Manual Entry + Validate on CSV) ----------
tab_manual_entry, tab_csv_validate = st.tabs(
    [" Manual Entry", " Validate on CSV"]
)

# ---------- Visit-level ICD10 callbacks (for Manual Entry) ----------
for _k in ("visit_dx_name", "visit_icd10", "visit_chief_complaint", "visit_symptoms"):
    if _k not in st.session_state:
        st.session_state[_k] = ""

def _on_visit_dx_change():
    """Auto-fill ICD10 when Diagnosis Name changes (visit-level, supports multiple)."""
    raw_name = (st.session_state.get("visit_dx_name") or "").strip()
    if not raw_name:
        return
    current_icd = st.session_state.get("visit_icd10", "")
    codes_str, names_str = resolve_icd10_multi(current_icd, raw_name)
    if codes_str:
        st.session_state["visit_icd10"] = codes_str
    if names_str:
        st.session_state["visit_dx_name"] = names_str

def _on_visit_icd_change():
    """Auto-fill Diagnosis Name when ICD10 changes (visit-level, supports multiple)."""
    raw_code = (st.session_state.get("visit_icd10") or "").strip()
    if not raw_code:
        return
    current_dx = st.session_state.get("visit_dx_name", "")
    codes_str, names_str = resolve_icd10_multi(raw_code, current_dx)
    if codes_str:
        st.session_state["visit_icd10"] = codes_str
    if names_str:
        st.session_state["visit_dx_name"] = names_str

# --- Manual-entry state for MULTIPLE services ---
if "manual_num_services" not in st.session_state:
    st.session_state["manual_num_services"] = 1  # start with one service

# === TAB 1: Manual Entry (multi-service → make_preds) ===
with tab_manual_entry:
    st.caption("Enter the required fields manually and run a prediction.")

    # -------- Visit-level fields (shared for all services) --------
    vcol1, vcol2 = st.columns(2)

    with vcol1:
        age = st.number_input("Age (required)", min_value=0, value=30, step=1)
        visit_type = st.selectbox(
            "Visit Type (required)",
            options=["ambulatory", "inpatient", "outpatient", "emergency"],
            index=2,  # default to outpatient
        )
        visit_dx_name = st.text_input(
            "Diagnosis Name (required)",
            key="visit_dx_name",
            placeholder="e.g., Iron deficiency anemia or E11,E78.2,D50,D51",
            on_change=_on_visit_dx_change,
        )
        visit_chief_complaint = st.text_input(
            "Chief Complaint (required)",
            key="visit_chief_complaint",
            placeholder="e.g., Fatigue and pallor",
        )

    with vcol2:
        provider_dept = st.text_input("Provider Department (required)", placeholder="e.g., Internal Medicine")
        gender = st.selectbox(
            "Patient Gender (required)",
            options=["Male", "Female"],
            index=0,
        )
        visit_icd10 = st.text_input(
            "ICD10 Code (required)",
            key="visit_icd10",
            placeholder="e.g., D50.9 or E11,E78.2,D50,D51",
            on_change=_on_visit_icd_change,
        )
        visit_symptoms = st.text_input(
            "Symptoms (optional)",
            key="visit_symptoms",
            placeholder="comma-separated…",
        )

    st.markdown("---")
    st.subheader("Services in this visit")

    # Number of current services
    num_services = st.session_state.get("manual_num_services", 1)

    # -------- Service-level fields (one block per service) --------
    for idx in range(1, num_services + 1):
        with st.expander(f"Service {idx}", expanded=(idx == 1)):
            sc1, sc2 = st.columns(2)
            with sc1:
                st.text_input(
                    "Service Name (required)",
                    key=f"service_name_{idx}",
                    placeholder="e.g., Ferritin",
                )
            with sc2:
                st.number_input(
                    "Quantity (required)",
                    min_value=1,
                    value=1,
                    step=1,
                    key=f"qty_{idx}",
                )

            # Remove button (only if more than one service)
            if num_services > 1:
                remove_clicked = st.button(
                    f"🗑 Remove this service",
                    key=f"remove_service_{idx}",
                )
                if remove_clicked:
                    # Shift later services up (idx+1 -> idx)
                    for j in range(idx, num_services):
                        st.session_state[f"service_name_{j}"] = st.session_state.get(f"service_name_{j+1}", "")
                        st.session_state[f"qty_{j}"] = st.session_state.get(f"qty_{j+1}", 1)
                    # Clear last service slot
                    last = num_services
                    for key_prefix in ["service_name_", "qty_"]:
                        st.session_state.pop(f"{key_prefix}{last}", None)
                    st.session_state["manual_num_services"] = num_services - 1
                    st.rerun()

    # Button to add more services (under the services list)
    add_col, _ = st.columns([1, 3])
    with add_col:
        if st.button("➕ Add another service", key="add_manual_service"):
            st.session_state["manual_num_services"] = num_services + 1
            st.rerun()

    # Run prediction button
    run_pred = st.button("Run Prediction", key="run_manual_pred")

    if run_pred:
        now = datetime.now()
        visit_id = f"MAN-{now.strftime('%Y%m%d%H%M%S')}"
        missing: List[str] = []
        type_errors: List[str] = []
        rows: List[dict] = []

        # Use current visit-level DX / ICD10 (after callbacks)
        visit_dx_input = st.session_state.get("visit_dx_name", "")
        visit_icd_input = st.session_state.get("visit_icd10", "")
        visit_cc = (st.session_state.get("visit_chief_complaint") or "").strip()
        visit_sym = (st.session_state.get("visit_symptoms") or "").strip()

        # Final ICD10/DX consistency at visit level (handles multiple codes)
        visit_icd_final, visit_dx_final = resolve_icd10_multi(visit_icd_input, visit_dx_input)

        # ---- Validate visit-level requireds ----
        if age is None:
            missing.append("Age")
        if not visit_type.strip():
            missing.append("Visit Type")
        if not provider_dept.strip():
            missing.append("Provider Department")
        if not gender.strip():
            missing.append("Patient Gender")
        if not visit_dx_final.strip():
            missing.append("Diagnosis Name / ICD10")
        if not visit_icd_final.strip():
            missing.append("ICD10")
        if not visit_cc.strip():
            missing.append("Chief Complaint")

        # ---- Type checks for text fields (only-numeric is suspicious) ----
        if provider_dept and looks_numeric(provider_dept):
            type_errors.append("Provider Department must be text, not only numbers.")
        if visit_dx_final and looks_numeric(visit_dx_final):
            type_errors.append("Diagnosis Name must be text, not only numbers.")
        if visit_cc and looks_numeric(visit_cc):
            type_errors.append("Chief Complaint must be text, not only numbers.")
        if visit_sym and looks_numeric(visit_sym):
            type_errors.append("Symptoms should be text (or a list), not only numbers.")

        # ---- Build one row per service ----
        num_services = st.session_state.get("manual_num_services", 1)
        for idx in range(1, num_services + 1):
            s_name = (st.session_state.get(f"service_name_{idx}") or "").strip()
            qty_val = st.session_state.get(f"qty_{idx}")

            # Per-service validation
            if not s_name:
                missing.append(f"Service {idx}: Service Name")
            if s_name and looks_numeric(s_name):
                type_errors.append(
                    f"Service {idx}: Service Name must be text, not only numbers."
                )
            if qty_val is None:
                missing.append(f"Service {idx}: Quantity")

            row = {
                # visit-level
                "VisitID":             visit_id,
                "Visit_Type":          visit_type.strip(),
                "PROVIDER_DEPARTMENT": provider_dept.strip(),
                "PATIENT_GENDER":      gender,
                "AGE":                 int(age) if age is not None else None,
                "Creation_Date":       now,
                "Updated_Date":        now,
                "DIAGNOS_NAME":        visit_dx_final,
                "ICD10":               visit_icd_final,
                "ProblemNote":         "",
                "Diagnose":            visit_dx_final,
                "CHIEF_COMPLAINT":     visit_cc,
                "Chief_Complaint":     visit_cc,
                "Symptoms":            visit_sym,

                # service-level
                "VisitServiceID":      idx,  # 1,2,3,...
                "Service_Name":        s_name,
                "Quantity":            int(qty_val) if qty_val is not None else None,
            }
            rows.append(row)

        # ---- If anything is wrong, show errors and do NOT call make_preds ----
        if missing or type_errors:
            if missing:
                st.error("Please fill: " + ", ".join(missing))
            if type_errors:
                st.error("Type errors:\n- " + "\n- ".join(type_errors))
        else:
            src = pd.DataFrame(rows)

            with st.spinner("Scoring the manual entry…"):
                history_df = make_preds(src)

            if history_df is None or history_df.empty:
                info_box("No prediction rows produced.")
            else:
                # Normalize Reason column name if needed
                if "Reason" not in history_df.columns and "Reason/Recommendation" in history_df.columns:
                    history_df["Reason"] = history_df["Reason/Recommendation"]

                # Hide VisitServiceID/service_id in the UI
                display_df = history_df.drop(
                    columns=[c for c in history_df.columns if c.lower() in {"visitserviceid", "service_id", "serviceid"}],
                    errors="ignore",
                )
                # Only show relevant columns
                cols_order = [c for c in ["Service_Name", "Medical_Prediction", "Reason"] if c in display_df.columns]
                if cols_order:
                    display_df = display_df[cols_order]
                rejected = (display_df["Medical_Prediction"] == "Rejected").sum() if "Medical_Prediction" in display_df else 0
                total = len(display_df)
                kpi_row([
                    ("Scored services", total),
                    ("Rejected", rejected),
                    ("Approved", total - rejected),
                ])
                st.dataframe(display_df, width="stretch")

# === TAB 2: Validate on CSV (upload CSV → pick VisitID → make_preds) ===
with tab_csv_validate:
    

    uploaded_file = st.file_uploader(
        "Upload CSV",
        type=["csv"],
        key="pred_csv_validate_uploader",
    )

    df_ids = pd.DataFrame()

    if uploaded_file is not None:
        try:
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
                key="pred_csv_visit_select",
            )
        with opt_col:
            show_src = st.checkbox("Show source row(s)", value=False, key="pred_csv_show_src")

        st.markdown('</div>', unsafe_allow_html=True)

        if st.button("Run Prediction on this Visit", key="pred_csv_run_btn"):
            vid = sel_visit.strip()
            if not vid:
                st.error("Please select a Visit ID.")
            else:
                # 1) Extract visit rows from the uploaded CSV itself
                src = df_ids[df_ids[visit_col].astype(str) == vid].copy()

                if src.empty:
                    info_box("No rows found in CSV for that Visit.")
                else:
                    if show_src:
                        st.subheader("Source Row(s) from uploaded CSV")
                        # Hide Creation_Date, Updated_Date, VisitServiceID in the *source* view
                        src_display = src.drop(
                            columns=["Creation_Date", "Updated_Date", "VisitServiceID"],
                            errors="ignore",
                        )
                        st.dataframe(src_display, width="stretch")

                    # 2) Run prediction model
                    try:
                        with st.spinner(f"Scoring services for VisitID {vid}…"):
                            history_df = make_preds(src)
                    except Exception as e:
                        st.error(f"Prediction pipeline failed: {e}")
                        history_df = None

                    if history_df is None or history_df.empty:
                        info_box("No prediction rows produced.")
                    else:
                        # Normalize Reason column name if needed
                        if "Reason" not in history_df.columns and "Reason/Recommendation" in history_df.columns:
                            history_df["Reason"] = history_df["Reason/Recommendation"]

                        # Hide VisitServiceID in display
                        display_df = history_df.drop(
                            columns=[c for c in history_df.columns if c.lower() in {"visitserviceid", "service_id", "serviceid"}],
                            errors="ignore",
                        )
                        cols_order = [c for c in ["Service_Name", "Medical_Prediction", "Reason"] if c in display_df.columns]
                        if cols_order:
                            display_df = display_df[cols_order]
                        rejected = (display_df["Medical_Prediction"] == "Rejected").sum() if "Medical_Prediction" in display_df else 0
                        total = len(display_df)
                        kpi_row([("Scored services", total), ("Rejected", rejected), ("Approved", total - rejected)])
                        st.dataframe(display_df, width="stretch")

# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:1.5rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)