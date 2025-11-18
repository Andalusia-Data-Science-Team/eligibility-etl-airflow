# pages/2_Predictions.py
import base64
from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
import streamlit as st

# --- path bootstrap: make sure the project root (parent of "src") is on sys.path ---
import sys
from pathlib import Path

_THIS = Path(__file__).resolve()

# Typical layouts this covers:
#   <project>/src/pages/2_Predictions.py           -> project root = parents[3]
#   <project>/pages/2_Predictions.py (rare)        -> project root = parents[1]
candidates = []

# If file lives in .../src/pages/...
if _THIS.parent.name == "pages" and _THIS.parent.parent.name == "src":
    candidates += [_THIS.parents[3], _THIS.parents[2], _THIS.parents[1]]
else:
    # Fallbacks for other layouts
    candidates += [_THIS.parents[2], _THIS.parents[1], _THIS.parents[0]]

for p in candidates:
    if p and str(p) not in sys.path:
        sys.path.append(str(p))
# --- end path bootstrap ---

from src.components import kpi_row, info_box
from src.AHJ_Claims import make_preds

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

    # 1) If ICD10 is provided and valid → trust it
    if icd10 and icd10 in code_to_name:
        return icd10, code_to_name[icd10]

    # 2) If only DX name is given → try to map
    if dx:
        key = " ".join(dx.lower().split())
        if key in name_to_code:
            c = name_to_code[key]
            return c, code_to_name.get(c, dx)
        # loose contains match as fallback
        match = next((c for c, name in code_to_name.items() if key and key in name.lower()), None)
        if match:
            return match, code_to_name[match]

    # 3) Fallback: pass through raw user text
    return icd10, dx

# ===== Manual-entry helpers (small static map just to help callbacks) =====
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

def _on_dx_pred():
    """Auto-fill ICD10 for Predictions manual tab when Diagnosis changes."""
    raw_name = st.session_state.get("dx_name_pred", "")
    icd, dx = resolve_icd10_pair("", raw_name)
    if icd:
        st.session_state["icd10_pred"] = icd

def _on_icd_pred():
    """Auto-fill Diagnosis for Predictions manual tab when ICD10 changes."""
    raw_code = st.session_state.get("icd10_pred", "")
    icd, dx = resolve_icd10_pair(raw_code, "")
    if dx:
        st.session_state["dx_name_pred"] = dx

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
  border:1px solid rgba(0,0,0,.06);
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

/* Remove Streamlit's default red tab underline completely */
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
  box-shadow:0 2px 6px rgba(0,0,0,.04);
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

# ---------- TABS (match Resubmission: Manual Entry + Validate on CSV) ----------
tab_manual_entry, tab_csv_validate = st.tabs(
    [" Manual Entry", " Validate on CSV"]
)

# === TAB 1: Manual Entry (minimal required fields → make_preds) ===
with tab_manual_entry:
    st.caption("Enter the required fields manually and run a prediction.")

    # Only the *prediction-relevant* fields (no rejection reason etc.)
    c1, c2 = st.columns(2)

    with c1:
        service_name = st.text_input("Service Name (required)", placeholder="e.g., Ferritin")
        dx_name = st.text_input(
            "Diagnosis Name (required)",
            key="dx_name_pred",
            placeholder="e.g., Iron deficiency anemia",
            on_change=_on_dx_pred,  # auto-fill ICD10 when DX changes
        )
        chief_complaint = st.text_input("Chief Complaint (required)", placeholder="e.g., Fatigue and pallor")
        qty = st.number_input("Quantity", min_value=1, value=1, step=1)
        age = st.number_input("Age", min_value=0, value=30, step=1)

    with c2:
        icd10 = st.text_input(
            "ICD10 Code (required)",
            key="icd10_pred",
            placeholder="e.g., D50.9",
            on_change=_on_icd_pred,  # auto-fill DX when ICD10 changes
        )
        symptoms = st.text_input("Symptoms (optional)", placeholder="comma-separated…")

    run_pred = st.button("Run Prediction", key="run_manual_pred")

    if run_pred:
        # Read the *current* mapped values from session_state
        dx_input = st.session_state.get("dx_name_pred", dx_name)
        icd10_input = st.session_state.get("icd10_pred", icd10)

        # Final consistency pass using full ICD10 table
        icd10_final, dx_final = resolve_icd10_pair(icd10_input, dx_input)

        # Validate requireds
        missing = []
        if not service_name.strip(): missing.append("Service Name")
        if not dx_final.strip():     missing.append("Diagnosis Name / ICD10")
        if not icd10_final.strip():  missing.append("ICD10")
        if not chief_complaint.strip(): missing.append("Chief Complaint")
        if age is None: missing.append("Age")
        if qty is None: missing.append("Quantity")

        if missing:
            st.error("Please fill: " + ", ".join(missing))
        else:
            # Build a one-row DF with required columns for make_preds()
            now = datetime.now()
            manual_row = {
                "VisitID":              f"MAN-{now.strftime('%Y%m%d%H%M%S')}",  # synthetic
                "Visit_Type":           "Manual Entry",
                "PROVIDER_DEPARTMENT":  "Unknown",
                "PATIENT_GENDER":       "Unknown",
                "AGE":                  int(age),
                "Creation_Date":        now,
                "Updated_Date":         now,
                "Service_Name":         service_name.strip(),
                "Quantity":             int(qty),
                "DIAGNOS_NAME":         dx_final,
                "ICD10":                icd10_final,
                "ProblemNote":          "",
                "Diagnose":             dx_final,
                "CHIEF_COMPLAINT":      chief_complaint.strip(),
                "Chief_Complaint":      chief_complaint.strip(),
                "Symptoms":             symptoms.strip(),
                "VisitServiceID":       f"VS-{now.strftime('%H%M%S%f')}",       # synthetic
            }
            src = pd.DataFrame([manual_row])

            with st.spinner("Scoring the manual entry…"):
                history_df = make_preds(src)

            if history_df is None or history_df.empty:
                info_box("No prediction rows produced.")
            else:
                rejected = (history_df["Medical_Prediction"] == "Rejected").sum() if "Medical_Prediction" in history_df else 0
                total = len(history_df)
                kpi_row([("Scored services", total), ("Rejected", rejected), ("Approved", total - rejected)])
                st.dataframe(history_df, width="stretch")

# === TAB 2: Validate on CSV (upload CSV → pick VisitID → make_preds) ===
with tab_csv_validate:
    st.caption("Upload a CSV of visit rows, pick a Visit ID, and run the prediction on those rows.")

    uploaded_file = st.file_uploader(
        "Upload CSV (should contain VisitID + the columns used by the prediction model)",
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
                        st.dataframe(src, width="stretch")

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
                        rejected = (history_df["Medical_Prediction"] == "Rejected").sum() if "Medical_Prediction" in history_df else 0
                        total = len(history_df)
                        kpi_row([("Scored services", total), ("Rejected", rejected), ("Approved", total - rejected)])
                        st.dataframe(history_df, width="stretch")

# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)
