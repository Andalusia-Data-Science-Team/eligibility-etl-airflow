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

from src.db import read_sql, cached_read, get_engines
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
SQL_DIR = ROOT / "sql"
DATA_DIR = ROOT / "data"

# ---------- Fallback CSVs ----------
HIST_CSV_CANDIDATES = [
    DATA_DIR / "data/history_service_names.csv",
    DATA_DIR / "history_service_names.csv",
]

def load_history_csv() -> Optional[pd.DataFrame]:
    for p in HIST_CSV_CANDIDATES:
        if p.exists():
            try:
                df = pd.read_csv(p, dtype=str, keep_default_na=False)
                df.columns = [c.strip() for c in df.columns]
                return df
            except Exception as e:
                st.error(f"Failed to read fallback CSV at {p}: {e}")
                return None
    return None

def network_issue_box(where: str):
    st.warning(
        f"⚠️ We couldn’t reach the {where} database right now. "
        f"This is often a temporary network or timeout issue.\n\n"
        f"→ Falling back to a local snapshot CSV if available."
    )

# ---------- TABS ----------
tab_history, tab_predict, tab_live = st.tabs(
    [" Historical Records", " Predict One Visit", " Live Source"]
)

# === HISTORY ===
with tab_history:
    c1, c2 = st.columns([1.5, 1])
    with c1:
        visit_filter = st.text_input("Filter by Visit ID (optional)", placeholder="e.g., 715397")
    with c2:
        limit_rows = st.number_input("Max rows", min_value=100, max_value=100000, value=100)
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("Load Predictions", key="btn_hist"):
        # Always load from CSV
        df = load_history_csv()

        if df is None or df.empty:
            info_box("No history rows available.")
        else:
            # Normalize column names
            cols_lower = [c.lower() for c in df.columns]
            if "visitid" not in cols_lower and "visit_id" in cols_lower:
                df.rename(columns={"visit_id": "VisitID"}, inplace=True)
            if "medical_prediction" not in cols_lower and "Medical_Prediction" in df.columns:
                pass
            elif "prediction" in cols_lower:
                df.rename(columns={"prediction": "Medical_Prediction"}, inplace=True)

            # Apply optional filter
            if visit_filter.strip():
                df = df[df["VisitID"].astype(str) == visit_filter.strip()]

            # Limit number of rows
            df = df.head(int(limit_rows))

            if df.empty:
                info_box("No rows matched your filters.")
            else:
                rejected = (df["Medical_Prediction"] == "Rejected").sum() if "Medical_Prediction" in df.columns else 0
                total = len(df)
                kpi_row([("History rows", total), ("Rejected", rejected), ("Approved", total - rejected)])
                st.dataframe(df, use_container_width=True)
                st.caption("Showing local snapshot.")
                
# === PREDICT ONE ===
with tab_predict:
    st.caption("Generate a new prediction for a Visit ID.")

    # Input only (no 'show source' checkbox)
    vcol = st.columns([3])[0]
    with vcol:
        visit_id = st.text_input("Visit ID (required)", placeholder="Enter a Visit ID to score")

    if st.button("Run predictions now", key="btn_predict"):
        if not visit_id.strip():
            st.error("⚠️ You must enter a Visit ID before running predictions.")
        else:
            params = {"vid": visit_id.strip()}
            try:
                with st.spinner("Loading visit data..."):
                    src = read_sql("REPLICA", """
SELECT DISTINCT
    VS.VisitID, VC.EnName AS Visit_Type, V.MainSpecialityEnName AS PROVIDER_DEPARTMENT,
    G.EnName AS PATIENT_GENDER, DATEDIFF(YEAR, PA.DateOfBirth, GETDATE()) AS AGE,VS.id as VisitServiceID,
    VS.ClaimDate AS Creation_Date, VS.UpdatedDate AS Updated_Date, VS.ServiceEnName AS [Service_Name], VS.Quantity,
    PCD.ICDDiagnoseNames AS DIAGNOS_NAME, PCD.ICDDiagnoseCodes AS ICD10, PCD.ProblemNote,
    PCD.ICDDiagnoseNames AS Diagnose,
    CC.ChiefComplaintNotes AS CHIEF_COMPLAINT, CC.ChiefComplaintNotes AS Chief_Complaint,
    SAS.Symptoms
FROM VisitMgt.VisitService AS VS
LEFT JOIN VisitMgt.Visit AS V ON VS.VisitID = V.ID
LEFT JOIN VISITMGT.SLKP_visitclassification VC ON V.VisitClassificationID = VC.ID
LEFT JOIN MPI.Patient PA ON PA.ID = V.PatientID
LEFT JOIN MPI.SLKP_Gender G ON PA.GenderID = G.ID
LEFT JOIN VisitMgt.VisitFinincailInfo AS VFI ON VFI.VisitID = VS.VisitID
LEFT JOIN PatPrlm.ChiefComplaint CC ON CC.VisitID = VS.VisitID
LEFT JOIN (
    SELECT VISITID,
           STRING_AGG(PCD.ICDDiagnoseName, ' , ') AS ICDDiagnoseNames,
           STRING_AGG(PCD.ICDDiagnoseCode, ' , ') AS ICDDiagnoseCodes,
           STRING_AGG(PC.Note, ' , ') AS ProblemNote
    FROM Patprlm.ProblemCard AS PC
    LEFT JOIN Patprlm.ProblemCardDetail PCD ON PC.ID = PCD.ProblemCardID
    GROUP BY VISITID
) AS PCD ON PCD.VisitID = VS.VisitID
LEFT JOIN (
    SELECT VISITID, STRING_AGG(SS.SignAndSymptomNotes, ',') AS Symptoms
    FROM [PatPrlm].[SignsAndSymptoms] SS
    GROUP BY VISITID
) AS SAS ON VS.VISITID = SAS.VISITID
WHERE V.VisitStatusID != 3 AND VC.EnName != 'Ambulatory' AND VS.IsDeleted = 0
  AND VS.CompanyShare > 0 AND VFI.ContractTypeID = 1 AND VS.VisitID = :vid
""", params)

                if src.empty:
                    info_box(f"No rows found on Replica for VisitID {visit_id}.")
                else:
                    with st.spinner(f"Scoring services for VisitID {visit_id}..."):
                        history_df = make_preds(src)

                    if history_df is None or history_df.empty:
                        info_box("produced no rows.")
                    else:
                        rejected = (history_df["Medical_Prediction"] == "Rejected").sum()
                        total = len(history_df)
                        kpi_row([
                            ("Scored services", total),
                            ("Rejected", rejected),
                            ("Approved", total - rejected),
                        ])
                        st.dataframe(history_df, use_container_width=True)
            except Exception:
                network_issue_box("live")


# === LIVE (manual entry; no DB) ===
with tab_live:
    st.caption("Enter the required fields manually and run a prediction.")

    with st.form("live_manual_pred"):
        c1, c2 = st.columns(2)

        with c1:
            service_name = st.text_input("Service Name (required)", placeholder="e.g., Ferritin")
            dx_name      = st.text_input("Diagnosis Name (required)", key="dx_name_pred", placeholder="e.g., Iron deficiency anemia")
            chief_complaint = st.text_input("Chief Complaint (required)", placeholder="e.g., Fatigue and pallor")
            qty          = st.number_input("Quantity", min_value=1, value=1, step=1)
            age          = st.number_input("Age", min_value=0, value=30, step=1)

        with c2:
            icd10        = st.text_input("ICD10 Code (required)", key="icd10_pred", placeholder="e.g., D50.9")
            symptoms     = st.text_input("Symptoms (optional)", placeholder="comma-separated…")

        run_pred = st.form_submit_button("Run Prediction")

    if run_pred:
        # Resolve ICD10 <-> Diagnosis
        icd10_final, dx_final = resolve_icd10_pair(icd10, dx_name)

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
                st.dataframe(history_df, use_container_width=True)


# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)