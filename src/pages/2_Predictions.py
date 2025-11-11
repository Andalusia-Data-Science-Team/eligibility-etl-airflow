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

st.markdown(
    """
<style>
.header-logo img {
    max-width: 170px;
    height: auto;
    mix-blend-mode: multiply;     /* makes white blend with background */
    background-color: transparent !important;
}
</style>
""",
    unsafe_allow_html=True,
)

# ---------- Palette ----------
PALETTE = {
    "bg": "#f8fafc",
    "paper": "#f8f5f2",
    "brand": "#7c4c24",  # Andalusia brown
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
ROOT = (
    HERE.parents[2]
    if (HERE.parent.name == "pages" and HERE.parents[1].name == "src")
    else HERE.parents[1]
)
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
    st.caption("View previous prediction results (from local CSV snapshot).")
    c1, c2 = st.columns([1.5, 1])
    with c1:
        visit_filter = st.text_input(
            "Filter by Visit ID (optional)", placeholder="e.g., 715397"
        )
    with c2:
        limit_rows = st.number_input(
            "Max rows", min_value=100, max_value=100000, value=100
        )
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("Load Predictions", key="btn_hist"):
        # Always load from CSV
        df = load_history_csv()

        if df is None or df.empty:
            info_box("No history rows available in the CSV snapshot.")
        else:
            # Normalize column names
            cols_lower = [c.lower() for c in df.columns]
            if "visitid" not in cols_lower and "visit_id" in cols_lower:
                df.rename(columns={"visit_id": "VisitID"}, inplace=True)
            if (
                "medical_prediction" not in cols_lower
                and "Medical_Prediction" in df.columns
            ):
                pass
            elif "prediction" in cols_lower:
                df.rename(columns={"prediction": "Medical_Prediction"}, inplace=True)

            # Apply optional filter
            if visit_filter.strip():
                df = df[df["VisitID"].astype(str) == visit_filter.strip()]

            # Limit number of rows
            df = df.head(int(limit_rows))

            if df.empty:
                info_box("No history rows matched your filters.")
            else:
                rejected = (
                    (df["Medical_Prediction"] == "Rejected").sum()
                    if "Medical_Prediction" in df.columns
                    else 0
                )
                total = len(df)
                kpi_row(
                    [
                        ("History rows", total),
                        ("Rejected", rejected),
                        ("Approved", total - rejected),
                    ]
                )
                st.dataframe(df, use_container_width=True)
                st.caption("Showing local CSV snapshot.")

# === PREDICT ONE ===
with tab_predict:
    st.caption(
        "Generate a new prediction for a Visit ID. Falls back to local snapshot if Replica is unavailable."
    )

    # Input only (no 'show source' checkbox)
    vcol = st.columns([3])[0]
    with vcol:
        visit_id = st.text_input(
            "Visit ID (required)", placeholder="Enter a Visit ID to score"
        )

    if st.button("Run predictions now", key="btn_predict"):
        if not visit_id.strip():
            st.error("⚠️ You must enter a Visit ID before running predictions.")
        else:
            params = {"vid": visit_id.strip()}
            try:
                with st.spinner("Loading visit data..."):
                    src = read_sql(
                        "REPLICA",
                        """
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
""",
                        params,
                    )

                if src.empty:
                    info_box(f"No rows found on Replica for VisitID {visit_id}.")
                else:
                    with st.spinner(f"Scoring services for VisitID {visit_id}..."):
                        history_df = make_preds(src)

                    if history_df is None or history_df.empty:
                        info_box("produced no rows.")
                    else:
                        rejected = (
                            history_df["Medical_Prediction"] == "Rejected"
                        ).sum()
                        total = len(history_df)
                        kpi_row(
                            [
                                ("Scored services", total),
                                ("Rejected", rejected),
                                ("Approved", total - rejected),
                            ]
                        )
                        st.dataframe(history_df, use_container_width=True)
            except Exception:
                network_issue_box("live")


# === LIVE ===
with tab_live:
    st.caption("Load latest Replica data · Choose a Visit ID · Predict it.")

    # Top action row (kept minimal & on-brand)
    load_col, _ = st.columns([1, 3])
    with load_col:
        load_clicked = st.button("Load latest window", key="btn_live_load")

    if load_clicked:
        try:
            base_sql = (SQL_DIR / "claims_ahj.sql").read_text().strip()
            if base_sql.endswith(";"):
                base_sql = base_sql[:-1]
            live_df = read_sql("REPLICA", base_sql, {})
            st.session_state["_live_window_df"] = live_df
            st.session_state["_live_err"] = None
        except Exception as e:
            st.session_state["_live_window_df"] = pd.DataFrame()
            st.session_state["_live_err"] = str(e)

    # Error surface (if any)
    if st.session_state.get("_live_err"):
        st.error(
            "Couldn’t reach REPLICA.\n\n"
            "• Check VPN / network / DB credentials\n"
            "• Verify `sql/claims_ahj.sql` exists\n\n"
            f"Details: {st.session_state['_live_err']}"
        )

    live_df = st.session_state.get("_live_window_df")

    if isinstance(live_df, pd.DataFrame) and not live_df.empty:
        # KPIs for context
        from src.components import kpi_row  # safe import

        kpi_row(
            [
                ("Rows", len(live_df)),
                (
                    "Visits",
                    live_df["VisitID"].nunique() if "VisitID" in live_df.columns else 0,
                ),
            ]
        )

        st.markdown("---")

        st.caption("Pick a Visit ID from the loaded window and run predictions on it.")
        select_col, persist_col = st.columns([2, 1])
        with select_col:
            visit_choices = (
                live_df["VisitID"].astype(str).unique().tolist()
                if "VisitID" in live_df.columns
                else []
            )
            sel_visit = st.selectbox("Select Visit ID", options=visit_choices)
        with persist_col:
            persist_from_live = st.checkbox(
                "Append to AI history", value=False, key="persist_from_live"
            )

        if st.button("Predict selected visit", key="btn_predict_from_live"):
            try:
                params = {"vid": str(sel_visit)}
                with st.spinner(f"Fetching VisitID {sel_visit} from REPLICA..."):
                    src = read_sql(
                        "REPLICA",
                        """
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
                    """,
                        params,
                    )

                if src.empty:
                    info_box(f"No rows found on REPLICA for VisitID {sel_visit}.")
                else:
                    with st.spinner(f"Scoring services for VisitID {sel_visit}..."):
                        history_df = make_preds(src)

                    if history_df is None or history_df.empty:
                        info_box("produced no rows.")
                    else:
                        rejected = (
                            history_df["Medical_Prediction"] == "Rejected"
                        ).sum()
                        total = len(history_df)
                        kpi_row(
                            [
                                ("Scored services", total),
                                ("Rejected", rejected),
                                ("Approved", total - rejected),
                            ]
                        )
                        st.dataframe(history_df, use_container_width=True)

                        if persist_from_live:
                            try:
                                eng = get_engines()["AI"]
                                with st.spinner(
                                    "Appending to AI.dbo.Claims_Predictions_History..."
                                ):
                                    history_df.to_sql(
                                        name="Claims_Predictions_History",
                                        con=eng,
                                        schema="dbo",
                                        if_exists="append",
                                        index=False,
                                        chunksize=1000,
                                    )
                                st.success(
                                    "Appended to AI.dbo.Claims_Predictions_History."
                                )
                            except Exception as e:
                                st.error(f"Failed to write to AI history: {e}")
            except Exception as e:
                st.error(f"Replica read failed: {e}")
    else:
        st.caption("Load the latest window first, then select a Visit ID to score.")


# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)
