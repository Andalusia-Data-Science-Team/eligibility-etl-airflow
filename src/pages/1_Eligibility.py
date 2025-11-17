# pages/1_Eligibility.py
import base64
from pathlib import Path
from datetime import datetime

import pandas as pd
import streamlit as st

# ---------------- Page Setup ----------------
st.set_page_config(page_title="Eligibility Viewer", page_icon="💊", layout="wide")

# ---------- Locate logo & build data URI ----------
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
.header-logo img { max-width:170px; height:auto; mix-blend-mode:multiply; background-color:transparent!important; }
</style>
""", unsafe_allow_html=True)

# ---------- Palette ----------
PALETTE = {"bg":"#f8fafc","paper":"#f8f5f2","brand":"#7c4c24","brand_hover":"#9a6231","muted":"#64748b"}

# ---------- CSS ----------
st.markdown(f"""
<style>
html, body, [data-testid="stAppViewContainer"]{{background:{PALETTE['bg']};}}
.block-container{{max-width:1280px; margin:0 auto;}}
.header-box{{display:flex;align-items:center;justify-content:space-between;background:{PALETTE['paper']};
  border:1px solid rgba(0,0,0,.06); border-radius:16px; box-shadow:0 2px 6px rgba(0,0,0,.06); padding:1.1rem 1.4rem; margin-bottom:.75rem;}}
.header-title h1{{margin:0;color:{PALETTE['brand']};font-weight:800;font-size:2rem;}}
.header-logo img{{max-width:170px;height:auto;display:block;}}
.subhead{{font-size:.9rem;font-weight:600;color:#475569;margin:.4rem 0 1rem;letter-spacing:.2px;text-align:center;}}
.result-head{{display:flex;align-items:center;gap:.5rem;padding:.8rem 1rem;background:linear-gradient(0deg,rgba(0,0,0,.02),rgba(0,0,0,.02)),{PALETTE['paper']};
  border:1px solid rgba(0,0,0,.06); border-radius:12px;}}
.result-body{{padding:1rem 0;}}
.icon-pill{{display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:10px;border:1px solid rgba(0,0,0,.06);background:#efe6df;filter:grayscale(100%);}}
.icon-medical::before{{content:"🧬";}}
.badge{{display:inline-block;padding:.35rem .65rem;border-radius:9999px;font-weight:700;font-size:.86rem;}}
.badge-ok{{background:#d1fadf;color:#166534;border:1px solid #bbf7d0;}}
.badge-warn{{background:#fef9c3;color:#854d0e;border:1px solid #fde68a;}}
.badge-err{{background:#fee2e2;color:#991b1b;border:1px solid #fecaca;}}
.badge-meta{{color:#475569;font-weight:600;font-size:.8rem;margin-left:auto;}}
.kv-grid{{display:grid;grid-template-columns:repeat(2,minmax(260px,1fr));gap:1rem 1.25rem;}}
@media (max-width:780px){{.kv-grid{{grid-template-columns:1fr;}}}}
.kv-item{{border-right:1px dashed rgba(0,0,0,.06);padding-right:1rem;}}
.kv-item:nth-child(2n){{border-right:none;padding-right:0;}}
.kv-label{{color:#64748b;font-weight:700;font-size:.88rem;margin-bottom:.15rem;letter-spacing:.2px;}}
.kv-value{{color:#0f172a;}}
.footer{{text-align:center;color:#94a3b8;margin-top:1.2rem;}}
</style>
""", unsafe_allow_html=True)

# ---------- Header ----------
st.markdown(
    f"""
<div class="header-box">
  <div class="header-title"><h1>Eligibility Viewer</h1></div>
  <div class="header-logo">{'<img src="' + _LOGO_URI + '" alt="Andalusia Health Logo"/>' if _LOGO_URI else ""}</div>
</div>
""",
    unsafe_allow_html=True,
)

st.markdown('<p class="subhead">Pick a Visit ID</p>', unsafe_allow_html=True)

# ---------------- Paths ----------------
HERE = Path(__file__).resolve()
ROOT = HERE.parents[2] if (HERE.parent.name == "pages" and HERE.parents[1].name == "src") else HERE.parents[1]
DATA_CSV = ROOT / "data" / "eligibility_history.csv"

# ---------------- Load Data (robust to encoding) ----------------
@st.cache_data(show_spinner=False)
def load_data(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        st.error(f"CSV file not found at: {csv_path}")
        st.stop()

    df = None
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, encoding=enc, on_bad_lines="skip")
            break
        except Exception:
            continue

    if df is None:
        st.error("Could not read CSV with common encodings.")
        st.stop()

    # normalize headers to lowercase (spaces preserved)
    df.columns = [c.lower() for c in df.columns]
    return df

df = load_data(DATA_CSV)

# ---------------- Small helper: safe pick from row ----------------
def _pick(row: pd.Series, candidates, default: str = "") -> str:
    """
    Return the first existing value from the row among candidate column names.
    We try exact, lowercase, ' ' -> '_' , and without spaces.
    """
    for cand in candidates:
        keys = {
            cand,
            cand.lower(),
            cand.lower().replace(" ", "_"),
            cand.lower().replace(" ", ""),
        }
        for k in keys:
            if k in row.index:
                val = row.get(k, "")
                if val is not None and str(val).strip() != "":
                    return str(val)
    return default

# ---------------- Status helpers ----------------
def _status_from_row(class_val, note_val) -> str:
    """
    Normalize into one of:
      'eligible', 'not-active', 'not-covered', 'out-network', 'coverage-suspended'
    Also honors note codes:
      1680 -> out-network, 1658 -> not-active
    """
    c = "" if pd.isna(class_val) else str(class_val).strip().lower()
    n = "" if pd.isna(note_val) else str(note_val).strip().lower()

    if n == "1680":
        return "out-network"
    if n == "1658":
        return "not-active"

    def has(*keywords):
        return any(k in c for k in keywords)

    if has("suspend"):
        return "coverage-suspended"
    if has("out of network", "out-of-network", "out network", "o.o.n", "oon", "out-network"):
        return "out-network"
    if has("not covered", "no coverage", "no-cover", "not-cover", "benefit not", "exceeded limit", "benefit exhausted"):
        return "not-covered"
    if has("not active", "inactive", "terminated", "expired", "blocked", "cancelled", "canceled", "denied", "rejected"):
        return "not-active"
    if has("eligible", "active", "approved", "authorized", "authorised", "covered", "valid", "in network", "in-network"):
        return "eligible"

    return "not-active"

def _badge_from_status(status: str) -> str:
    s = (status or "").strip().lower()
    cls_map = {
        "eligible": "badge-ok",
        "out-network": "badge-warn",
        "coverage-suspended": "badge-warn",
        "not-covered": "badge-err",
        "not-active": "badge-err",
    }
    label_map = {
        "eligible": "Eligible",
        "out-network": "Out of network",
        "coverage-suspended": "Coverage suspended",
        "not-covered": "Not covered",
        "not-active": "Not active",
    }
    return f'<span class="badge {cls_map.get(s, "badge-err")}">{label_map.get(s, "Not active")}</span>'

# ---------------- Controls ----------------
visit_ids = sorted(df["visit_id"].astype(str).unique().tolist())
selected_visit = st.selectbox(
    "Visit ID",
    visit_ids,
    index=0,
    help="Most recent record is shown",
    label_visibility="visible",
)

# ---------------- Pick latest record for this visit ----------------
DATE_PRIORITY = ["insertion_date", "eligibility_requested_date"]
sort_col = next((c for c in DATE_PRIORITY if c in df.columns), None)

if sort_col:
    record = (
        df[df["visit_id"].astype(str) == selected_visit]
        .copy()
        .sort_values(by=sort_col, ascending=False, kind="stable")
        .head(1)
    )
else:
    record = df[df["visit_id"].astype(str) == selected_visit].head(1)

# ---------------- Display ----------------
if not record.empty:
    row = record.iloc[0]

    class_val = _pick(row, ["class", "eligibility_status", "status"])
    note_val  = _pick(row, ["note", "eligibility_note"])
    status = _status_from_row(class_val, note_val)

    st.markdown(
        f"""
        <div class="result-head">
          <span class="icon-pill icon-medical"></span>
          {_badge_from_status(status)}
          <span class="badge-meta">Latest snapshot</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="result-body">', unsafe_allow_html=True)
    st.markdown(
        f"""
        <div class="kv-grid">
          <div class="kv-item"><div class="kv-label">Visit</div><div class="kv-value">{_pick(row, ['visit_id'])}</div></div>
          <div class="kv-item"><div class="kv-label">Eligibility Status</div><div class="kv-value">{_pick(row, ['eligibility_status','class'])}</div></div>
          <div class="kv-item"><div class="kv-label">Eligibility Note</div><div class="kv-value">{_pick(row, ['eligibility_note','note'])}</div></div>
          <div class="kv-item"><div class="kv-label">Insurance Company</div><div class="kv-value">{_pick(row, ['insurance company'])}</div></div>
          <div class="kv-item"><div class="kv-label">Policy Number</div><div class="kv-value">{_pick(row, ['policy number'])}</div></div>
          <div class="kv-item"><div class="kv-label">Class Name</div><div class="kv-value">{_pick(row, ['class name','class'])}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('</div>', unsafe_allow_html=True)
else:
    st.warning("No record found for this Visit ID.")

# ---------- Footer ----------
st.markdown(
    f'<p class="footer">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)