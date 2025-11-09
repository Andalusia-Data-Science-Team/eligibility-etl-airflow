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
.header-logo img {
    max-width: 170px;
    height: auto;
    mix-blend-mode: multiply;
    background-color: transparent !important;
}
</style>
""", unsafe_allow_html=True)

# ---------- Palette ----------
PALETTE = {
    "bg": "#f8fafc",
    "paper": "#f8f5f2",
    "brand": "#7c4c24",
    "brand_hover": "#9a6231",
    "muted": "#64748b",
}

# ---------- CSS ----------
st.markdown(
    f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{
  background: {PALETTE['bg']};
}}
.block-container {{
  max-width: 1280px;
  margin: 0 auto;
}}
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

.subhead {{
  font-size:.9rem; font-weight:600; color:#475569;
  margin:.4rem 0 1rem; letter-spacing:.2px; text-align:center;
}}
.surface-card {{
  background:#fff;
  border:1px solid rgba(0,0,0,.06);
  border-radius:14px;
  box-shadow:0 2px 6px rgba(0,0,0,.04);
  transition: box-shadow .15s ease, transform .08s ease;
}}
.surface-card:hover {{ box-shadow:0 6px 18px rgba(0,0,0,.08); }}
.result-card {{ padding: 0; overflow:hidden; }}
.result-head {{
  display:flex; align-items:center; gap:.5rem;
  padding:.8rem 1rem;
  background: linear-gradient(0deg, rgba(0,0,0,.02), rgba(0,0,0,.02)), {PALETTE['paper']};
  border-bottom:1px solid rgba(0,0,0,.06);
}}
.result-body {{ padding: 1rem 1.2rem; }}
.icon-pill {{
  display:inline-flex; align-items:center; justify-content:center;
  width:34px; height:34px; border-radius:10px;
  border:1px solid rgba(0,0,0,.06);
  background:#efe6df; filter: grayscale(100%);
}}
.icon-medical::before {{ content:"🧬"; }}
.badge {{ display:inline-block; padding:.35rem .65rem; border-radius:9999px; font-weight:700; font-size:.86rem; }}
.badge-ok   {{ background:#d1fadf; color:#166534; border:1px solid #bbf7d0; }}
.badge-warn {{ background:#fef9c3; color:#854d0e; border:1px solid #fde68a; }}
.badge-err  {{ background:#fee2e2; color:#991b1b; border:1px solid #fecaca; }}
.badge-meta {{ color:#475569; font-weight:600; font-size:.8rem; margin-left:auto; }}
.kv-grid {{
  display:grid; grid-template-columns: repeat(2, minmax(220px,1fr));
  gap: 1rem 1.25rem;
}}
@media (max-width: 780px) {{ .kv-grid {{ grid-template-columns:1fr; }} }}
.kv-item {{ border-right:1px dashed rgba(0,0,0,.06); padding-right: 1rem; }}
.kv-item:nth-child(2n) {{ border-right:none; padding-right:0; }}
.kv-label {{ color:#64748b; font-weight:700; font-size:.88rem; margin-bottom:.15rem; letter-spacing:.2px; }}
.kv-value {{ color:#0f172a; }}
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
    <h1>Eligibility Viewer</h1>
  </div>
  <div class="header-logo">
    {"<img src='" + _LOGO_URI + "' alt='Andalusia Health Logo'/>" if _LOGO_URI else ""}
  </div>
</div>
""",
    unsafe_allow_html=True,
)

st.markdown('<p class="subhead">Pick a Visit ID</p>', unsafe_allow_html=True)

# ---------------- Paths ----------------
HERE = Path(__file__).resolve()
ROOT = HERE.parents[2] if (HERE.parent.name == "pages" and HERE.parents[1].name == "src") else HERE.parents[1]
DATA_CSV = ROOT / "data" / "eligibility_history.csv"

# ---------------- Load Data (fixed encoding) ----------------
@st.cache_data(show_spinner=False)
def load_data(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        st.error(f"CSV file not found at: {csv_path}")
        st.stop()

    tried = []
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin1"):
        try:
            df = pd.read_csv(
                csv_path,
                dtype=str,
                keep_default_na=False,
                encoding=enc,
                on_bad_lines="skip",
            )
            st.caption(f"Loaded {csv_path.name} with encoding: {enc}")
            break
        except Exception as e:
            tried.append(f"{enc}: {e}")
            df = None

    if df is None:
        st.error("Could not read the CSV with common encodings:\n" + "\n".join(tried))
        st.stop()

    df.columns = [c.lower() for c in df.columns]
    return df

df = load_data(DATA_CSV)

# ---------------- Helpers ----------------
def _status_from_row(class_val, note_val) -> str:
    c = "" if pd.isna(class_val) else str(class_val).strip().lower()
    n = "" if pd.isna(note_val) else str(note_val).strip()
    if c in {"active", "eligible"}: return "eligible"
    if "out" in c: return "out_of_network"
    if "not" in c or "inactive" in c: return "not_active"
    if n == "1680 ": return "out_of_network"
    if n == "1658 ": return "not_active"
    return "none"

def _badge_from_status(status: str) -> str:
    cls = {"eligible":"badge-ok", "out_of_network":"badge-warn", "not_active":"badge-err"}.get(status, "badge")
    label = {"eligible":"Eligible", "out_of_network":"Out of network", "not_active":"Not active"}.get(status, status.title() if status else "Unknown")
    return f'<span class="badge {cls}">{label}</span>'

# ---------------- Controls ----------------
visit_ids = sorted(df["visit_id"].astype(str).unique().tolist())
selected_visit = st.selectbox(
    "Visit ID",
    visit_ids,
    index=0,
    help="Most recent record is shown",
    label_visibility="visible",
)
st.markdown('</div>', unsafe_allow_html=True)

# ---------------- Display ----------------
record = df[df["visit_id"].astype(str) == selected_visit].sort_values(
    by="insertion_date", ascending=False
).head(1)

if not record.empty:
    row = record.iloc[0]
    status = _status_from_row(row.get("class"), row.get("note"))

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
          <div class="kv-item"><div class="kv-label">Visit</div><div class="kv-value">{selected_visit}</div></div>
          <div class="kv-item"><div class="kv-label">Status</div><div class="kv-value">{row.get('class','')}</div></div>
          <div class="kv-item"><div class="kv-label">Note</div><div class="kv-value">{row.get('note','')}</div></div>
          <div class="kv-item"><div class="kv-label">Check Date</div><div class="kv-value">{row.get('insertion_date','')}</div></div>
          <div class="kv-item"><div class="kv-label">Eligibility Status</div><div class="kv-value">{row.get('eligibility_status','')}</div></div>
          <div class="kv-item"><div class="kv-label">Eligibility Note</div><div class="kv-value">{row.get('eligibility_note','')}</div></div>
          <div class="kv-item"><div class="kv-label">Eligibility Requested Date</div><div class="kv-value">{row.get('eligibility_requested_date','')}</div></div>
          <div class="kv-item"><div class="kv-label">Iqama No</div><div class="kv-value">{row.get('iqama_no','')}</div></div>
          <div class="kv-item"><div class="kv-label">Insurance Company</div><div class="kv-value">{row.get('insurance company','')}</div></div>
          <div class="kv-item"><div class="kv-label">Policy Number</div><div class="kv-value">{row.get('policy number','')}</div></div>
          <div class="kv-item"><div class="kv-label">Class Name</div><div class="kv-value">{row.get('class name','')}</div></div>
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