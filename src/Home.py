import streamlit as st
from datetime import datetime
from pathlib import Path
import base64

# ---------- Page Setup ----------
st.set_page_config(page_title="Claims Copilot", page_icon="💊", layout="wide")


# ---------- Locate logo & build data URI (so it always renders) ----------
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

# ---------- Palette ----------
PALETTE = {
    "bg": "#f8fafc",  # soft neutral background
    "paper": "#f8f5f2",  # warm paper tone
    "brand": "#7c4c24",  # Andalusia brown
    "brand_hover": "#9a6231",  # hover tone
    "muted": "#64748b",  # neutral text
}

# ---------- CSS ----------
st.markdown(
    f"""
<style>
html, body, [data-testid="stAppViewContainer"] {{
    background: {PALETTE['bg']};
}}

.header-box {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    background: {PALETTE['paper']};
    border: 1px solid rgba(0,0,0,0.06);
    border-radius: 16px;
    box-shadow: 0 2px 6px rgba(0,0,0,0.06);
    padding: 1.3rem 1.8rem;
    margin-bottom: 1rem;
}}

.header-box h1 {{
    color: {PALETTE['brand']};
    font-weight: 800;
    font-size: 2rem;
    margin: 0;
}}

.header-box p {{
    color: {PALETTE['muted']};
    font-size: 0.95rem;
    margin-top: .35rem;
}}

.logo-right {{
    text-align: right;
}}
.logo-right img {{
    max-width: 170px;
    height: auto;
    mix-blend-mode: multiply;         /* makes white areas blend out */
    background-color: transparent !important;
}}

.subhead {{
    font-size: 0.9rem;
    font-weight: 600;
    color: #475569;
    margin: 0.4rem 0 1rem;
    letter-spacing: 0.2px;
    text-align: center;
}}

.stButton > button {{
    width: 100%;
    background: {PALETTE['brand']} !important;
    color: #fff !important;
    border: 1px solid {PALETTE['brand']} !important;
    border-radius: 12px !important;
    font-weight: 700 !important;
    font-size: 0.95rem !important;
    padding: 0.7rem 1rem !important;
    box-shadow: 0 2px 6px rgba(0,0,0,0.08);
    transition: all 0.2s ease-in-out;
}}
.stButton > button:hover {{
    background: {PALETTE['brand_hover']} !important;
    border-color: {PALETTE['brand_hover']} !important;
    transform: translateY(-2px);
    box-shadow: 0 6px 16px rgba(0,0,0,0.12);
}}
.stButton > button:active {{
    background: #6a3f1e !important;
    transform: translateY(0);
}}

.card.surface {{
    background: #ffffff;
    border: 1px solid rgba(0,0,0,0.05);
    border-radius: 12px;
    padding: 1rem 1.2rem;
    box-shadow: 0 2px 6px rgba(0,0,0,0.04);
    text-align: center;
    transition: transform 0.15s ease, box-shadow 0.2s ease;
    height: 98px;
}}
.card.surface:hover {{
    transform: translateY(-2px);
    box-shadow: 0 6px 18px rgba(0,0,0,0.08);
}}
.card p {{
    margin: 0;
    color: #444;
    font-size: 0.93rem;
    line-height: 1.35;
}}
.center-wrap {{
    max-width: 980px;
    margin: 0 auto;
}}
</style>
""",
    unsafe_allow_html=True,
)

# ---------- Header Box ----------
st.markdown(
    f"""
    <div class="header-box">
        <div>
            <h1>Claims Copilot</h1>
            <p>Eligibility → Predictions → Resubmission</p>
        </div>
        <div class="logo-right">
            {'<img src="' + _LOGO_URI + '" alt="Andalusia Health Logo"/>' if _LOGO_URI else ''}
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ---------- Navigation ----------
st.markdown('<p class="subhead">Select a section to begin</p>', unsafe_allow_html=True)

sp_l, content, sp_r = st.columns([1, 8, 1])
with content:
    st.markdown('<div class="center-wrap">', unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)

    with c1:
        if st.button("Eligibility", key="btn-elig", use_container_width=True):
            st.switch_page("pages/1_Eligibility.py")
        st.markdown(
            '<div class="card surface"><p>Check patient eligibility and payer class.</p></div>',
            unsafe_allow_html=True,
        )

    with c2:
        if st.button("Predictions", key="btn-pred", use_container_width=True):
            st.switch_page("pages/2_Predictions.py")
        st.markdown(
            '<div class="card surface"><p>Run AI predictions for claims.</p></div>',
            unsafe_allow_html=True,
        )

    with c3:
        if st.button("Resubmission", key="btn-resub", use_container_width=True):
            st.switch_page("pages/3_Resubmission.py")
        st.markdown(
            '<div class="card surface"><p>Generate justifications and export.</p></div>',
            unsafe_allow_html=True,
        )

# ---------- Footer ----------
st.markdown(
    f'<p style="text-align:center;color:#94a3b8;margin-top:2rem">©️ {datetime.now().year} Claims Copilot</p>',
    unsafe_allow_html=True,
)
