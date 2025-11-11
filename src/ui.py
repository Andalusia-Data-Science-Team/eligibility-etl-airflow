# src/ui.py
import os
import streamlit as st

# ------------------------ Brand + Medical Palette ------------------------
TOKENS = {
    # Surface & text
    "bg": "#f7fafc",  # canvas
    "paper": "#ffffff",  # cards
    "ink": "#0f172a",  # headings
    "muted_ink": "#475569",  # body
    # Andalusia accents (used subtly)
    "brand_brown": "#7c4c24",
    "brand_brown_2": "#B87c4c",
    "brand_tint": "#EBD9D1",
    # Functional/medical semantics
    "primary": "#2563eb",  # solid actions (blue-600)
    "primary_hover": "#1d4ed8",
    "primary_soft": "#e0ecff",
    "success_bg": "#e8f7ee",
    "success_fg": "#166534",
    "success_bd": "#34d399",
    "warn_bg": "#fff7e6",
    "warn_fg": "#7c2d12",
    "warn_bd": "#fbbf24",
    "error_bg": "#feecec",
    "error_fg": "#7f1d1d",
    "error_bd": "#f87171",
    "neutral_bd": "rgba(15, 23, 42, .06)",
    "shadow_sm": "0 1px 3px rgba(16,24,40,.08)",
    "shadow_md": "0 6px 24px rgba(2,6,23,.08)",
    "focus": "#93c5fd",  # focus ring
}


def _css_vars():
    return ";".join([f"--{k}:{v}" for k, v in TOKENS.items()])


# ------------------------ Global Theme Injection ------------------------
def inject_theme():
    st.markdown(
        f"""
<style>
:root{{{_css_vars()};}}
html, body, [data-testid="stAppViewContainer"]{{
  background:var(--bg); color:var(--ink);
  font-family: ui-sans-serif, -apple-system, Segoe UI, Roboto, Helvetica, Arial;
}}

/* Cap max content width for readability (1180–1280px) */
.block-container{{max-width:1280px; margin:0 auto;}}

/* Hide Streamlit's built-in multipage nav (avoid duplicates) */
[data-testid="stSidebarNav"]{{display:none!important;}}

/* ---------- Header (reduced height, logo on right) ---------- */
.app-header{{
  display:flex; align-items:center; gap:1rem;
  padding:.85rem 1.1rem;
  border-radius:14px;
  background:linear-gradient(180deg,var(--paper),#fbfbfb);
  border:1px solid var(--neutral_bd);
  box-shadow:var(--shadow_sm);
  margin:.35rem 0 1rem; position:relative; overflow:hidden;
}}
.app-header:after{{
  content:""; position:absolute; right:-80px; top:-60px; width:240px; height:240px; border-radius:50%;
  background: radial-gradient(closest-side,var(--brand_tint),transparent 70%); opacity:.5;
}}
.app-header h1{{margin:0; font-weight:900; letter-spacing:.2px; color:var(--brand_brown);}}
.app-header p{{margin:.1rem 0 0; color:var(--muted_ink);}}

/* ---------- Cards ---------- */
.card{{
  background:var(--paper); border:1px solid var(--neutral_bd);
  border-radius:14px; padding:1rem 1.25rem; box-shadow:var(--shadow_sm);
}}
.card:hover{{box-shadow:var(--shadow_md); transition:box-shadow .25s ease;}}

/* ---------- Icon pills ---------- */
.icon-pill{{
  display:inline-flex; align-items:center; justify-content:center;
  width:32px; height:32px; border-radius:10px; font-size:16px;
  border:1px solid var(--neutral_bd); box-shadow:var(--shadow_sm);
  transition:transform .12s ease, box-shadow .25s ease;
  filter: grayscale(100%);
}}
.icon-pill:hover{{transform:translateY(-1px); box-shadow:var(--shadow_md);}}
.icon-medical{{background:#efe6df;}}
.icon-action{{background:#ece9e6;}}
.icon-analytics{{background:#eef2ff;}}
.icon-search{{background:#edf2f7;}}

/* ---------- Status badges ---------- */
.badge{{display:inline-block; padding:.38rem .65rem; border-radius:.65rem; font-weight:700; font-size:.85rem; border:1px solid transparent}}
.badge-ok{{background:var(--success_bg); color:var(--success_fg); border-color:var(--success_bd);}}
.badge-warn{{background:var(--warn_bg); color:var(--warn_fg); border-color:var(--warn_bd);}}
.badge-err{{background:var(--error_bg); color:var(--error_fg); border-color:var(--error_bd);}}

/* ---------- Tabs ---------- */
.stTabs [data-baseweb="tab-list"] button{{
  background:var(--paper); border:1px solid var(--neutral_bd);
  margin-right:.5rem; border-radius:12px; padding:.55rem 1rem;
  transition:all .15s ease; font-weight:700; color:#334155;
}}
.stTabs [data-baseweb="tab-list"] button:hover{{box-shadow:var(--shadow_sm); transform:translateY(-1px);}}
.stTabs [aria-selected="true"]{{
  border-color:var(--primary)!important; color:var(--primary)!important; position:relative;
  box-shadow: 0 -2px 6px rgba(37, 99, 235, .08) inset;
}}
.stTabs [aria-selected="true"]::after{{
  content:""; position:absolute; left:12%; right:12%; bottom:-3px; height:3px; border-radius:2px; background:var(--primary);
}}

/* ---------- Buttons ---------- */
.stButton > button{{
  background:var(--primary); color:#fff; border:1px solid var(--primary); border-radius:10px;
  padding:.6rem 1.05rem; font-weight:700; letter-spacing:.2px;
  box-shadow:0 1px 2px rgba(16,24,40,.12); transition:transform .06s ease, box-shadow .2s ease, filter .1s ease;
}}
.stButton > button:hover{{filter:brightness(1.02); box-shadow:0 3px 12px rgba(16,24,40,.18);}}
.stButton > button:active{{transform:translateY(1px);}}

/* Outline for downloads/neutral */
.stDownloadButton > button{{
  background:#fff; color:var(--primary); border:1.5px solid var(--primary); border-radius:10px; padding:.6rem 1.05rem; font-weight:700
}}
.stDownloadButton > button:hover{{border-color:var(--primary_hover);}}

/* ---------- Inputs focus ---------- */
input:focus, textarea:focus, select:focus, button:focus, [role="button"]:focus{{
  outline:3px solid var(--focus)!important; outline-offset:2px!important;
}}

/* ---------- Dataframe/Table polish ---------- */
div[data-testid="stDataFrame"]{{
  border-radius:12px; overflow:hidden; border:1px solid var(--neutral_bd); box-shadow:var(--shadow_sm);
}}
div[data-testid="stDataFrame"] thead th{{font-weight:600!important;}}
div[data-testid="stDataFrame"] tbody tr{{height:44px;}}
div[data-testid="stDataFrame"] tbody tr:nth-child(even){{background: rgba(15,23,42,.02);}}

/* ---------- Sidebar ---------- */
[data-testid="stSidebar"] > div:first-child{{padding-top:.4rem;}}
.sb-logo img{{max-width:140px; height:auto;}}
.sb-card{{background:var(--paper); border:1px solid var(--neutral_bd); border-radius:14px; padding:.85rem 1rem; box-shadow:var(--shadow_sm);}}
.sb-item{{display:flex; align-items:center; gap:.5rem; padding:.5rem .65rem; border-radius:10px; color:#334155; font-weight:600;}}
.sb-item.active{{background:var(--primary_soft); color:var(--primary); border:1px solid var(--primary);}}

/* ---------- Make Streamlit wrappers transparent (kills the white strip) ---------- */
[data-testid="stForm"],
[data-testid="stTabs"],
[data-testid="stTab"],
[data-testid="stVerticalBlock"],
[data-testid="stHorizontalBlock"],
[data-testid="stTextInput"],
[data-testid="stSelectbox"],
[data-testid="stDataFrame"],
.block-container {{
    background: transparent !important;
    box-shadow: none !important;
}}
[data-testid="stAppViewContainer"] > .main {{
    background: transparent !important;
}}
section.main > div {{
    background: transparent !important;
}}

/* ---------- Footer ---------- */
.footer{{margin-top:2rem; padding-top:1rem; border-top:1px solid var(--neutral_bd); text-align:center; color:var(--muted_ink);}}

/* ---------- Header logo consistency ---------- */
.app-header img{{max-width:180px; height:auto; mix-blend-mode:multiply; background-color:transparent !important;}}

/* Easy fix: hide the extra white bar (blank input field) */
[data-testid="stTextInput"]:has(input:placeholder-shown):not(:has(label:not(:empty))) {{
    display: none !important;
}}

/* Hide any unlabeled, empty text inputs (the white bar under section titles) */
section.main [data-testid="stTextInput"]:has(> label:empty) {{
    display: none !important;
}}

/* Fallback for browsers without :has() support */
section.main [data-testid="stTextInput"] > label:empty {{ display: none !important; }}
section.main [data-testid="stTextInput"] > label:empty + div {{ display: none !important; }}

/* Kill accidental blank "cards" (the white bar) */
.surface-card:empty {{
  display: none !important;
  padding: 0 !important;
  margin: 0 !important;
  border: 0 !important;
  box-shadow: none !important;
}}

</style>
""",
        unsafe_allow_html=True,
    )

    st.markdown(
        """
    <script>
    (function () {
      function hideEmptyCards() {
        document.querySelectorAll('.surface-card').forEach(function (el) {
          // Treat as empty if no element children AND no non-whitespace text
          var hasElementChild = Array.from(el.childNodes).some(function(n){ return n.nodeType === 1; });
          var hasText = Array.from(el.childNodes).some(function(n){ return n.nodeType === 3 && n.textContent.trim().length > 0; });
          if (!hasElementChild && !hasText) {
            el.style.display = 'none';
            el.style.padding = '0';
            el.style.margin = '0';
            el.style.border = '0';
            el.style.boxShadow = 'none';
          }
        });
      }
      hideEmptyCards();
      // Catch late/dynamic renders
      new MutationObserver(hideEmptyCards).observe(document.body, { childList: true, subtree: true });
    })();
    </script>
    """,
        unsafe_allow_html=True,
    )


# ------------------------ Page Setup ------------------------
def setup_page(title: str, icon_path: str = "assets/your_company_logo.png"):
    # Use the logo as favicon/page icon
    st.set_page_config(page_title=title, page_icon=icon_path, layout="wide")
    inject_theme()


# ------------------------ Header with logo on RIGHT ------------------------
def app_header(
    title: str,
    subtitle: str,
    icon_emoji: str,
    icon_class: str = "icon-medical",
    logo_path: str = "assets/your_company_logo.png",
    logo_width: int = 160,
):
    # flip columns: text left, logo right
    col_text, col_logo = st.columns([0.78, 0.22], vertical_alignment="center")
    with col_text:
        st.markdown(
            f"""
            <div class="app-header">
              <div style="display:flex;align-items:center;gap:.8rem;">
                <span class="icon-pill {icon_class}">{icon_emoji}</span>
                <div>
                  <h1>{title}</h1>
                  <p>{subtitle}</p>
                </div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_logo:
        st.markdown("<div style='text-align:right;'>", unsafe_allow_html=True)
        if os.path.exists(logo_path):
            st.image(logo_path, width=logo_width)
        st.markdown("</div>", unsafe_allow_html=True)


# ------------------------ Sidebar Navigation (single system) ------------------------
def sidebar_nav(active: str):
    with st.sidebar:
        st.markdown(
            '<div class="sb-logo" style="text-align:center;margin:.25rem 0 1rem">',
            unsafe_allow_html=True,
        )
        st.image("assets/your_company_logo.png", use_container_width=True)
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown('<div class="sb-card">', unsafe_allow_html=True)
        items = [
            ("Eligibility", "🏥", "pages/1_Eligibility.py"),
            ("Predictions", "🩺", "pages/2_Predictions.py"),
            ("Resubmission", "📤", "pages/3_Resubmission.py"),
        ]
        for label, emoji, path in items:
            is_active = label == active
            st.markdown(
                f'<div class="sb-item {"active" if is_active else ""}">'
                f'<span class="icon-pill icon-search">{emoji}</span>{label}</div>',
                unsafe_allow_html=True,
            )
            if not is_active:
                if st.button(f"Open {label}", key=f"nav-{label}"):
                    st.switch_page(path)
        st.markdown("</div>", unsafe_allow_html=True)


# ------------------------ Small HTML helpers ------------------------
def open_card():
    st.markdown('<div class="card">', unsafe_allow_html=True)


def close_card():
    st.markdown("</div>", unsafe_allow_html=True)


def status_badge(status: str) -> str:
    m = {
        "eligible": "badge-ok",
        "success": "badge-ok",
        "warning": "badge-warn",
        "out_of_network": "badge-warn",
        "not_active": "badge-err",
        "error": "badge-err",
    }
    cls = m.get(status.lower(), "badge")
    label = status.replace("_", " ").title()
    return f'<span class="badge {cls}">{label}</span>'
