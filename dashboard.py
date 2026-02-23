"""
Austin Multifamily Intelligence Dashboard
==========================================
Run with: streamlit run dashboard.py

Tabs:
  1. Market Overview    — Supply pressure score by submarket
  2. Supply Pipeline    — Under construction + projected delivery dates
  3. Absorption         — Delivery volume vs vacancy over time
  4. Timing Intelligence — Buy/sell signals by submarket
  5. Permit Browser     — Raw CO data, searchable
"""

import io
import os
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN

load_dotenv()

DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/austin_co")

# ─────────────────────────────────────────────────────────────────────────────
# COSTAR DATA — update these from your GeographyList.xlsx periodically
# ─────────────────────────────────────────────────────────────────────────────
COSTAR_DATA = {
    "North Austin": {
        "vacancy": 0.140,
        "rent_growth": -0.063,
        "inventory": 31903,
        "under_constr": 1065,
        "delivered_12mo": 374,
        "asking_rent": 1395,
        "absorption_12mo": 1379,
        "avg_days_on_market": 58,
        "concession_pct": 0.065,
    },
    "Northwest Austin": {
        "vacancy": 0.110,
        "rent_growth": -0.059,
        "inventory": 35400,
        "under_constr": 586,
        "delivered_12mo": 129,
        "asking_rent": 1343,
        "absorption_12mo": 364,
        "avg_days_on_market": 48,
        "concession_pct": 0.055,
    },
    "East Austin": {
        "vacancy": 0.138,
        "rent_growth": -0.041,
        "inventory": 19201,
        "under_constr": 2328,
        "delivered_12mo": 792,
        "asking_rent": 1774,
        "absorption_12mo": 1636,
        "avg_days_on_market": 55,
        "concession_pct": 0.050,
    },
    "Downtown Austin": {
        "vacancy": 0.138,
        "rent_growth": 0.012,
        "inventory": 9001,
        "under_constr": 352,
        "delivered_12mo": 1254,
        "asking_rent": 3575,
        "absorption_12mo": 994,
        "avg_days_on_market": 62,
        "concession_pct": 0.045,
    },
    "Pflugerville": {
        "vacancy": 0.139,
        "rent_growth": -0.067,
        "inventory": 23825,
        "under_constr": 444,
        "delivered_12mo": 752,
        "asking_rent": 1386,
        "absorption_12mo": 1074,
        "avg_days_on_market": 60,
        "concession_pct": 0.070,
    },
    "Northeast Austin": {
        "vacancy": 0.213,
        "rent_growth": -0.057,
        "inventory": 19387,
        "under_constr": 2436,
        "delivered_12mo": 3102,
        "asking_rent": 1397,
        "absorption_12mo": 2138,
        "avg_days_on_market": 82,
        "concession_pct": 0.105,
    },
    "South Austin": {
        "vacancy": 0.119,
        "rent_growth": -0.056,
        "inventory": 21649,
        "under_constr": 1063,
        "delivered_12mo": 930,
        "asking_rent": 1386,
        "absorption_12mo": 955,
        "avg_days_on_market": 52,
        "concession_pct": 0.060,
    },
    "Round Rock": {
        "vacancy": 0.105,
        "rent_growth": -0.064,
        "inventory": 21390,
        "under_constr": 217,
        "delivered_12mo": 252,
        "asking_rent": 1426,
        "absorption_12mo": 1400,
        "avg_days_on_market": 44,
        "concession_pct": 0.055,
    },
    "Midtown Austin": {
        "vacancy": 0.127,
        "rent_growth": -0.021,
        "inventory": 17027,
        "under_constr": 1681,
        "delivered_12mo": 426,
        "asking_rent": 1562,
        "absorption_12mo": 808,
        "avg_days_on_market": 54,
        "concession_pct": 0.040,
    },
    "Georgetown-Leander": {
        "vacancy": 0.166,
        "rent_growth": -0.067,
        "inventory": 17489,
        "under_constr": 574,
        "delivered_12mo": 1251,
        "asking_rent": 1514,
        "absorption_12mo": 2053,
        "avg_days_on_market": 65,
        "concession_pct": 0.075,
    },
    "Southeast Austin": {
        "vacancy": 0.209,
        "rent_growth": -0.052,
        "inventory": 16442,
        "under_constr": 571,
        "delivered_12mo": 2335,
        "asking_rent": 1397,
        "absorption_12mo": 1416,
        "avg_days_on_market": 78,
        "concession_pct": 0.095,
    },
    "Riverside": {
        "vacancy": 0.116,
        "rent_growth": -0.065,
        "inventory": 18784,
        "under_constr": 298,
        "delivered_12mo": 401,
        "asking_rent": 1377,
        "absorption_12mo": 542,
        "avg_days_on_market": 55,
        "concession_pct": 0.068,
    },
    "Southwest Austin": {
        "vacancy": 0.113,
        "rent_growth": -0.034,
        "inventory": 13882,
        "under_constr": 949,
        "delivered_12mo": 803,
        "asking_rent": 1687,
        "absorption_12mo": 238,
        "avg_days_on_market": 58,
        "concession_pct": 0.042,
    },
    "Cedar Park": {
        "vacancy": 0.116,
        "rent_growth": -0.054,
        "inventory": 15829,
        "under_constr": 0,
        "delivered_12mo": 391,
        "asking_rent": 1438,
        "absorption_12mo": 905,
        "avg_days_on_market": 50,
        "concession_pct": 0.058,
    },
    "South Central Austin": {
        "vacancy": 0.116,
        "rent_growth": -0.035,
        "inventory": 13679,
        "under_constr": 572,
        "delivered_12mo": 570,
        "asking_rent": 1740,
        "absorption_12mo": 210,
        "avg_days_on_market": 53,
        "concession_pct": 0.040,
    },
    "Buda-Kyle": {
        "vacancy": 0.157,
        "rent_growth": -0.046,
        "inventory": 11355,
        "under_constr": 280,
        "delivered_12mo": 798,
        "asking_rent": 1445,
        "absorption_12mo": 992,
        "avg_days_on_market": 63,
        "concession_pct": 0.072,
    },
    "San Marcos": {
        "vacancy": 0.214,
        "rent_growth": -0.051,
        "inventory": 10828,
        "under_constr": 807,
        "delivered_12mo": 725,
        "asking_rent": 1261,
        "absorption_12mo": 1151,
        "avg_days_on_market": 80,
        "concession_pct": 0.090,
    },
    "Far North Austin": {
        "vacancy": 0.262,
        "rent_growth": -0.033,
        "inventory": 4032,
        "under_constr": 336,
        "delivered_12mo": 917,
        "asking_rent": 1561,
        "absorption_12mo": 701,
        "avg_days_on_market": 95,
        "concession_pct": 0.115,
    },
    "Lake Travis": {
        "vacancy": 0.141,
        "rent_growth": -0.030,
        "inventory": 3978,
        "under_constr": 0,
        "delivered_12mo": 322,
        "asking_rent": 1813,
        "absorption_12mo": 38,
        "avg_days_on_market": 68,
        "concession_pct": 0.048,
    },
    "Central Austin": {
        "vacancy": 0.092,
        "rent_growth": -0.021,
        "inventory": 3971,
        "under_constr": 0,
        "delivered_12mo": 38,
        "asking_rent": 1578,
        "absorption_12mo": -10,
        "avg_days_on_market": 42,
        "concession_pct": 0.030,
    },
    "West Austin": {
        "vacancy": 0.061,
        "rent_growth": -0.006,
        "inventory": 2152,
        "under_constr": 168,
        "delivered_12mo": 0,
        "asking_rent": 2009,
        "absorption_12mo": 14,
        "avg_days_on_market": 32,
        "concession_pct": 0.018,
    },
    "Far West Austin": {
        "vacancy": 0.054,
        "rent_growth": 0.008,
        "inventory": 149,
        "under_constr": 0,
        "delivered_12mo": 0,
        "asking_rent": 1645,
        "absorption_12mo": 0,
        "avg_days_on_market": 28,
        "concession_pct": 0.015,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# MATTHEWS BRAND COLORS
# ─────────────────────────────────────────────────────────────────────────────
ACCENT    = "#C8102E"   # Matthews red — highlights, active tabs, key metrics
NAVY      = "#1A1A2E"   # Deep navy/charcoal — headers and primary text
GREEN     = "#16A34A"   # Success / BUY
AMBER     = "#D97706"   # Warning / HOLD
RED       = "#C8102E"   # Danger / SELL (same as accent)
BG        = "#FFFFFF"   # Primary background — white
CARD_BG   = "#F8F9FA"   # Card background — light gray
BORDER    = "#E5E7EB"   # Subtle gray border
TEXT      = "#1A1A2E"   # Primary text — navy
MUTED     = "#6B7280"   # Secondary text — gray

PLOTLY_LAYOUT = dict(
    paper_bgcolor=BG,
    plot_bgcolor=BG,
    font=dict(color=TEXT, family="'DM Sans', sans-serif"),
    xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, tickfont=dict(color=MUTED)),
    yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER, tickfont=dict(color=MUTED)),
    legend=dict(bgcolor="rgba(255,255,255,0.9)", bordercolor=BORDER, font=dict(color=TEXT)),
    margin=dict(t=40, r=20, b=40, l=60),
)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="Austin MF Intelligence", page_icon="🏢", layout="wide")

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Inter:wght@300;400;500;600;700&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {{ background-color: {BG}; color: {TEXT}; font-family: 'DM Sans', sans-serif; }}
.main {{ background-color: {BG}; }}
.block-container {{ padding: 1.5rem 1rem; max-width: 1600px; }}
.dash-header {{ font-family: 'Inter', sans-serif; font-size: clamp(1.2rem, 3vw, 2.2rem); font-weight: 700; letter-spacing: 0.02em; color: {NAVY}; line-height: 1.1; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
.dash-header span {{ color: {ACCENT}; }}
.dash-sub {{ font-family: 'DM Mono', monospace; font-size: 0.72rem; color: {MUTED}; letter-spacing: 0.18em; text-transform: uppercase; margin-bottom: 1.5rem; }}
.kpi-card {{ background: {CARD_BG}; border: 1px solid {BORDER}; border-left: 3px solid {ACCENT}; border-radius: 4px; padding: 1rem 1.2rem; margin-bottom: 1rem; }}
.kpi-label {{ font-family: 'DM Mono', monospace; font-size: 0.65rem; color: {MUTED}; letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 0.4rem; }}
.kpi-value {{ font-family: 'Inter', sans-serif; font-size: 1.9rem; font-weight: 700; color: {NAVY}; line-height: 1; }}
.section-title {{ font-family: 'DM Mono', monospace; font-size: 0.65rem; color: {MUTED}; letter-spacing: 0.2em; text-transform: uppercase; border-bottom: 1px solid {BORDER}; padding-bottom: 0.5rem; margin-bottom: 1rem; }}
.stTabs [data-baseweb="tab-list"] {{ background-color: {BG}; border-bottom: 1px solid {BORDER}; gap: 0; }}
.stTabs [data-baseweb="tab"] {{ font-family: 'DM Mono', monospace; font-size: 0.7rem; letter-spacing: 0.12em; text-transform: uppercase; color: {MUTED}; padding: 0.75rem 1.5rem; border-bottom: 2px solid transparent; background: transparent; }}
.stTabs [aria-selected="true"] {{ color: {ACCENT} !important; border-bottom: 2px solid {ACCENT} !important; background: transparent !important; }}
.stDataFrame {{ border: 1px solid {BORDER}; border-radius: 4px; }}
input, select, textarea {{ background-color: {CARD_BG} !important; color: {TEXT} !important; border: 1px solid {BORDER} !important; border-radius: 4px !important; }}
::-webkit-scrollbar {{ width: 4px; height: 4px; }}
::-webkit-scrollbar-track {{ background: {BG}; }}
::-webkit-scrollbar-thumb {{ background: {BORDER}; border-radius: 2px; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# COSTAR XLSX UPLOADER (sidebar)
# ─────────────────────────────────────────────────────────────────────────────
COSTAR_COL_MAP = {
    "Vacancy Rate": "vacancy",
    "Market Asking Rent/Unit": "asking_rent",
    "Market Asking Rent Growth": "rent_growth",
    "Under Construction Units": "under_constr",
    "12 Mo Delivered Units": "delivered_12mo",
    "12 Mo Absorbed Units": "absorption_12mo",
    "Inventory Units": "inventory",
    "Avg Days on Market": "avg_days_on_market",
    "Concession (% of Asking Rent)": "concession_pct",
}

with st.sidebar:
    st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.65rem;color:{MUTED};letter-spacing:0.15em;text-transform:uppercase;margin-bottom:0.5rem;">Update CoStar Data</div>', unsafe_allow_html=True)
    uploaded = st.file_uploader("Upload GeographyList.xlsx", type=["xlsx"], label_visibility="collapsed")
    if uploaded is not None:
        try:
            xls = pd.read_excel(uploaded, sheet_name=0)
            # Identify submarket name column (usually "Geography Name" or first col)
            name_col = None
            for c in xls.columns:
                cl = c.strip().lower()
                if cl in ("geography name", "submarket", "submarket name", "name"):
                    name_col = c
                    break
            if name_col is None:
                name_col = xls.columns[0]

            updated_count = 0
            for _, row in xls.iterrows():
                sm_name = str(row[name_col]).strip()
                if sm_name not in COSTAR_DATA:
                    continue
                entry = COSTAR_DATA[sm_name]
                for xlsx_col, field in COSTAR_COL_MAP.items():
                    matched_col = None
                    for c in xls.columns:
                        if xlsx_col.lower() in c.lower():
                            matched_col = c
                            break
                    if matched_col is not None and pd.notna(row[matched_col]):
                        val = float(row[matched_col])
                        # CoStar exports rates as percentages (e.g. 14.0 not 0.14)
                        if field in ("vacancy", "rent_growth", "concession_pct") and abs(val) > 1:
                            val = val / 100.0
                        entry[field] = val
                updated_count += 1
            st.success(f"Updated {updated_count} submarkets")
        except Exception as e:
            st.error(f"Error reading file: {e}")

# ─────────────────────────────────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_permits(min_year=2010):
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql(f"""
        SELECT permit_num, issue_date, address, zip_code, latitude, longitude,
               total_units, project_name, work_class, submarket_name,
               delivery_year, delivery_quarter, delivery_yyyyq
        FROM co_projects
        WHERE total_units >= 5 AND delivery_year >= {min_year} AND issue_date IS NOT NULL
        ORDER BY issue_date DESC
    """, conn)
    conn.close()
    df["issue_date"] = pd.to_datetime(df["issue_date"])
    return df

@st.cache_data(ttl=300)
def load_quarterly():
    conn = psycopg2.connect(DB_DSN)
    df = pd.read_sql("""
        SELECT submarket_name, delivery_year, delivery_quarter,
               delivery_yyyyq, project_count, total_units_delivered
        FROM submarket_deliveries WHERE delivery_year >= 2010 ORDER BY delivery_yyyyq
    """, conn)
    conn.close()
    return df

def get_costar_df():
    rows = [{"submarket_name": k, **v} for k, v in COSTAR_DATA.items()]
    return pd.DataFrame(rows)

def pressure_score(row):
    # Existing factors (scaled back to make room for new signals)
    v = min((row["vacancy"] - 0.08) / 0.15, 1.0) * 25          # 25pts vacancy
    d = min(row["delivered_12mo"] / max(row["inventory"], 1) / 0.12, 1.0) * 20  # 20pts deliveries
    u = min(row["under_constr"] / max(row["inventory"], 1) / 0.15, 1.0) * 20   # 20pts pipeline
    r = min((-row["rent_growth"]) / 0.08, 1.0) * 15             # 15pts rent growth

    # New factors
    # Absorption: low absorption vs deliveries = pressure (clamped 0-10)
    absorption_ratio = row.get("absorption_12mo", 0) / max(row["delivered_12mo"], 1)
    a = max(0.0, min((1 - absorption_ratio) / 0.5, 1.0)) * 10   # 10pts absorption

    # Days on market: above 45 days = pressure signal (clamped 0-5)
    dom = row.get("avg_days_on_market", 45)
    dom_score = max(0.0, min((dom - 45) / 60, 1.0)) * 5         # 5pts days on market

    # Concessions: above 4% = distress signal (clamped 0-5)
    conc = row.get("concession_pct", 0)
    conc_score = max(0.0, min(max(conc - 0.04, 0) / 0.10, 1.0)) * 5  # 5pts concessions

    return round(max(0, v + d + u + r + a + dom_score + conc_score), 1)

def sig(score):
    if score >= 60: return "SELL"
    if score >= 35: return "HOLD"
    return "BUY"

def sig_color(score):
    if score >= 60: return RED
    if score >= 35: return AMBER
    return GREEN

try:
    df = load_permits()
    dq = load_quarterly()
    db_ok = True
except Exception as e:
    st.error(f"DB error: {e}")
    df = pd.DataFrame()
    dq = pd.DataFrame()
    db_ok = False

dc = get_costar_df()
dc["score"] = dc.apply(pressure_score, axis=1)
dc["signal"] = dc["score"].apply(sig)

# ─────────────────────────────────────────────────────────────────────────────
# HEADER
# ─────────────────────────────────────────────────────────────────────────────
h1, h2 = st.columns([3, 1])
with h1:
    st.markdown('<div class="dash-header">AUSTIN <span>MULTIFAMILY</span> INTELLIGENCE</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="dash-sub">C-105 Certificates of Occupancy · {len(df):,} permits · {datetime.now().strftime("%b %d, %Y")}</div>', unsafe_allow_html=True)
with h2:
    st.markdown("<br>", unsafe_allow_html=True)
    yr = st.selectbox("", ["All Time", "Last 5 Years", "Last 3 Years", "Last 12 Months", "Last 6 Months"], label_visibility="collapsed")

# Year-based cutoffs (for delivery_year filtering)
_year_cutoff_map = {
    "All Time":      2000,
    "Last 5 Years":  datetime.now().year - 5,
    "Last 3 Years":  datetime.now().year - 3,
    "Last 12 Months": None,
    "Last 6 Months":  None,
}
# Date-based cutoffs (for issue_date filtering — more precise)
_date_cutoff_map = {
    "Last 12 Months": (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d'),
    "Last 6 Months":  (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d'),
}

if yr in _date_cutoff_map:
    _cutoff_date = _date_cutoff_map[yr]
    df_f = df[df["issue_date"] >= _cutoff_date] if not df.empty else df
    # For quarterly data, approximate using delivery_year from the date cutoff
    _cutoff_year = int(_cutoff_date[:4])
    dq_f = dq[dq["delivery_year"] >= _cutoff_year] if not dq.empty else dq
else:
    _cutoff_year = _year_cutoff_map.get(yr, 2000)
    df_f = df[df["delivery_year"] >= _cutoff_year] if not df.empty else df
    dq_f = dq[dq["delivery_year"] >= _cutoff_year] if not dq.empty else dq

# KPIs
k1, k2, k3, k4, k5 = st.columns(5)
for col, label, val in [
    (k1, "Units Delivered",   f"{int(df_f['total_units'].sum()):,}" if not df_f.empty else "—"),
    (k2, "Projects",          f"{len(df_f):,}" if not df_f.empty else "—"),
    (k3, "Avg Project Size",  f"{int(df_f['total_units'].mean())}" if not df_f.empty else "—"),
    (k4, "Active Submarkets", f"{df_f['submarket_name'].nunique()}" if not df_f.empty else "—"),
    (k5, "Sell Signal Mkts",  f"{len(dc[dc['signal']=='SELL'])}"),
]:
    with col:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{val}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
t1, t2, t3, t4, t5 = st.tabs(["  MARKET OVERVIEW  ", "  SUPPLY PIPELINE  ", "  ABSORPTION  ", "  TIMING INTELLIGENCE  ", "  PERMIT BROWSER  "])

# ══════════════ TAB 1 ══════════════
with t1:
    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown('<div class="section-title">Units Delivered by Submarket</div>', unsafe_allow_html=True)
        if not df_f.empty:
            sub = df_f.groupby("submarket_name")["total_units"].sum().sort_values().reset_index()
            fig = go.Figure(go.Bar(
                x=sub["total_units"], y=sub["submarket_name"], orientation="h",
                marker=dict(
                    color=sub["total_units"],
                    colorscale=[[0, "#E5E7EB"], [0.5, "#9CA3AF"], [1, NAVY]],
                    showscale=False
                ),
                text=sub["total_units"].apply(lambda x: f"{x:,}"), textposition="outside",
                textfont=dict(size=10, color=MUTED, family="DM Mono"),
            ))
            fig.update_layout(**PLOTLY_LAYOUT, height=500, xaxis_showgrid=False, xaxis_showticklabels=False, xaxis_zeroline=False)
            st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="section-title">Supply Pressure Score</div>', unsafe_allow_html=True)
        for _, r in dc.sort_values("score", ascending=False).iterrows():
            sc = sig_color(r["score"])
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;padding:8px 10px;background:{CARD_BG};border:1px solid {BORDER};border-radius:4px;">
                <div style="flex:1;font-size:0.78rem;color:{TEXT};">{r['submarket_name']}</div>
                <div style="width:80px;height:4px;background:{BORDER};border-radius:2px;overflow:hidden;">
                    <div style="width:{int(r['score'])}%;height:100%;background:{sc};border-radius:2px;"></div>
                </div>
                <div style="width:28px;font-family:'DM Mono',monospace;font-size:0.7rem;color:{MUTED};text-align:right;">{r['score']:.0f}</div>
                <div style="padding:2px 6px;font-family:'DM Mono',monospace;font-size:0.62rem;border:1px solid {sc};color:{sc};border-radius:3px;">{r['signal']}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Quarterly Deliveries — All Submarkets</div>', unsafe_allow_html=True)
    if not dq_f.empty:
        qa = dq_f.groupby("delivery_yyyyq")["total_units_delivered"].sum().reset_index().sort_values("delivery_yyyyq")
        qa["rolling"] = qa["total_units_delivered"].rolling(4, min_periods=1).mean()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=qa["delivery_yyyyq"], y=qa["total_units_delivered"], marker_color=ACCENT, opacity=0.4, name="Quarterly"))
        fig2.add_trace(go.Scatter(x=qa["delivery_yyyyq"], y=qa["rolling"], mode="lines", line=dict(color=NAVY, width=2), name="4Q Avg"))
        fig2.update_layout(**PLOTLY_LAYOUT, height=260)
        st.plotly_chart(fig2, use_container_width=True)

# ══════════════ TAB 2 ══════════════
with t2:
    st.markdown('<div class="section-title">Under Construction vs Historical Delivery Pace</div>', unsafe_allow_html=True)
    ca, cb = st.columns(2)

    with ca:
        if not df_f.empty:
            pace = df_f[df_f["delivery_year"] >= 2018].groupby("submarket_name")["total_units"].sum().div(24).reset_index().rename(columns={"total_units": "avg_qtr"})
            pipe = dc[["submarket_name","under_constr","delivered_12mo","inventory"]].copy()
            pipe = pipe.merge(pace, on="submarket_name", how="left")
            pipe["avg_qtr"] = pipe["avg_qtr"].fillna(50)
            pipe["months_to_deliver"] = (pipe["under_constr"] / (pipe["avg_qtr"] / 3)).clip(0, 48).round(1)
            pipe = pipe[pipe["under_constr"] > 0].sort_values("under_constr", ascending=False)

            fig3 = go.Figure()
            fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["under_constr"], name="Under Construction", marker_color=NAVY, opacity=0.85))
            fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["delivered_12mo"], name="Delivered Last 12mo", marker_color=ACCENT, opacity=0.7))
            fig3.update_layout(**PLOTLY_LAYOUT, barmode="group", height=380, xaxis_tickangle=-45)
            st.plotly_chart(fig3, use_container_width=True)

    with cb:
        st.markdown('<div class="section-title">Projected Delivery Timeline</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:1rem;">Based on historical CO pace — not CoStar estimates</div>', unsafe_allow_html=True)
        if not df_f.empty:
            for _, r in pipe.head(14).iterrows():
                m = r["months_to_deliver"]
                uc = r["under_constr"]
                urgency_c = RED if m <= 6 else (AMBER if m <= 12 else MUTED)
                urgency_l = "IMMINENT" if m <= 6 else (f"~{m:.0f} MO")
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:5px;padding:8px 10px;background:{CARD_BG};border:1px solid {BORDER};border-left:2px solid {urgency_c};border-radius:4px;">
                    <div style="flex:1;font-size:0.78rem;color:{TEXT};">{r['submarket_name']}</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.68rem;color:{MUTED};">{uc:,.0f} UC</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.68rem;color:{urgency_c};width:75px;text-align:right;">{urgency_l}</div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Annual Delivery Volume — Top 8 Submarkets</div>', unsafe_allow_html=True)
    if not df_f.empty:
        top8 = df_f.groupby("submarket_name")["total_units"].sum().nlargest(8).index.tolist()
        ann = df_f[df_f["submarket_name"].isin(top8)].groupby(["delivery_year","submarket_name"])["total_units"].sum().reset_index()
        colors8 = [NAVY, ACCENT, "#374151", "#6B7280", "#9CA3AF", "#1A1A2E", "#C8102E", "#D97706"]
        fig4 = go.Figure()
        for i, s in enumerate(top8):
            d = ann[ann["submarket_name"] == s]
            fig4.add_trace(go.Scatter(x=d["delivery_year"], y=d["total_units"], name=s, mode="lines+markers", line=dict(color=colors8[i % len(colors8)], width=2), marker=dict(size=5)))
        fig4.update_layout(**PLOTLY_LAYOUT, height=300)
        st.plotly_chart(fig4, use_container_width=True)

# ══════════════ TAB 3 ══════════════
with t3:
    st.markdown('<div class="section-title">Delivery Volume vs Current Vacancy — Bubble = Units Under Construction</div>', unsafe_allow_html=True)
    if not df_f.empty:
        sub_s = df_f.groupby("submarket_name")["total_units"].sum().reset_index()
        abs_df = sub_s.merge(dc, on="submarket_name", how="inner")
        abs_df["score"] = abs_df.apply(pressure_score, axis=1)

        fig5 = go.Figure(go.Scatter(
            x=abs_df["total_units"], y=abs_df["vacancy"] * 100,
            mode="markers+text",
            marker=dict(size=abs_df["under_constr"].apply(lambda x: max(8, min(x/50,40))),
                       color=abs_df["score"], colorscale=[[0,GREEN],[0.5,AMBER],[1,RED]],
                       showscale=True, colorbar=dict(title="Pressure", tickfont=dict(size=9,color=MUTED)),
                       line=dict(width=1,color=BORDER)),
            text=abs_df["submarket_name"].apply(lambda x: x.replace(" Austin","").replace(" County","")),
            textposition="top center", textfont=dict(size=9,color=MUTED,family="DM Mono"),
            hovertemplate="<b>%{text}</b><br>Units: %{x:,}<br>Vacancy: %{y:.1f}%<extra></extra>",
        ))
        fig5.add_hline(y=10, line=dict(color=GREEN,width=1,dash="dot"), annotation_text="10% baseline", annotation_font_color=MUTED)
        fig5.add_hline(y=15, line=dict(color=AMBER,width=1,dash="dot"), annotation_text="15% caution", annotation_font_color=MUTED)
        fig5.add_hline(y=20, line=dict(color=RED,width=1,dash="dot"), annotation_text="20% oversupplied", annotation_font_color=MUTED)
        fig5.update_layout(**PLOTLY_LAYOUT, height=460, xaxis_title="Total Units Delivered (historical)", yaxis_title="Vacancy Rate (%)")
        st.plotly_chart(fig5, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Vacancy vs Rent Growth — Quadrant Analysis</div>', unsafe_allow_html=True)
    fig6 = go.Figure(go.Scatter(
        x=dc["vacancy"]*100, y=dc["rent_growth"]*100,
        mode="markers+text",
        marker=dict(size=12, color=dc["score"], colorscale=[[0,GREEN],[0.5,AMBER],[1,RED]], showscale=False, line=dict(width=1,color=BORDER)),
        text=dc["submarket_name"].apply(lambda x: x.replace(" Austin","").replace(" County","")),
        textposition="top center", textfont=dict(size=9,color=MUTED,family="DM Mono"),
        hovertemplate="<b>%{text}</b><br>Vacancy: %{x:.1f}%<br>Rent Growth: %{y:.1f}%<extra></extra>",
    ))
    fig6.add_vline(x=14, line=dict(color=BORDER,width=1,dash="dot"))
    fig6.add_hline(y=0, line=dict(color=BORDER,width=1,dash="dot"))
    for x, y, label, c in [(8,3,"BUY ZONE",GREEN),(20,3,"RECOVERING",AMBER),(8,-4,"WATCH",AMBER),(20,-4,"SELL ZONE",RED)]:
        fig6.add_annotation(x=x,y=y,text=label,showarrow=False,font=dict(size=8,color=c,family="DM Mono"),bgcolor="rgba(248,249,250,0.85)")
    fig6.update_layout(**PLOTLY_LAYOUT, height=380, xaxis_title="Vacancy Rate (%)", yaxis_title="Rent Growth (%)")
    st.plotly_chart(fig6, use_container_width=True)

# ══════════════ TAB 4 ══════════════
with t4:
    st.markdown('<div class="section-title">Buy / Hold / Sell Signal by Submarket</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:1.5rem;">Composite: vacancy (25) · deliveries (20) · pipeline (20) · rent growth (15) · absorption (10) · days on market (5) · concessions (5)</div>', unsafe_allow_html=True)

    cs, ch, cb2 = st.columns(3)
    for col, sig_label, sc in [(cs,"SELL",RED),(ch,"HOLD",AMBER),(cb2,"BUY",GREEN)]:
        with col:
            filtered = dc[dc["signal"] == sig_label].sort_values("score", ascending=sig_label!="SELL")
            st.markdown(f'<div style="font-family:\'Inter\',sans-serif;font-size:1.2rem;font-weight:700;color:{sc};letter-spacing:0.05em;margin-bottom:1rem;padding-bottom:0.5rem;border-bottom:2px solid {sc};">{sig_label} — {len(filtered)}</div>', unsafe_allow_html=True)
            for _, r in filtered.iterrows():
                st.markdown(f"""
                <div style="padding:10px 12px;background:{CARD_BG};border:1px solid {BORDER};border-left:3px solid {sc};margin-bottom:6px;border-radius:4px;">
                    <div style="font-size:0.82rem;font-weight:600;color:{NAVY};margin-bottom:5px;">{r['submarket_name']}</div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:3px;">
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">VACANCY</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{TEXT};">{r['vacancy']*100:.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">RENT GROWTH</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{RED if r['rent_growth']<0 else GREEN};">{r['rent_growth']*100:+.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">UNDER CONSTR</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{TEXT};">{r['under_constr']:,}</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">ABSORPTION</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{TEXT};">{r.get('absorption_12mo',0):,}</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">CONCESSIONS</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{TEXT};">{r.get('concession_pct',0)*100:.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">SCORE</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{sc};font-weight:600;">{r['score']:.0f}/100</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Pressure Score Ranked</div>', unsafe_allow_html=True)
    sd = dc.sort_values("score", ascending=True)
    fig7 = go.Figure(go.Bar(
        x=sd["score"], y=sd["submarket_name"], orientation="h",
        marker_color=[sig_color(s) for s in sd["score"]], opacity=0.85,
        text=sd["score"].apply(lambda x: f"{x:.0f}"), textposition="outside",
        textfont=dict(size=9,color=MUTED,family="DM Mono"),
    ))
    fig7.add_vline(x=60, line=dict(color=RED,width=1,dash="dot"), annotation_text="SELL", annotation_font_color=RED)
    fig7.add_vline(x=35, line=dict(color=AMBER,width=1,dash="dot"), annotation_text="HOLD", annotation_font_color=AMBER)
    fig7.update_layout(**PLOTLY_LAYOUT, height=520, xaxis_range=[0,110], yaxis_tickfont_size=10, yaxis_tickfont_family="DM Mono")
    st.plotly_chart(fig7, use_container_width=True)

# ══════════════ TAB 5 ══════════════
with t5:
    fa, fb, fc = st.columns([2, 2, 1])
    with fa:
        search = st.text_input("", placeholder="Search address, project, or ZIP...", label_visibility="collapsed")
    with fb:
        subs = ["All Submarkets"] + sorted(df["submarket_name"].dropna().unique().tolist()) if not df.empty else ["All Submarkets"]
        sub_sel = st.selectbox("", subs, label_visibility="collapsed")
    with fc:
        min_u = st.number_input("", value=5, min_value=5, step=10, label_visibility="collapsed")

    disp = df_f.copy() if not df_f.empty else pd.DataFrame()
    if not disp.empty:
        if search:
            m = (disp["address"].str.contains(search,case=False,na=False) |
                 disp["project_name"].str.contains(search,case=False,na=False) |
                 disp["zip_code"].astype(str).str.contains(search,case=False,na=False))
            disp = disp[m]
        if sub_sel != "All Submarkets":
            disp = disp[disp["submarket_name"] == sub_sel]
        if min_u > 5:
            disp = disp[disp["total_units"] >= min_u]

        st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:0.5rem;">{len(disp):,} permits</div>', unsafe_allow_html=True)
        show = disp[["issue_date","address","zip_code","submarket_name","total_units","project_name","permit_num"]].rename(columns={
            "issue_date":"CO Date","address":"Address","zip_code":"ZIP",
            "submarket_name":"Submarket","total_units":"Units","project_name":"Project","permit_num":"Permit #"
        })
        st.dataframe(show.head(500), use_container_width=True, height=500, hide_index=True)
        st.download_button("Export CSV", disp.to_csv(index=False), f"austin_co_{datetime.now().strftime('%Y%m%d')}.csv", "text/csv")

# ─────────────────────────────────────────────────────────────────────────────
# POWERPOINT EXPORT
# ─────────────────────────────────────────────────────────────────────────────
PPTX_NAVY = RGBColor(0x1A, 0x1A, 0x2E)
PPTX_RED = RGBColor(0xC8, 0x10, 0x2E)
PPTX_WHITE = RGBColor(0xFF, 0xFF, 0xFF)
PPTX_GRAY = RGBColor(0x6B, 0x72, 0x80)

def _add_text(slide, left, top, width, height, text, font_size=12, color=PPTX_NAVY, bold=False, alignment=PP_ALIGN.LEFT):
    txBox = slide.shapes.add_textbox(Inches(left), Inches(top), Inches(width), Inches(height))
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.text = text
    p.font.size = Pt(font_size)
    p.font.color.rgb = color
    p.font.bold = bold
    p.alignment = alignment
    return tf

def build_pptx(dc_df, df_filtered):
    prs = Presentation()
    prs.slide_width = Inches(13.333)
    prs.slide_height = Inches(7.5)

    # --- Slide 1: Title ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])  # blank
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = PPTX_NAVY
    _add_text(slide, 0.8, 1.5, 11, 1.5, "AUSTIN MULTIFAMILY INTELLIGENCE", 44, PPTX_WHITE, True, PP_ALIGN.LEFT)
    _add_text(slide, 0.8, 3.2, 8, 0.8, "Market Analysis & Investment Signals", 22, PPTX_RED, False, PP_ALIGN.LEFT)
    _add_text(slide, 0.8, 5.0, 8, 0.6, f"Matthews Real Estate Investment Services  |  {datetime.now().strftime('%B %d, %Y')}", 14, PPTX_GRAY, False, PP_ALIGN.LEFT)

    # --- Slide 2: Market KPIs ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_text(slide, 0.8, 0.4, 10, 0.6, "MARKET KPIs", 28, PPTX_NAVY, True)
    _add_text(slide, 0.8, 1.0, 10, 0.3, f"All Time (2010+)  |  C-105 Certificates of Occupancy", 12, PPTX_GRAY)
    kpis = [
        ("Units Delivered", f"{int(df_filtered['total_units'].sum()):,}" if not df_filtered.empty else "N/A"),
        ("Projects", f"{len(df_filtered):,}" if not df_filtered.empty else "N/A"),
        ("Avg Project Size", f"{int(df_filtered['total_units'].mean()):,}" if not df_filtered.empty else "N/A"),
        ("Sell Signal Mkts", f"{len(dc_df[dc_df['signal']=='SELL'])}"),
    ]
    for i, (label, val) in enumerate(kpis):
        x = 0.8 + i * 3.0
        _add_text(slide, x, 2.0, 2.8, 0.4, label, 12, PPTX_GRAY)
        _add_text(slide, x, 2.5, 2.8, 0.8, val, 36, PPTX_NAVY, True)

    # --- Slide 3: Top 5 SELL submarkets ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_text(slide, 0.8, 0.4, 10, 0.6, "SELL SIGNAL SUBMARKETS", 28, PPTX_RED, True)
    sells = dc_df[dc_df["signal"] == "SELL"].sort_values("score", ascending=False).head(5)
    headers = ["Submarket", "Score", "Vacancy", "Rent Growth", "Under Constr", "Absorption"]
    for j, h in enumerate(headers):
        _add_text(slide, 0.8 + j * 2.0, 1.5, 1.9, 0.4, h, 11, PPTX_GRAY, True)
    for i, (_, r) in enumerate(sells.iterrows()):
        y = 2.0 + i * 0.6
        vals = [r["submarket_name"], f"{r['score']:.0f}/100", f"{r['vacancy']*100:.1f}%",
                f"{r['rent_growth']*100:+.1f}%", f"{r['under_constr']:,.0f}", f"{r.get('absorption_12mo',0):,.0f}"]
        for j, v in enumerate(vals):
            color = PPTX_RED if j == 1 else PPTX_NAVY
            _add_text(slide, 0.8 + j * 2.0, y, 1.9, 0.5, v, 13, color, j == 0)

    # --- Slide 4: Supply Pipeline ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_text(slide, 0.8, 0.4, 10, 0.6, "SUPPLY PIPELINE", 28, PPTX_NAVY, True)
    _add_text(slide, 0.8, 1.0, 10, 0.3, "Top 8 submarkets by units under construction", 12, PPTX_GRAY)
    top_pipe = dc_df[dc_df["under_constr"] > 0].sort_values("under_constr", ascending=False).head(8)
    headers = ["Submarket", "Under Constr", "Delivered 12mo", "Inventory", "Vacancy"]
    for j, h in enumerate(headers):
        _add_text(slide, 0.8 + j * 2.4, 1.6, 2.3, 0.4, h, 11, PPTX_GRAY, True)
    for i, (_, r) in enumerate(top_pipe.iterrows()):
        y = 2.1 + i * 0.55
        vals = [r["submarket_name"], f"{r['under_constr']:,.0f}", f"{r['delivered_12mo']:,.0f}",
                f"{r['inventory']:,.0f}", f"{r['vacancy']*100:.1f}%"]
        for j, v in enumerate(vals):
            _add_text(slide, 0.8 + j * 2.4, y, 2.3, 0.4, v, 12, PPTX_NAVY, j == 0)

    # --- Slide 5: Quadrant Chart (table version) ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_text(slide, 0.8, 0.4, 10, 0.6, "VACANCY vs RENT GROWTH", 28, PPTX_NAVY, True)
    _add_text(slide, 0.8, 1.0, 10, 0.3, "Quadrant analysis — submarkets by investment signal", 12, PPTX_GRAY)
    sorted_dc = dc_df.sort_values("score", ascending=False)
    headers = ["Submarket", "Vacancy", "Rent Growth", "Score", "Signal"]
    for j, h in enumerate(headers):
        _add_text(slide, 0.8 + j * 2.4, 1.6, 2.3, 0.4, h, 11, PPTX_GRAY, True)
    for i, (_, r) in enumerate(sorted_dc.head(15).iterrows()):
        y = 2.1 + i * 0.34
        sig_c = PPTX_RED if r["signal"] == "SELL" else (RGBColor(0xD9, 0x77, 0x06) if r["signal"] == "HOLD" else RGBColor(0x16, 0xA3, 0x4A))
        vals = [(r["submarket_name"], PPTX_NAVY), (f"{r['vacancy']*100:.1f}%", PPTX_NAVY),
                (f"{r['rent_growth']*100:+.1f}%", PPTX_RED if r["rent_growth"] < 0 else RGBColor(0x16, 0xA3, 0x4A)),
                (f"{r['score']:.0f}", PPTX_NAVY), (r["signal"], sig_c)]
        for j, (v, c) in enumerate(vals):
            _add_text(slide, 0.8 + j * 2.4, y, 2.3, 0.3, v, 11, c, j == 0 or j == 4)

    # --- Slide 6: Methodology ---
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    _add_text(slide, 0.8, 0.4, 10, 0.6, "METHODOLOGY & DATA SOURCES", 28, PPTX_NAVY, True)
    methodology = (
        "Supply Pressure Score (0-100)\n"
        "Composite of seven weighted factors:\n"
        "  - Vacancy Rate: 25 pts\n"
        "  - 12-Month Deliveries vs Inventory: 20 pts\n"
        "  - Under Construction vs Inventory: 20 pts\n"
        "  - Rent Growth (inverted): 15 pts\n"
        "  - Absorption vs Deliveries: 10 pts\n"
        "  - Avg Days on Market: 5 pts\n"
        "  - Concession Rate: 5 pts\n\n"
        "Signals: BUY (<35) | HOLD (35-59) | SELL (60+)\n\n"
        "Data Sources:\n"
        "  - City of Austin Open Data Portal (C-105 Certificates of Occupancy)\n"
        "  - CoStar Group (vacancy, rent, absorption, pipeline)\n"
        "  - Filtered to NEW/SHELL permits, 5-500 units, deduplicated by project"
    )
    _add_text(slide, 0.8, 1.2, 11, 5.0, methodology, 14, PPTX_NAVY)
    _add_text(slide, 0.8, 6.5, 11, 0.5, "Matthews Real Estate Investment Services  |  Confidential", 11, PPTX_GRAY)

    buf = io.BytesIO()
    prs.save(buf)
    buf.seek(0)
    return buf

with st.sidebar:
    st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.65rem;color:{MUTED};letter-spacing:0.15em;text-transform:uppercase;margin-top:1.5rem;margin-bottom:0.5rem;">Export</div>', unsafe_allow_html=True)
    pptx_buf = build_pptx(dc, df_f)
    st.download_button(
        label="Export to PowerPoint",
        data=pptx_buf,
        file_name=f"austin_mf_intelligence_{datetime.now().strftime('%Y%m%d')}.pptx",
        mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )

# ─────────────────────────────────────────────────────────────────────────────
# FOOTER
# ─────────────────────────────────────────────────────────────────────────────
st.markdown(f"""
<div style="margin-top:3rem;padding-top:1rem;border-top:1px solid {BORDER};display:flex;justify-content:space-between;align-items:center;">
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};">DATA: Austin Open Data Portal · CoStar Group · C-105 Certificates of Occupancy</div>
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};letter-spacing:0.12em;">MATTHEWS REAL ESTATE INVESTMENT SERVICES</div>
</div>
""", unsafe_allow_html=True)
