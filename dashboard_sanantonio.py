"""
San Antonio Multifamily Intelligence Dashboard
===============================================
Run with: streamlit run dashboard_sanantonio.py

Tabs:
  1. Market Overview    — Supply pressure score by submarket
  2. Supply Pipeline    — Under construction + projected delivery dates
  3. Absorption         — Delivery volume vs vacancy over time
  4. Timing Intelligence — Buy/sell signals by submarket
  5. Permit Browser     — Raw CO data, searchable
"""

import os
from datetime import datetime
from typing import Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()

DB_DSN = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/austin_co")

# ─────────────────────────────────────────────────────────────────────────────
# COSTAR DATA — San Antonio Submarkets (CoStar Q1 2025)
# Fields: vacancy, rent_growth, inventory, under_constr, delivered_12mo,
#         asking_rent, absorption_12mo, avg_days_on_market, concession_pct
# ─────────────────────────────────────────────────────────────────────────────
COSTAR_DATA = {
    "North San Antonio": {
        "vacancy": 0.118,
        "rent_growth": -0.042,
        "inventory": 28450,
        "under_constr": 1240,
        "delivered_12mo": 890,
        "asking_rent": 1285,
        "absorption_12mo": 1050,
        "avg_days_on_market": 52,
        "concession_pct": 0.055,
    },
    "Northwest San Antonio": {
        "vacancy": 0.107,
        "rent_growth": -0.038,
        "inventory": 22180,
        "under_constr": 680,
        "delivered_12mo": 420,
        "asking_rent": 1198,
        "absorption_12mo": 580,
        "avg_days_on_market": 46,
        "concession_pct": 0.048,
    },
    "Northeast San Antonio": {
        "vacancy": 0.165,
        "rent_growth": -0.051,
        "inventory": 18920,
        "under_constr": 1850,
        "delivered_12mo": 2100,
        "asking_rent": 1142,
        "absorption_12mo": 1420,
        "avg_days_on_market": 68,
        "concession_pct": 0.082,
    },
    "South San Antonio": {
        "vacancy": 0.132,
        "rent_growth": -0.044,
        "inventory": 14760,
        "under_constr": 520,
        "delivered_12mo": 640,
        "asking_rent": 1095,
        "absorption_12mo": 720,
        "avg_days_on_market": 57,
        "concession_pct": 0.062,
    },
    "Southeast San Antonio": {
        "vacancy": 0.189,
        "rent_growth": -0.058,
        "inventory": 12340,
        "under_constr": 380,
        "delivered_12mo": 1480,
        "asking_rent": 1068,
        "absorption_12mo": 890,
        "avg_days_on_market": 74,
        "concession_pct": 0.088,
    },
    "Downtown San Antonio": {
        "vacancy": 0.142,
        "rent_growth": 0.008,
        "inventory": 8920,
        "under_constr": 310,
        "delivered_12mo": 680,
        "asking_rent": 2145,
        "absorption_12mo": 510,
        "avg_days_on_market": 60,
        "concession_pct": 0.042,
    },
    "Stone Oak": {
        "vacancy": 0.095,
        "rent_growth": -0.028,
        "inventory": 11280,
        "under_constr": 0,
        "delivered_12mo": 180,
        "asking_rent": 1485,
        "absorption_12mo": 320,
        "avg_days_on_market": 40,
        "concession_pct": 0.028,
    },
    "Medical Center": {
        "vacancy": 0.112,
        "rent_growth": -0.031,
        "inventory": 16540,
        "under_constr": 420,
        "delivered_12mo": 340,
        "asking_rent": 1322,
        "absorption_12mo": 480,
        "avg_days_on_market": 48,
        "concession_pct": 0.038,
    },
    "New Braunfels": {
        "vacancy": 0.148,
        "rent_growth": -0.049,
        "inventory": 9870,
        "under_constr": 680,
        "delivered_12mo": 920,
        "asking_rent": 1388,
        "absorption_12mo": 780,
        "avg_days_on_market": 62,
        "concession_pct": 0.068,
    },
    "Schertz-Cibolo": {
        "vacancy": 0.138,
        "rent_growth": -0.045,
        "inventory": 8420,
        "under_constr": 560,
        "delivered_12mo": 740,
        "asking_rent": 1265,
        "absorption_12mo": 620,
        "avg_days_on_market": 58,
        "concession_pct": 0.060,
    },
    "Boerne-Helotes": {
        "vacancy": 0.088,
        "rent_growth": -0.018,
        "inventory": 4280,
        "under_constr": 180,
        "delivered_12mo": 120,
        "asking_rent": 1542,
        "absorption_12mo": 210,
        "avg_days_on_market": 36,
        "concession_pct": 0.022,
    },
    "East San Antonio": {
        "vacancy": 0.172,
        "rent_growth": -0.053,
        "inventory": 7840,
        "under_constr": 420,
        "delivered_12mo": 860,
        "asking_rent": 1048,
        "absorption_12mo": 540,
        "avg_days_on_market": 70,
        "concession_pct": 0.078,
    },
}

# ─────────────────────────────────────────────────────────────────────────────
# COLORS
# ─────────────────────────────────────────────────────────────────────────────
GOLD = "#C9A84C"
RED = "#C0392B"
GREEN = "#27AE60"
YELLOW = "#F39C12"
BG = "#0A0C10"
CARD_BG = "#111318"
BORDER = "#1E2128"
TEXT = "#E8E3D8"
MUTED = "#6B7280"

PLOTLY_LAYOUT = dict(
    paper_bgcolor=CARD_BG,
    plot_bgcolor=CARD_BG,
    font=dict(color=TEXT, family="'DM Mono', monospace"),
    xaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
    yaxis=dict(gridcolor=BORDER, zerolinecolor=BORDER),
    legend=dict(bgcolor="rgba(0,0,0,0)", bordercolor=BORDER),
    margin=dict(t=40, r=20, b=40, l=60),
)

# ─────────────────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(page_title="San Antonio MF Intelligence", page_icon="⬛", layout="wide")

st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Bebas+Neue&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {{ background-color: {BG}; color: {TEXT}; font-family: 'DM Sans', sans-serif; }}
.main {{ background-color: {BG}; }}
.block-container {{ padding: 1.5rem 2rem; max-width: 1600px; }}
.dash-header {{ font-family: 'Bebas Neue', sans-serif; font-size: 2.8rem; letter-spacing: 0.12em; color: {TEXT}; line-height: 1; }}
.dash-sub {{ font-family: 'DM Mono', monospace; font-size: 0.72rem; color: {MUTED}; letter-spacing: 0.18em; text-transform: uppercase; margin-bottom: 1.5rem; }}
.kpi-card {{ background: {CARD_BG}; border: 1px solid {BORDER}; border-left: 3px solid {GOLD}; padding: 1rem 1.2rem; margin-bottom: 1rem; }}
.kpi-label {{ font-family: 'DM Mono', monospace; font-size: 0.65rem; color: {MUTED}; letter-spacing: 0.15em; text-transform: uppercase; margin-bottom: 0.4rem; }}
.kpi-value {{ font-family: 'Bebas Neue', sans-serif; font-size: 2rem; color: {TEXT}; line-height: 1; }}
.section-title {{ font-family: 'DM Mono', monospace; font-size: 0.65rem; color: {MUTED}; letter-spacing: 0.2em; text-transform: uppercase; border-bottom: 1px solid {BORDER}; padding-bottom: 0.5rem; margin-bottom: 1rem; }}
.stTabs [data-baseweb="tab-list"] {{ background: {CARD_BG}; border-bottom: 1px solid {BORDER}; gap: 0; padding: 0; }}
.stTabs [data-baseweb="tab"] {{ font-family: 'DM Mono', monospace; font-size: 0.7rem; letter-spacing: 0.12em; text-transform: uppercase; color: {MUTED}; padding: 0.75rem 1.5rem; border-bottom: 2px solid transparent; background: transparent; }}
.stTabs [aria-selected="true"] {{ color: {GOLD} !important; border-bottom: 2px solid {GOLD} !important; background: transparent !important; }}
::-webkit-scrollbar {{ width: 4px; height: 4px; }}
::-webkit-scrollbar-track {{ background: {BG}; }}
::-webkit-scrollbar-thumb {{ background: {BORDER}; border-radius: 2px; }}
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_permits(min_year=2010):
    try:
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
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=300)
def load_quarterly():
    try:
        conn = psycopg2.connect(DB_DSN)
        df = pd.read_sql("""
            SELECT submarket_name, delivery_year, delivery_quarter,
                   delivery_yyyyq, project_count, total_units_delivered
            FROM submarket_deliveries WHERE delivery_year >= 2010 ORDER BY delivery_yyyyq
        """, conn)
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()

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
    if score >= 35: return YELLOW
    return GREEN

try:
    df = load_permits()
    dq = load_quarterly()
    db_ok = not df.empty
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
    st.markdown('<div class="dash-header">SAN ANTONIO MULTIFAMILY INTELLIGENCE</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="dash-sub">CoStar Market Data · {datetime.now().strftime("%b %d, %Y")}</div>', unsafe_allow_html=True)
with h2:
    st.markdown("<br>", unsafe_allow_html=True)
    yr = st.selectbox("", ["All Time", "Last 5 Years", "Last 3 Years", "Last 12 Months"], label_visibility="collapsed")

cutoff = {"All Time": 2000, "Last 5 Years": datetime.now().year - 5, "Last 3 Years": datetime.now().year - 3, "Last 12 Months": datetime.now().year - 1}.get(yr, 2000)
df_f = df[df["delivery_year"] >= cutoff] if not df.empty else df
dq_f = dq[dq["delivery_year"] >= cutoff] if not dq.empty else dq

# KPIs
k1, k2, k3, k4, k5 = st.columns(5)
for col, label, val in [
    (k1, "Submarkets Tracked", f"{len(dc)}"),
    (k2, "Avg Vacancy",        f"{dc['vacancy'].mean()*100:.1f}%"),
    (k3, "Avg Rent Growth",    f"{dc['rent_growth'].mean()*100:+.1f}%"),
    (k4, "Total Under Constr", f"{int(dc['under_constr'].sum()):,}"),
    (k5, "Sell Signal Mkts",   f"{len(dc[dc['signal']=='SELL'])}"),
]:
    with col:
        st.markdown(f'<div class="kpi-card"><div class="kpi-label">{label}</div><div class="kpi-value">{val}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────────────────────────────────────
t1, t2, t3, t4 = st.tabs(["  MARKET OVERVIEW  ", "  SUPPLY PIPELINE  ", "  ABSORPTION  ", "  TIMING INTELLIGENCE  "])

# ══════════════ TAB 1 ══════════════
with t1:
    c1, c2 = st.columns([3, 2])
    with c1:
        st.markdown('<div class="section-title">Under Construction by Submarket</div>', unsafe_allow_html=True)
        sub = dc.sort_values("under_constr")
        fig = go.Figure(go.Bar(
            x=sub["under_constr"], y=sub["submarket_name"], orientation="h",
            marker=dict(color=sub["under_constr"], colorscale=[[0,"#1A1E26"],[0.5,"#8B6914"],[1,GOLD]], showscale=False),
            text=sub["under_constr"].apply(lambda x: f"{x:,}"), textposition="outside",
            textfont=dict(size=10, color=MUTED, family="DM Mono"),
        ))
        fig.update_layout(**PLOTLY_LAYOUT, height=500, xaxis_showgrid=False, xaxis_showticklabels=False, xaxis_zeroline=False)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.markdown('<div class="section-title">Supply Pressure Score</div>', unsafe_allow_html=True)
        for _, r in dc.sort_values("score", ascending=False).iterrows():
            sc = sig_color(r["score"])
            st.markdown(f"""
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;padding:8px 10px;background:{CARD_BG};border:1px solid {BORDER};">
                <div style="flex:1;font-size:0.78rem;">{r['submarket_name']}</div>
                <div style="width:80px;height:4px;background:{BORDER};border-radius:2px;overflow:hidden;">
                    <div style="width:{int(r['score'])}%;height:100%;background:{sc};"></div>
                </div>
                <div style="width:28px;font-family:'DM Mono',monospace;font-size:0.7rem;color:{MUTED};text-align:right;">{r['score']:.0f}</div>
                <div style="padding:2px 6px;font-family:'DM Mono',monospace;font-size:0.62rem;border:1px solid {sc};color:{sc};">{r['signal']}</div>
            </div>
            """, unsafe_allow_html=True)

# ══════════════ TAB 2 ══════════════
with t2:
    st.markdown('<div class="section-title">Under Construction vs Delivered Last 12 Months</div>', unsafe_allow_html=True)
    pipe = dc[["submarket_name","under_constr","delivered_12mo","inventory"]].copy()
    pipe = pipe[pipe["under_constr"] > 0].sort_values("under_constr", ascending=False)

    fig3 = go.Figure()
    fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["under_constr"], name="Under Construction", marker_color=GOLD, opacity=0.85))
    fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["delivered_12mo"], name="Delivered Last 12mo", marker_color="#2C3E50", opacity=0.9))
    fig3.update_layout(**PLOTLY_LAYOUT, barmode="group", height=420, xaxis_tickangle=-45)
    st.plotly_chart(fig3, use_container_width=True)

    st.markdown('<div class="section-title">Pipeline as % of Inventory</div>', unsafe_allow_html=True)
    pipe2 = dc.copy()
    pipe2["pipeline_pct"] = pipe2["under_constr"] / pipe2["inventory"].clip(lower=1) * 100
    pipe2 = pipe2.sort_values("pipeline_pct", ascending=False)
    fig4 = go.Figure(go.Bar(
        x=pipe2["submarket_name"], y=pipe2["pipeline_pct"],
        marker_color=[sig_color(s) for s in pipe2["score"]], opacity=0.85,
        text=pipe2["pipeline_pct"].apply(lambda x: f"{x:.1f}%"), textposition="outside",
        textfont=dict(size=9, color=MUTED, family="DM Mono"),
    ))
    fig4.add_hline(y=15, line=dict(color=YELLOW, width=1, dash="dot"), annotation_text="15% caution")
    fig4.update_layout(**PLOTLY_LAYOUT, height=380, xaxis_tickangle=-45, yaxis_title="Pipeline % of Inventory")
    st.plotly_chart(fig4, use_container_width=True)

# ══════════════ TAB 3 ══════════════
with t3:
    st.markdown('<div class="section-title">Absorption vs Deliveries — 12 Month</div>', unsafe_allow_html=True)
    abs_df = dc.copy()
    abs_df["score"] = abs_df.apply(pressure_score, axis=1)
    fig5 = go.Figure(go.Scatter(
        x=dc["delivered_12mo"], y=dc["vacancy"] * 100,
        mode="markers+text",
        marker=dict(size=14, color=abs_df["score"], colorscale=[[0,GREEN],[0.5,YELLOW],[1,RED]],
                   showscale=True, colorbar=dict(title="Pressure", tickfont=dict(size=9,color=MUTED)),
                   line=dict(width=1,color=BORDER)),
        text=dc["submarket_name"].apply(lambda x: x.replace(" San Antonio","").replace(" County","")),
        textposition="top center", textfont=dict(size=9,color=MUTED,family="DM Mono"),
        hovertemplate="<b>%{text}</b><br>Delivered: %{x:,}<br>Vacancy: %{y:.1f}%<extra></extra>",
    ))
    fig5.add_hline(y=10, line=dict(color=GREEN,width=1,dash="dot"), annotation_text="10% baseline")
    fig5.add_hline(y=15, line=dict(color=YELLOW,width=1,dash="dot"), annotation_text="15% caution")
    fig5.add_hline(y=20, line=dict(color=RED,width=1,dash="dot"), annotation_text="20% oversupplied")
    fig5.update_layout(**PLOTLY_LAYOUT, height=460, xaxis_title="Units Delivered Last 12mo", yaxis_title="Vacancy Rate (%)")
    st.plotly_chart(fig5, use_container_width=True)

    st.markdown('<div class="section-title">Vacancy vs Rent Growth — Quadrant Analysis</div>', unsafe_allow_html=True)
    fig6 = go.Figure(go.Scatter(
        x=dc["vacancy"]*100, y=dc["rent_growth"]*100,
        mode="markers+text",
        marker=dict(size=12, color=dc["score"], colorscale=[[0,GREEN],[0.5,YELLOW],[1,RED]], showscale=False, line=dict(width=1,color=BORDER)),
        text=dc["submarket_name"].apply(lambda x: x.replace(" San Antonio","").replace(" County","")),
        textposition="top center", textfont=dict(size=9,color=MUTED,family="DM Mono"),
        hovertemplate="<b>%{text}</b><br>Vacancy: %{x:.1f}%<br>Rent Growth: %{y:.1f}%<extra></extra>",
    ))
    fig6.add_vline(x=14, line=dict(color=BORDER,width=1,dash="dot"))
    fig6.add_hline(y=0, line=dict(color=BORDER,width=1,dash="dot"))
    for x, y, label, c in [(8,3,"BUY ZONE",GREEN),(20,3,"RECOVERING",YELLOW),(8,-4,"WATCH",YELLOW),(20,-4,"SELL ZONE",RED)]:
        fig6.add_annotation(x=x,y=y,text=label,showarrow=False,font=dict(size=8,color=c,family="DM Mono"),bgcolor="rgba(0,0,0,0.6)")
    fig6.update_layout(**PLOTLY_LAYOUT, height=380, xaxis_title="Vacancy Rate (%)", yaxis_title="Rent Growth (%)")
    st.plotly_chart(fig6, use_container_width=True)

# ══════════════ TAB 4 ══════════════
with t4:
    st.markdown('<div class="section-title">Buy / Hold / Sell Signal by Submarket</div>', unsafe_allow_html=True)
    st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:1.5rem;">Composite: vacancy (25) · deliveries (20) · pipeline (20) · rent growth (15) · absorption (10) · days on market (5) · concessions (5)</div>', unsafe_allow_html=True)

    cs, ch, cb2 = st.columns(3)
    for col, sig_label, sc in [(cs,"SELL",RED),(ch,"HOLD",YELLOW),(cb2,"BUY",GREEN)]:
        with col:
            filtered = dc[dc["signal"] == sig_label].sort_values("score", ascending=sig_label!="SELL")
            st.markdown(f'<div style="font-family:\'Bebas Neue\',sans-serif;font-size:1.3rem;color:{sc};letter-spacing:0.1em;margin-bottom:1rem;">{sig_label} — {len(filtered)}</div>', unsafe_allow_html=True)
            for _, r in filtered.iterrows():
                st.markdown(f"""
                <div style="padding:10px 12px;background:{CARD_BG};border:1px solid {BORDER};border-left:3px solid {sc};margin-bottom:6px;">
                    <div style="font-size:0.82rem;font-weight:500;margin-bottom:5px;">{r['submarket_name']}</div>
                    <div style="display:grid;grid-template-columns:1fr 1fr;gap:3px;">
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">VACANCY</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;">{r['vacancy']*100:.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">RENT GROWTH</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{RED if r['rent_growth']<0 else GREEN};">{r['rent_growth']*100:+.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">UNDER CONSTR</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;">{r['under_constr']:,}</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">ABSORPTION</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;">{r.get('absorption_12mo',0):,}</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">CONCESSIONS</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;">{r.get('concession_pct',0)*100:.1f}%</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{MUTED};">SCORE</div>
                        <div style="font-family:'DM Mono',monospace;font-size:0.62rem;color:{sc};">{r['score']:.0f}/100</div>
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
    fig7.add_vline(x=60, line=dict(color=RED,width=1,dash="dot"), annotation_text="SELL")
    fig7.add_vline(x=35, line=dict(color=YELLOW,width=1,dash="dot"), annotation_text="HOLD")
    fig7.update_layout(**PLOTLY_LAYOUT, height=480, xaxis_range=[0,110], yaxis_tickfont_size=10, yaxis_tickfont_family="DM Mono")
    st.plotly_chart(fig7, use_container_width=True)

st.markdown(f"""
<div style="margin-top:3rem;padding-top:1rem;border-top:1px solid {BORDER};display:flex;justify-content:space-between;">
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};">DATA: CoStar Group · San Antonio Multifamily Market</div>
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};">MATTHEWS REAL ESTATE INVESTMENT SERVICES</div>
</div>
""", unsafe_allow_html=True)
