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
# COSTAR DATA — update these from your GeographyList.xlsx periodically
# ─────────────────────────────────────────────────────────────────────────────
COSTAR_DATA = {
    "North Austin": {
        "vacancy": 0.14,
        "rent_growth": -0.063,
        "inventory": 31903,
        "under_constr": 1065,
        "delivered_12mo": 374,
        "asking_rent": 1394,
    },
    "Northwest Austin": {
        "vacancy": 0.11,
        "rent_growth": -0.059,
        "inventory": 35400,
        "under_constr": 586,
        "delivered_12mo": 129,
        "asking_rent": 1343,
    },
    "East Austin": {
        "vacancy": 0.138,
        "rent_growth": -0.041,
        "inventory": 19201,
        "under_constr": 2328,
        "delivered_12mo": 792,
        "asking_rent": 1773,
    },
    "Downtown Austin": {
        "vacancy": 0.138,
        "rent_growth": 0.012,
        "inventory": 9001,
        "under_constr": 352,
        "delivered_12mo": 1254,
        "asking_rent": 3574,
    },
    "Pflugerville": {
        "vacancy": 0.139,
        "rent_growth": -0.067,
        "inventory": 23825,
        "under_constr": 444,
        "delivered_12mo": 752,
        "asking_rent": 1387,
    },
    "Northeast Austin": {
        "vacancy": 0.213,
        "rent_growth": -0.056,
        "inventory": 19387,
        "under_constr": 2436,
        "delivered_12mo": 3102,
        "asking_rent": 1397,
    },
    "South Austin": {
        "vacancy": 0.119,
        "rent_growth": -0.056,
        "inventory": 21649,
        "under_constr": 1063,
        "delivered_12mo": 930,
        "asking_rent": 1386,
    },
    "Round Rock": {
        "vacancy": 0.105,
        "rent_growth": -0.064,
        "inventory": 21390,
        "under_constr": 217,
        "delivered_12mo": 252,
        "asking_rent": 1426,
    },
    "Midtown Austin": {
        "vacancy": 0.127,
        "rent_growth": -0.021,
        "inventory": 17027,
        "under_constr": 1681,
        "delivered_12mo": 426,
        "asking_rent": 1562,
    },
    "Georgetown-Leander": {
        "vacancy": 0.167,
        "rent_growth": -0.067,
        "inventory": 17489,
        "under_constr": 574,
        "delivered_12mo": 1251,
        "asking_rent": 1513,
    },
    "Southeast Austin": {
        "vacancy": 0.209,
        "rent_growth": -0.052,
        "inventory": 16442,
        "under_constr": 571,
        "delivered_12mo": 2335,
        "asking_rent": 1397,
    },
    "Riverside": {
        "vacancy": 0.116,
        "rent_growth": -0.065,
        "inventory": 18784,
        "under_constr": 298,
        "delivered_12mo": 401,
        "asking_rent": 1377,
    },
    "Southwest Austin": {
        "vacancy": 0.113,
        "rent_growth": -0.035,
        "inventory": 13882,
        "under_constr": 949,
        "delivered_12mo": 803,
        "asking_rent": 1686,
    },
    "Cedar Park": {
        "vacancy": 0.116,
        "rent_growth": -0.054,
        "inventory": 15829,
        "under_constr": 0,
        "delivered_12mo": 391,
        "asking_rent": 1438,
    },
    "South Central Austin": {
        "vacancy": 0.116,
        "rent_growth": -0.036,
        "inventory": 13679,
        "under_constr": 572,
        "delivered_12mo": 570,
        "asking_rent": 1738,
    },
    "Buda-Kyle": {
        "vacancy": 0.157,
        "rent_growth": -0.046,
        "inventory": 11355,
        "under_constr": 280,
        "delivered_12mo": 798,
        "asking_rent": 1446,
    },
    "San Marcos": {
        "vacancy": 0.214,
        "rent_growth": -0.051,
        "inventory": 10828,
        "under_constr": 807,
        "delivered_12mo": 725,
        "asking_rent": 1261,
    },
    "Far North Austin": {
        "vacancy": 0.262,
        "rent_growth": -0.034,
        "inventory": 4032,
        "under_constr": 336,
        "delivered_12mo": 917,
        "asking_rent": 1560,
    },
    "Lake Travis": {
        "vacancy": 0.142,
        "rent_growth": -0.03,
        "inventory": 3978,
        "under_constr": 0,
        "delivered_12mo": 322,
        "asking_rent": 1813,
    },
    "Central Austin": {
        "vacancy": 0.092,
        "rent_growth": -0.021,
        "inventory": 3971,
        "under_constr": 0,
        "delivered_12mo": 38,
        "asking_rent": 1578,
    },
    "West Austin": {
        "vacancy": 0.061,
        "rent_growth": -0.006,
        "inventory": 2152,
        "under_constr": 168,
        "delivered_12mo": 0,
        "asking_rent": 2009,
    },
    "Far West Austin": {
        "vacancy": 0.054,
        "rent_growth": 0.008,
        "inventory": 149,
        "under_constr": 0,
        "delivered_12mo": 0,
        "asking_rent": 1645,
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
st.set_page_config(page_title="Austin MF Intelligence", page_icon="⬛", layout="wide")

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
    v = min((row["vacancy"] - 0.08) / 0.15, 1.0) * 30
    d = min(row["delivered_12mo"] / max(row["inventory"], 1) / 0.12, 1.0) * 25
    u = min(row["under_constr"] / max(row["inventory"], 1) / 0.15, 1.0) * 25
    r = min((-row["rent_growth"]) / 0.08, 1.0) * 20
    return round(max(0, v + d + u + r), 1)

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
    st.markdown('<div class="dash-header">AUSTIN MULTIFAMILY INTELLIGENCE</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="dash-sub">C-105 Certificates of Occupancy · {len(df):,} permits · {datetime.now().strftime("%b %d, %Y")}</div>', unsafe_allow_html=True)
with h2:
    st.markdown("<br>", unsafe_allow_html=True)
    yr = st.selectbox("", ["All Time", "Last 5 Years", "Last 3 Years", "Last 12 Months"], label_visibility="collapsed")

cutoff = {"All Time": 2000, "Last 5 Years": datetime.now().year - 5, "Last 3 Years": datetime.now().year - 3, "Last 12 Months": datetime.now().year - 1}.get(yr, 2000)
df_f = df[df["delivery_year"] >= cutoff] if not df.empty else df
dq_f = dq[dq["delivery_year"] >= cutoff] if not dq.empty else dq

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
                marker=dict(color=sub["total_units"], colorscale=[[0,"#1A1E26"],[0.5,"#8B6914"],[1,GOLD]], showscale=False),
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
            <div style="display:flex;align-items:center;gap:10px;margin-bottom:6px;padding:8px 10px;background:{CARD_BG};border:1px solid {BORDER};">
                <div style="flex:1;font-size:0.78rem;">{r['submarket_name']}</div>
                <div style="width:80px;height:4px;background:{BORDER};border-radius:2px;overflow:hidden;">
                    <div style="width:{int(r['score'])}%;height:100%;background:{sc};"></div>
                </div>
                <div style="width:28px;font-family:'DM Mono',monospace;font-size:0.7rem;color:{MUTED};text-align:right;">{r['score']:.0f}</div>
                <div style="padding:2px 6px;font-family:'DM Mono',monospace;font-size:0.62rem;border:1px solid {sc};color:{sc};">{r['signal']}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Quarterly Deliveries — All Submarkets</div>', unsafe_allow_html=True)
    if not dq_f.empty:
        qa = dq_f.groupby("delivery_yyyyq")["total_units_delivered"].sum().reset_index().sort_values("delivery_yyyyq")
        qa["rolling"] = qa["total_units_delivered"].rolling(4, min_periods=1).mean()
        fig2 = go.Figure()
        fig2.add_trace(go.Bar(x=qa["delivery_yyyyq"], y=qa["total_units_delivered"], marker_color=GOLD, opacity=0.5, name="Quarterly"))
        fig2.add_trace(go.Scatter(x=qa["delivery_yyyyq"], y=qa["rolling"], mode="lines", line=dict(color=TEXT, width=2), name="4Q Avg"))
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
            fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["under_constr"], name="Under Construction", marker_color=GOLD, opacity=0.85))
            fig3.add_trace(go.Bar(x=pipe["submarket_name"], y=pipe["delivered_12mo"], name="Delivered Last 12mo", marker_color="#2C3E50", opacity=0.9))
            fig3.update_layout(**PLOTLY_LAYOUT, barmode="group", height=380, xaxis_tickangle=-45)
            st.plotly_chart(fig3, use_container_width=True)

    with cb:
        st.markdown('<div class="section-title">Projected Delivery Timeline</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:1rem;">Based on historical CO pace — not CoStar estimates</div>', unsafe_allow_html=True)
        if not df_f.empty:
            for _, r in pipe.head(14).iterrows():
                m = r["months_to_deliver"]
                uc = r["under_constr"]
                urgency_c = RED if m <= 6 else (YELLOW if m <= 12 else MUTED)
                urgency_l = "IMMINENT" if m <= 6 else (f"~{m:.0f} MO")
                st.markdown(f"""
                <div style="display:flex;align-items:center;gap:10px;margin-bottom:5px;padding:8px 10px;background:{CARD_BG};border:1px solid {BORDER};border-left:2px solid {urgency_c};">
                    <div style="flex:1;font-size:0.78rem;">{r['submarket_name']}</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.68rem;color:{MUTED};">{uc:,.0f} UC</div>
                    <div style="font-family:'DM Mono',monospace;font-size:0.68rem;color:{urgency_c};width:75px;text-align:right;">{urgency_l}</div>
                </div>
                """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Annual Delivery Volume — Top 8 Submarkets</div>', unsafe_allow_html=True)
    if not df_f.empty:
        top8 = df_f.groupby("submarket_name")["total_units"].sum().nlargest(8).index.tolist()
        ann = df_f[df_f["submarket_name"].isin(top8)].groupby(["delivery_year","submarket_name"])["total_units"].sum().reset_index()
        colors8 = [GOLD,"#E74C3C","#3498DB","#2ECC71","#9B59B6","#E67E22","#1ABC9C","#F39C12"]
        fig4 = go.Figure()
        for i, s in enumerate(top8):
            d = ann[ann["submarket_name"] == s]
            fig4.add_trace(go.Scatter(x=d["delivery_year"], y=d["total_units"], name=s, mode="lines+markers", line=dict(color=colors8[i], width=2), marker=dict(size=5)))
        fig4.update_layout(**PLOTLY_LAYOUT, height=300)
        st.plotly_chart(fig4, use_container_width=True)

# ══════════════ TAB 3 ══════════════
with t3:
    st.markdown('<div class="section-title">Delivery Volume vs Current Vacancy — Bubble = Units Under Construction</div>', unsafe_allow_html=True)
    if not df_f.empty:
        sub_s = df_f.groupby("submarket_name").agg(total_units=("total_units","sum"), projects=("permit_num","count")).reset_index()
        abs_df = sub_s.merge(dc, on="submarket_name", how="inner")
        abs_df["score"] = abs_df.apply(pressure_score, axis=1)

        fig5 = go.Figure(go.Scatter(
            x=abs_df["total_units"], y=abs_df["vacancy"] * 100,
            mode="markers+text",
            marker=dict(size=abs_df["under_constr"].apply(lambda x: max(8, min(x/50,40))),
                       color=abs_df["score"], colorscale=[[0,GREEN],[0.5,YELLOW],[1,RED]],
                       showscale=True, colorbar=dict(title="Pressure", tickfont=dict(size=9,color=MUTED)),
                       line=dict(width=1,color=BORDER)),
            text=abs_df["submarket_name"].apply(lambda x: x.replace(" Austin","").replace(" County","")),
            textposition="top center", textfont=dict(size=9,color=MUTED,family="DM Mono"),
            hovertemplate="<b>%{text}</b><br>Units: %{x:,}<br>Vacancy: %{y:.1f}%<extra></extra>",
        ))
        fig5.add_hline(y=10, line=dict(color=GREEN,width=1,dash="dot"), annotation_text="10% baseline")
        fig5.add_hline(y=15, line=dict(color=YELLOW,width=1,dash="dot"), annotation_text="15% caution")
        fig5.add_hline(y=20, line=dict(color=RED,width=1,dash="dot"), annotation_text="20% oversupplied")
        fig5.update_layout(**PLOTLY_LAYOUT, height=460, xaxis_title="Total Units Delivered (historical)", yaxis_title="Vacancy Rate (%)")
        st.plotly_chart(fig5, use_container_width=True)

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<div class="section-title">Vacancy vs Rent Growth — Quadrant Analysis</div>', unsafe_allow_html=True)
    fig6 = go.Figure(go.Scatter(
        x=dc["vacancy"]*100, y=dc["rent_growth"]*100,
        mode="markers+text",
        marker=dict(size=12, color=dc["score"], colorscale=[[0,GREEN],[0.5,YELLOW],[1,RED]], showscale=False, line=dict(width=1,color=BORDER)),
        text=dc["submarket_name"].apply(lambda x: x.replace(" Austin","").replace(" County","")),
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
    st.markdown(f'<div style="font-family:\'DM Mono\',monospace;font-size:0.62rem;color:{MUTED};margin-bottom:1.5rem;">Composite: vacancy · rent growth · delivery volume · pipeline pressure</div>', unsafe_allow_html=True)

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

st.markdown(f"""
<div style="margin-top:3rem;padding-top:1rem;border-top:1px solid {BORDER};display:flex;justify-content:space-between;">
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};">DATA: Austin Open Data Portal · CoStar Group · C-105 Certificates of Occupancy</div>
    <div style="font-family:'DM Mono',monospace;font-size:0.6rem;color:{MUTED};">MATTHEWS REAL ESTATE INVESTMENT SERVICES</div>
</div>
""", unsafe_allow_html=True)
