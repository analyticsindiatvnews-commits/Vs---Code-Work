"""
CDN Log Intelligence — Streamlit App
Reads a folder of Akamai parquet CDN logs, parses dates from reqTimeSec,
and provides full UA analysis + geography, performance, cache, error dashboards
with date-range filtering and side-by-side comparison mode.
"""

import os
import glob
import re
from datetime import datetime, timedelta
from urllib.parse import unquote

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pyarrow.parquet as pq
import streamlit as st

# ──────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="CDN Log Intelligence",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ──────────────────────────────────────────────────────────────
# STYLING
# ──────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; padding-bottom: 2rem; }
    .metric-card {
        background: #1e2130; border-radius: 10px; padding: 16px 20px;
        border: 1px solid #2d3147;
    }
    .metric-val { font-size: 28px; font-weight: 800; color: #00e5a0; }
    .metric-lbl { font-size: 12px; color: #6b7280; text-transform: uppercase; letter-spacing: 0.08em; }
    .metric-delta { font-size: 12px; margin-top: 4px; }
    .stTabs [data-baseweb="tab"] { font-size: 14px; }
    h2 { font-size: 1.2rem !important; }
</style>
""", unsafe_allow_html=True)

COLORS = ["#00e5a0","#5b7fff","#a78bfa","#ffc847","#fb923c",
          "#f472b6","#38bdf8","#34d399","#ff6b6b","#e2e8f0","#6b7280","#facc15"]

# ──────────────────────────────────────────────────────────────
# UA PLATFORM TAGGER
# ──────────────────────────────────────────────────────────────
def tag_platform(ua: str) -> str:
    if not ua or ua != ua:  # nan check
        return "Null/Invalid"
    u = ua.lower()
    if "dalvik" in u:
        if re.search(r"aft[a-z0-9]", ua, re.I): return "Amazon Fire TV"
        if "bravia" in u:                        return "Sony BRAVIA"
        if "mibox" in u or "mitvstick" in u:    return "Xiaomi/MiBox"
        return "Other Android TV"
    if "web0s" in u or "webos" in u:            return "LG WebOS"
    if "tizen" in u:                            return "Samsung Tizen"
    if "applecoremedia" in u:
        if "apple tv" in u:                     return "Apple TV"
        if "iphone" in u:                       return "iPhone (ACM)"
        if "ipad" in u:                         return "iPad (ACM)"
        return "Apple Other"
    if "iphone" in u:                           return "iPhone Browser"
    if "ipad" in u:                             return "iPad Browser"
    if "macintosh" in u:                        return "Mac Browser"
    if "windows" in u:                          return "Windows Browser"
    if "linux" in u and "exoplayer" not in u and "android" not in u and "smart-tv" not in u:
        return "Linux Browser"
    if "exoplayer" in u:
        m = re.match(r"^([^\s/]+)", ua)
        name = m.group(0).split("/")[0].strip() if m else "ExoPlayer"
        return f"App: {name}"
    if "lavf" in u or "ffmpeg" in u:           return "Tool: FFmpeg"
    if "curl" in u:                             return "Tool: curl"
    if "python-requests" in u:                  return "Tool: python-requests"
    if "okhttp" in u:                           return "Tool: OkHttp"
    if "adflag" in u:                           return "Service: AdFlag"
    if "hls2disk" in u:                         return "Service: hls2disk"
    if "palo alto" in u or "xpanse" in u:       return "Scanner: Palo Alto"
    if "recordedfuture" in u:                   return "Scanner: RecordedFuture"
    if "akamai" in u:                           return "Scanner: Akamai"
    if "slackbot" in u:                         return "Bot: Slack"
    if "whatsapp" in u:                         return "Bot: WhatsApp"
    if ua.strip() in ["(null)", "-", ""]:        return "Null/Invalid"
    return "Other"

def extract_android_version(ua: str) -> str:
    m = re.search(r"Android (\d+)", ua or "")
    return m.group(1) if m else None

def extract_device_model(ua: str) -> str:
    m = re.search(r"Android [\d.]+;\s*(.+?)\s*Build/", ua or "")
    return m.group(1).strip() if m else None

# ──────────────────────────────────────────────────────────────
# DATA LOADING
# ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False)
def load_parquet_folder(folder: str) -> pd.DataFrame:
    """Load all parquet files from a folder (recursive). Returns combined DataFrame."""
    patterns = [
        os.path.join(folder, "**", "*.parquet"),
        os.path.join(folder, "*.parquet"),
    ]
    files = []
    for p in patterns:
        files.extend(glob.glob(p, recursive=True))
    files = sorted(set(files))
    if not files:
        return pd.DataFrame()

    dfs = []
    progress = st.progress(0, text="Loading parquet files…")
    for i, f in enumerate(files):
        try:
            df = pq.read_table(f, columns=[
                "UA", "reqTimeSec", "country", "city", "state",
                "statusCode", "bytes", "totalBytes", "throughput",
                "timeToFirstByte", "transferTimeMSec", "turnAroundTimeMSec",
                "cacheStatus", "errorCode", "rspContentType", "reqHost",
                "tlsVersion", "proto", "fileSizeBucket", "asn",
                "reqPath", "reqMethod", "objSize",
            ]).to_pandas()
            df["_source_file"] = os.path.basename(f)
            dfs.append(df)
        except Exception as e:
            st.warning(f"Could not read {os.path.basename(f)}: {e}")
        progress.progress((i + 1) / len(files), text=f"Loaded {i+1}/{len(files)} files…")
    progress.empty()
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)

@st.cache_data(show_spinner=False)
def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """Parse timestamps, decode UAs, tag platforms."""
    with st.spinner("Enriching data…"):
        df = df.copy()
        # Timestamp
        df["_ts"] = pd.to_numeric(df["reqTimeSec"], errors="coerce")
        df["_dt"] = pd.to_datetime(df["_ts"], unit="s", utc=True).dt.tz_convert("Asia/Kolkata")
        df["_date"] = df["_dt"].dt.date
        df["_hour"] = df["_dt"].dt.hour

        # UA decode + tag
        df["_ua"] = df["UA"].apply(lambda x: unquote(str(x)) if pd.notna(x) else "")
        df["_platform"] = df["_ua"].apply(tag_platform)
        df["_android_ver"] = df["_ua"].apply(extract_android_version)
        df["_device_model"] = df["_ua"].apply(extract_device_model)

        # Numerics
        for col in ["bytes","totalBytes","throughput","timeToFirstByte",
                    "transferTimeMSec","turnAroundTimeMSec","objSize"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["_cache_hit"] = df["cacheStatus"].astype(str) == "1"
        df["_is_error"] = df["statusCode"].astype(str).str.startswith(("4","5"))

    return df

# ──────────────────────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────────────────────
def fmt(n):
    if n is None or (isinstance(n, float) and np.isnan(n)):
        return "—"
    if n >= 1e9:  return f"{n/1e9:.2f}B"
    if n >= 1e6:  return f"{n/1e6:.2f}M"
    if n >= 1e3:  return f"{n/1e3:.1f}K"
    return str(int(n))

def metric_card(label, value, delta=None, delta_label="vs prev period"):
    delta_html = ""
    if delta is not None:
        color = "#00e5a0" if delta >= 0 else "#ff6b6b"
        arrow = "▲" if delta >= 0 else "▼"
        delta_html = f'<div class="metric-delta" style="color:{color}">{arrow} {abs(delta):.1f}% {delta_label}</div>'
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-lbl">{label}</div>
        <div class="metric-val">{value}</div>
        {delta_html}
    </div>""", unsafe_allow_html=True)

def bar_chart(df_agg, x, y, title, color=None, top_n=15):
    df_agg = df_agg.nlargest(top_n, y) if top_n else df_agg
    fig = px.bar(df_agg, x=y, y=x, orientation="h",
                 title=title, color_discrete_sequence=[color or COLORS[0]])
    fig.update_layout(yaxis=dict(categoryorder="total ascending"),
                      height=max(300, top_n * 28),
                      margin=dict(l=0, r=0, t=40, b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"))
    fig.update_xaxes(gridcolor="#2d3147")
    fig.update_yaxes(gridcolor="rgba(0,0,0,0)")
    return fig

def pie_chart(df_agg, names, values, title):
    fig = px.pie(df_agg, names=names, values=values, title=title,
                 color_discrete_sequence=COLORS, hole=0.4)
    fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=340,
                      paper_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"), showlegend=True,
                      legend=dict(orientation="v"))
    return fig

def time_series(df, x, y, title, color=COLORS[0]):
    fig = px.line(df, x=x, y=y, title=title, color_discrete_sequence=[color])
    fig.update_traces(line_width=2)
    fig.update_layout(height=280, margin=dict(l=0,r=0,t=40,b=0),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font=dict(color="#e4e8f0"))
    fig.update_xaxes(gridcolor="#2d3147")
    fig.update_yaxes(gridcolor="#2d3147")
    return fig

# ──────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📡 CDN Log Intelligence")
    st.markdown("---")

    # We set value="/data" so it automatically references your Docker volume mount
    folder_path = st.text_input(
        "Parquet folder path",
        value="/data", 
        help="Absolute path to the folder containing your .parquet files"
    )

    load_btn = st.button("Reload Data", type="primary", use_container_width=True)

    st.markdown("---")
    mode = st.radio("Analysis Mode", ["Single Range", "Compare Two Ranges"])
    st.markdown("---")
    st.markdown("**Filters**")
    platform_filter = st.multiselect("Platforms", options=[], key="plat_filter")
    country_filter = st.multiselect("Countries", options=[], key="cty_filter")
    host_filter = st.multiselect("Request Hosts", options=[], key="host_filter")

# ──────────────────────────────────────────────────────────────
# MAIN STATE
# ──────────────────────────────────────────────────────────────
if "df_full" not in st.session_state:
    st.session_state.df_full = None

# Automatically load data on startup if it hasn't been loaded yet, or if the reload button is clicked
if (load_btn or st.session_state.df_full is None) and folder_path:
    if not os.path.isdir(folder_path):
        st.error(f"❌ Folder not found: `{folder_path}`. Make sure your Docker volume is mounted correctly.")
    else:
        raw = load_parquet_folder(folder_path)
        if raw.empty:
            st.error(f"No parquet files found in or under `{folder_path}`.")
        else:
            st.session_state.df_full = enrich(raw)
            st.success(f"✅ Loaded {fmt(len(st.session_state.df_full))} rows from `{folder_path}`")

df_full = st.session_state.df_full

# ──────────────────────────────────────────────────────────────
# LANDING / UPLOAD PROMPT
# ──────────────────────────────────────────────────────────────
if df_full is None:
    st.markdown("""
    <div style="text-align:center; padding: 80px 40px;">
        <div style="font-size:60px; margin-bottom:16px">📡</div>
        <h1 style="font-size:2.2rem; font-weight:800; margin-bottom:12px">CDN Log Intelligence</h1>
        <p style="color:#6b7280; font-size:16px; max-width:500px; margin:0 auto 32px">
            Enter the path to your Akamai parquet log folder in the sidebar and click <strong>Load Data</strong>.
        </p>
        <div style="background:#1e2130; border:1px solid #2d3147; border-radius:12px; padding:24px; max-width:520px; margin:0 auto; text-align:left">
            <div style="font-family:monospace; font-size:12px; color:#6b7280; margin-bottom:12px">SUPPORTED FORMAT</div>
            <div style="font-family:monospace; font-size:13px; color:#00e5a0">
                /your/folder/<br>
                &nbsp;&nbsp;├── part_0000000.parquet  &nbsp; ← one file per day<br>
                &nbsp;&nbsp;├── part_0000001.parquet<br>
                &nbsp;&nbsp;└── ...
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.stop()

# ──────────────────────────────────────────────────────────────
# DATE RANGE SELECTOR
# ──────────────────────────────────────────────────────────────
all_dates = sorted(df_full["_date"].dropna().unique())
min_d, max_d = all_dates[0], all_dates[-1]

st.markdown(f"**Data available:** `{min_d}` → `{max_d}` &nbsp;·&nbsp; {len(all_dates)} days &nbsp;·&nbsp; {fmt(len(df_full))} total rows")

if mode == "Single Range":
    col1, col2 = st.columns(2)
    with col1:
        d_start = st.date_input("From", value=min_d, min_value=min_d, max_value=max_d)
    with col2:
        d_end = st.date_input("To", value=max_d, min_value=min_d, max_value=max_d)

    df = df_full[(df_full["_date"] >= d_start) & (df_full["_date"] <= d_end)].copy()
    df_compare = None

else:  # Compare Two Ranges
    col1, col2, col3, col4 = st.columns(4)
    with col1: d_start  = st.date_input("Range A — From", value=min_d, min_value=min_d, max_value=max_d)
    with col2: d_end    = st.date_input("Range A — To",   value=min_d + timedelta(days=max(0,len(all_dates)//2-1)), min_value=min_d, max_value=max_d)
    with col3: d_start2 = st.date_input("Range B — From", value=all_dates[len(all_dates)//2] if len(all_dates)>1 else min_d, min_value=min_d, max_value=max_d)
    with col4: d_end2   = st.date_input("Range B — To",   value=max_d, min_value=min_d, max_value=max_d)

    df         = df_full[(df_full["_date"] >= d_start)  & (df_full["_date"] <= d_end)].copy()
    df_compare = df_full[(df_full["_date"] >= d_start2) & (df_full["_date"] <= d_end2)].copy()

# ── Apply sidebar filters ──
all_platforms = sorted(df_full["_platform"].dropna().unique())
all_countries = sorted(df_full["country"].dropna().unique())
all_hosts     = sorted(df_full["reqHost"].dropna().unique())

# Update sidebar filter options
with st.sidebar:
    platform_filter = st.multiselect("Platforms", options=all_platforms, key="plat_filter2")
    country_filter  = st.multiselect("Countries",  options=all_countries,  key="cty_filter2")
    host_filter     = st.multiselect("Req Hosts",  options=all_hosts,      key="host_filter2")

def apply_filters(frame):
    if platform_filter: frame = frame[frame["_platform"].isin(platform_filter)]
    if country_filter:  frame = frame[frame["country"].isin(country_filter)]
    if host_filter:     frame = frame[frame["reqHost"].isin(host_filter)]
    return frame

df = apply_filters(df)
if df_compare is not None:
    df_compare = apply_filters(df_compare)

# ──────────────────────────────────────────────────────────────
# TABS
# ──────────────────────────────────────────────────────────────
tabs = st.tabs(["📊 Overview", "📱 UA Analysis", "🌍 Geography", "⚡ Performance", "💾 Cache & Content", "🚨 Errors & Anomalies", "🔍 Raw Explorer"])

# ──────────────────────────────────────────────────────────────
# TAB 0 — OVERVIEW
# ──────────────────────────────────────────────────────────────
with tabs[0]:
    total      = len(df)
    uniq_ua    = df["_ua"].nunique()
    uniq_ip    = df["cliIP"].nunique() if "cliIP" in df.columns else "—"
    cache_rate = df["_cache_hit"].mean() * 100 if total else 0
    err_rate   = df["_is_error"].mean() * 100 if total else 0
    avg_tput   = df["throughput"].mean() if "throughput" in df.columns else None
    total_tb   = df["totalBytes"].sum() / 1e12 if "totalBytes" in df.columns else 0

    st.markdown("### Key Metrics")
    c1,c2,c3,c4,c5,c6 = st.columns(6)

    def delta_vs(col_a, col_b=None, agg="count"):
        if df_compare is None or col_b is None:
            return None
        a = len(df) if agg == "count" else df[col_a].mean()
        b = len(df_compare) if agg == "count" else df_compare[col_a].mean()
        return ((a - b) / b * 100) if b else None

    with c1: metric_card("Total Requests",    fmt(total),         delta_vs("count"))
    with c2: metric_card("Unique UAs",        fmt(uniq_ua))
    with c3: metric_card("Cache Hit Rate",    f"{cache_rate:.1f}%")
    with c4: metric_card("Error Rate",        f"{err_rate:.1f}%")
    with c5: metric_card("Avg Throughput",    f"{avg_tput/1e3:.0f} Mbps" if avg_tput else "—")
    with c6: metric_card("Total Transferred", f"{total_tb:.2f} TB" if total_tb else "—")

    st.markdown("---")

    # Requests over time
    st.markdown("### Requests Over Time")
    ts_agg = df.groupby(df["_dt"].dt.floor("1h")).size().reset_index()
    ts_agg.columns = ["hour", "requests"]
    fig = time_series(ts_agg, "hour", "requests", "Requests per Hour")
    st.plotly_chart(fig, use_container_width=True)

    if df_compare is not None:
        ts_b = df_compare.groupby(df_compare["_dt"].dt.floor("1h")).size().reset_index()
        ts_b.columns = ["hour", "requests"]
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(x=ts_agg["hour"],y=ts_agg["requests"],name=f"A: {d_start}→{d_end}",line=dict(color=COLORS[0],width=2)))
        fig2.add_trace(go.Scatter(x=ts_b["hour"], y=ts_b["requests"], name=f"B: {d_start2}→{d_end2}",line=dict(color=COLORS[1],width=2)))
        fig2.update_layout(title="Range A vs Range B — Requests per Hour", height=300,
                           margin=dict(l=0,r=0,t=40,b=0),
                           paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                           font=dict(color="#e4e8f0"))
        fig2.update_xaxes(gridcolor="#2d3147"); fig2.update_yaxes(gridcolor="#2d3147")
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        sc = df["statusCode"].value_counts().reset_index()
        sc.columns = ["Status","Count"]
        st.plotly_chart(pie_chart(sc,"Status","Count","Status Code Distribution"), use_container_width=True)
    with c2:
        hc = df["reqHost"].value_counts().reset_index()
        hc.columns = ["Host","Count"]
        st.plotly_chart(bar_chart(hc,"Host","Count","Top Request Hosts", color=COLORS[1], top_n=10), use_container_width=True)

# ──────────────────────────────────────────────────────────────
# TAB 1 — UA ANALYSIS
# ──────────────────────────────────────────────────────────────
with tabs[1]:
    st.markdown("### Platform Distribution")

    plat = df.groupby("_platform").size().reset_index(name="requests")
    plat["pct"] = (plat["requests"] / len(df) * 100).round(2)
    plat = plat.sort_values("requests", ascending=False)

    c1, c2 = st.columns([2, 1])
    with c1:
        fig = bar_chart(plat, "_platform", "requests", "Requests by Platform", top_n=20)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        top10 = plat.head(10)
        st.plotly_chart(pie_chart(top10, "_platform", "requests", "Top 10 Platforms"), use_container_width=True)

    if df_compare is not None:
        st.markdown("#### Platform Comparison — Range A vs Range B")
        p_a = df.groupby("_platform").size().reset_index(name="Range A")
        p_b = df_compare.groupby("_platform").size().reset_index(name="Range B")
        merged = p_a.merge(p_b, on="_platform", how="outer").fillna(0)
        merged["Δ%"] = ((merged["Range A"] - merged["Range B"]) / merged["Range B"].replace(0,np.nan) * 100).round(1)
        merged = merged.sort_values("Range A", ascending=False).head(15)
        fig_cmp = go.Figure()
        fig_cmp.add_bar(x=merged["_platform"], y=merged["Range A"], name=f"A: {d_start}→{d_end}", marker_color=COLORS[0])
        fig_cmp.add_bar(x=merged["_platform"], y=merged["Range B"], name=f"B: {d_start2}→{d_end2}", marker_color=COLORS[1])
        fig_cmp.update_layout(barmode="group", height=380, margin=dict(l=0,r=0,t=40,b=0),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font=dict(color="#e4e8f0"), xaxis_tickangle=-30)
        fig_cmp.update_yaxes(gridcolor="#2d3147")
        st.plotly_chart(fig_cmp, use_container_width=True)
        st.dataframe(merged, use_container_width=True, hide_index=True)

    st.markdown("---")
    st.markdown("### Android — Device Models")
    android_df = df[df["_device_model"].notna()]
    if len(android_df):
        dm = android_df.groupby("_device_model").size().reset_index(name="requests").sort_values("requests",ascending=False)
        c1, c2 = st.columns([3,1])
        with c1:
            st.plotly_chart(bar_chart(dm,"_device_model","requests","Top Device Models",color=COLORS[0],top_n=20), use_container_width=True)
        with c2:
            st.dataframe(dm.head(30), use_container_width=True, hide_index=True)
    else:
        st.info("No Android device model data in current selection.")

    st.markdown("---")
    st.markdown("### Android OS Versions")
    av = df[df["_android_ver"].notna()].groupby("_android_ver").size().reset_index(name="requests")
    av["_android_ver_int"] = pd.to_numeric(av["_android_ver"], errors="coerce")
    av = av.sort_values("_android_ver_int")
    fig_av = px.bar(av, x="_android_ver", y="requests", title="Android Version Distribution",
                    color_discrete_sequence=[COLORS[2]])
    fig_av.update_layout(height=300, margin=dict(l=0,r=0,t=40,b=0),
                         paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                         font=dict(color="#e4e8f0"))
    fig_av.update_yaxes(gridcolor="#2d3147")
    st.plotly_chart(fig_av, use_container_width=True)

    st.markdown("---")
    st.markdown("### Top Unique UAs")
    ua_top = df.groupby("_ua").size().reset_index(name="requests").sort_values("requests",ascending=False).head(50)
    st.dataframe(ua_top, use_container_width=True, hide_index=True)

# ──────────────────────────────────────────────────────────────
# TAB 2 — GEOGRAPHY
# ──────────────────────────────────────────────────────────────
with tabs[2]:
    st.markdown("### Country Distribution")
    c1, c2 = st.columns(2)
    with c1:
        ct = df.groupby("country").size().reset_index(name="requests").sort_values("requests",ascending=False)
        st.plotly_chart(bar_chart(ct,"country","requests","Requests by Country",color=COLORS[3],top_n=20), use_container_width=True)
    with c2:
        st.plotly_chart(pie_chart(ct.head(10),"country","requests","Top 10 Countries"), use_container_width=True)

    if df_compare is not None:
        st.markdown("#### Country Comparison")
        c_a = df.groupby("country").size().reset_index(name="Range A")
        c_b = df_compare.groupby("country").size().reset_index(name="Range B")
        cm = c_a.merge(c_b, on="country", how="outer").fillna(0).sort_values("Range A",ascending=False).head(15)
        fig_cmp = go.Figure()
        fig_cmp.add_bar(x=cm["country"], y=cm["Range A"], name=f"A", marker_color=COLORS[0])
        fig_cmp.add_bar(x=cm["country"], y=cm["Range B"], name=f"B", marker_color=COLORS[1])
        fig_cmp.update_layout(barmode="group", height=340, margin=dict(l=0,r=0,t=40,b=0),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              font=dict(color="#e4e8f0"))
        fig_cmp.update_yaxes(gridcolor="#2d3147")
        st.plotly_chart(fig_cmp, use_container_width=True)

    st.markdown("---")
    st.markdown("### India — State Breakdown")
    india = df[df["country"] == "IN"]
    if len(india):
        st._decode = lambda x: unquote(str(x)) if pd.notna(x) else x
        india = india.copy()
        india["state_decoded"] = india["state"].apply(lambda x: unquote(str(x)) if pd.notna(x) else x)
        st_agg = india.groupby("state_decoded").size().reset_index(name="requests").sort_values("requests",ascending=False)
        c1, c2 = st.columns([2,1])
        with c1:
            st.plotly_chart(bar_chart(st_agg,"state_decoded","requests","Requests by Indian State",color=COLORS[4],top_n=25), use_container_width=True)
        with c2:
            st.dataframe(st_agg, use_container_width=True, hide_index=True)
    else:
        st.info("No India traffic in current selection.")

    st.markdown("---")
    st.markdown("### City Breakdown")
    city_agg = df[df["city"].notna() & (df["city"] != "NaN")].groupby("city").size().reset_index(name="requests").sort_values("requests",ascending=False)
    st.plotly_chart(bar_chart(city_agg,"city","requests","Top Cities",color=COLORS[5],top_n=20), use_container_width=True)

# ──────────────────────────────────────────────────────────────
# TAB 3 — PERFORMANCE
# ──────────────────────────────────────────────────────────────
with tabs[3]:
    st.markdown("### Throughput")
    c1,c2,c3,c4 = st.columns(4)
    with c1: metric_card("Avg Throughput", f"{df['throughput'].mean()/1e3:.1f} Mbps" if df["throughput"].notna().any() else "—")
    with c2: metric_card("Median Throughput", f"{df['throughput'].median()/1e3:.1f} Mbps" if df["throughput"].notna().any() else "—")
    with c3: metric_card("Avg TTFB", f"{df['timeToFirstByte'].mean():.0f} ms" if df["timeToFirstByte"].notna().any() else "—")
    with c4: metric_card("Avg Transfer Time", f"{df['transferTimeMSec'].mean():.0f} ms" if df["transferTimeMSec"].notna().any() else "—")

    st.markdown("---")

    # Throughput over time
    tp_ts = df.groupby(df["_dt"].dt.floor("1h"))["throughput"].mean().reset_index()
    tp_ts.columns = ["hour","avg_throughput_kbps"]
    tp_ts["avg_throughput_mbps"] = tp_ts["avg_throughput_kbps"] / 1e3
    st.plotly_chart(time_series(tp_ts,"hour","avg_throughput_mbps","Avg Throughput Over Time (Mbps)", COLORS[0]), use_container_width=True)

    if df_compare is not None:
        st.markdown("#### Throughput Comparison")
        tp_b = df_compare.groupby(df_compare["_dt"].dt.floor("1h"))["throughput"].mean().reset_index()
        tp_b.columns = ["hour","avg_throughput_kbps"]
        tp_b["avg_throughput_mbps"] = tp_b["avg_throughput_kbps"] / 1e3
        fig_tp = go.Figure()
        fig_tp.add_trace(go.Scatter(x=tp_ts["hour"],y=tp_ts["avg_throughput_mbps"],name=f"A",line=dict(color=COLORS[0],width=2)))
        fig_tp.add_trace(go.Scatter(x=tp_b["hour"], y=tp_b["avg_throughput_mbps"], name=f"B",line=dict(color=COLORS[1],width=2)))
        fig_tp.update_layout(height=300, margin=dict(l=0,r=0,t=40,b=0),
                             paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                             font=dict(color="#e4e8f0"))
        fig_tp.update_xaxes(gridcolor="#2d3147"); fig_tp.update_yaxes(gridcolor="#2d3147")
        st.plotly_chart(fig_tp, use_container_width=True)

    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        # TTFB by platform
        ttfb_plat = df.groupby("_platform")["timeToFirstByte"].mean().reset_index().sort_values("timeToFirstByte")
        ttfb_plat.columns = ["Platform","Avg TTFB (ms)"]
        fig_ttfb = px.bar(ttfb_plat, x="Avg TTFB (ms)", y="Platform", orientation="h",
                          title="Avg TTFB by Platform", color_discrete_sequence=[COLORS[2]])
        fig_ttfb.update_layout(height=380, margin=dict(l=0,r=0,t=40,b=0),
                               paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                               font=dict(color="#e4e8f0"), yaxis=dict(categoryorder="total ascending"))
        fig_ttfb.update_xaxes(gridcolor="#2d3147")
        st.plotly_chart(fig_ttfb, use_container_width=True)
    with c2:
        # Throughput distribution
        tput_valid = df["throughput"].dropna()
        tput_valid = tput_valid[tput_valid > 0]
        if len(tput_valid):
            fig_hist = px.histogram(tput_valid/1e3, nbins=50, title="Throughput Distribution (Mbps)",
                                    color_discrete_sequence=[COLORS[1]])
            fig_hist.update_layout(height=380, margin=dict(l=0,r=0,t=40,b=0),
                                   paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                   font=dict(color="#e4e8f0"), showlegend=False)
            fig_hist.update_yaxes(gridcolor="#2d3147")
            st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("---")
    st.markdown("### TLS & Protocol")
    c1,c2 = st.columns(2)
    with c1:
        tls = df.groupby("tlsVersion").size().reset_index(name="requests")
        st.plotly_chart(pie_chart(tls,"tlsVersion","requests","TLS Version Distribution"), use_container_width=True)
    with c2:
        proto = df.groupby("proto").size().reset_index(name="requests")
        st.plotly_chart(pie_chart(proto,"proto","requests","Protocol Distribution"), use_container_width=True)

# ──────────────────────────────────────────────────────────────
# TAB 4 — CACHE & CONTENT
# ──────────────────────────────────────────────────────────────
with tabs[4]:
    st.markdown("### Cache")
    cache_rate = df["_cache_hit"].mean()*100

    c1,c2,c3 = st.columns(3)
    with c1: metric_card("Overall Cache Hit Rate", f"{cache_rate:.1f}%")
    with c2: metric_card("Cache Hits",  fmt(df["_cache_hit"].sum()))
    with c3: metric_card("Cache Misses",fmt((~df["_cache_hit"]).sum()))

    # Cache hit rate over time
    cache_ts = df.groupby(df["_dt"].dt.floor("1h"))["_cache_hit"].mean().reset_index()
    cache_ts.columns = ["hour","hit_rate"]
    cache_ts["hit_rate_pct"] = cache_ts["hit_rate"] * 100
    st.plotly_chart(time_series(cache_ts,"hour","hit_rate_pct","Cache Hit Rate Over Time (%)",COLORS[3]), use_container_width=True)

    if df_compare is not None:
        st.markdown("#### Cache Hit Rate Comparison")
        cache_b = df_compare.groupby(df_compare["_dt"].dt.floor("1h"))["_cache_hit"].mean().reset_index()
        cache_b.columns = ["hour","hit_rate"]
        cache_b["hit_rate_pct"] = cache_b["hit_rate"] * 100
        fig_ch = go.Figure()
        fig_ch.add_trace(go.Scatter(x=cache_ts["hour"],y=cache_ts["hit_rate_pct"],name="A",line=dict(color=COLORS[0],width=2)))
        fig_ch.add_trace(go.Scatter(x=cache_b["hour"], y=cache_b["hit_rate_pct"], name="B",line=dict(color=COLORS[1],width=2)))
        fig_ch.update_layout(height=280,margin=dict(l=0,r=0,t=40,b=0),
                             paper_bgcolor="rgba(0,0,0,0)",plot_bgcolor="rgba(0,0,0,0)",
                             font=dict(color="#e4e8f0"))
        fig_ch.update_xaxes(gridcolor="#2d3147"); fig_ch.update_yaxes(gridcolor="#2d3147")
        st.plotly_chart(fig_ch, use_container_width=True)

    st.markdown("---")
    c1,c2 = st.columns(2)
    with c1:
        ct = df.groupby("rspContentType").size().reset_index(name="requests")
        st.plotly_chart(pie_chart(ct,"rspContentType","requests","Content Type Distribution"), use_container_width=True)
    with c2:
        fs = df.groupby("fileSizeBucket").size().reset_index(name="requests")
        st.plotly_chart(pie_chart(fs,"fileSizeBucket","requests","File Size Distribution"), use_container_width=True)

    st.markdown("---")
    st.markdown("### Bandwidth")
    bw_ts = df.groupby(df["_dt"].dt.floor("1h"))["totalBytes"].sum().reset_index()
    bw_ts.columns = ["hour","bytes"]
    bw_ts["GB"] = bw_ts["bytes"] / 1e9
    st.plotly_chart(time_series(bw_ts,"hour","GB","Bandwidth per Hour (GB)",COLORS[4]), use_container_width=True)

# ──────────────────────────────────────────────────────────────
# TAB 5 — ERRORS & ANOMALIES
# ──────────────────────────────────────────────────────────────
with tabs[5]:
    st.markdown("### Error Overview")
    err_df = df[df["_is_error"]]
    c1,c2,c3 = st.columns(3)
    with c1: metric_card("Total Errors",  fmt(len(err_df)))
    with c2: metric_card("Error Rate",    f"{df['_is_error'].mean()*100:.2f}%")
    with c3: metric_card("4xx Errors",    fmt((df["statusCode"].astype(str).str.startswith("4")).sum()))

    c1,c2 = st.columns(2)
    with c1:
        sc_err = err_df.groupby("statusCode").size().reset_index(name="count").sort_values("count",ascending=False)
        st.plotly_chart(bar_chart(sc_err,"statusCode","count","Error Status Codes",color=COLORS[8],top_n=10), use_container_width=True)
    with c2:
        if "errorCode" in df.columns:
            ec = df[df["errorCode"].notna() & (df["errorCode"] != "")].groupby("errorCode").size().reset_index(name="count").sort_values("count",ascending=False)
            if len(ec):
                st.plotly_chart(bar_chart(ec,"errorCode","count","Error Code Breakdown",color=COLORS[3],top_n=10), use_container_width=True)

    st.markdown("---")
    st.markdown("### Error Rate Over Time")
    err_ts = df.groupby(df["_dt"].dt.floor("1h"))["_is_error"].mean().reset_index()
    err_ts.columns = ["hour","error_rate"]
    err_ts["error_pct"] = err_ts["error_rate"] * 100
    st.plotly_chart(time_series(err_ts,"hour","error_pct","Error Rate Over Time (%)",COLORS[8]), use_container_width=True)

    st.markdown("---")
    st.markdown("### Anomaly Flags")

    flags = []
    null_ua = df["_platform"].eq("Null/Invalid").sum()
    if null_ua: flags.append(f"🔴 **{fmt(null_ua)} requests** have null/invalid UA — cannot be attributed to any platform")

    hls2disk_cnt = df["_platform"].eq("Service: hls2disk").sum()
    if hls2disk_cnt: flags.append(f"⚠️ **{fmt(hls2disk_cnt)} requests** from `hls2disk` — stream ripping tool detected")

    ffmpeg_cnt = df["_platform"].eq("Tool: FFmpeg").sum()
    if ffmpeg_cnt: flags.append(f"⚠️ **{fmt(ffmpeg_cnt)} requests** from FFmpeg/Lavf — potential stream scraping")

    palo_cnt = df["_platform"].eq("Scanner: Palo Alto").sum()
    if palo_cnt: flags.append(f"🛡️ **{fmt(palo_cnt)} requests** from Palo Alto Cortex Xpanse — CDN endpoint scanning detected")

    future_android = df[df["_android_ver"].notna() & (pd.to_numeric(df["_android_ver"], errors="coerce") >= 16)]
    if len(future_android): flags.append(f"⚠️ **{fmt(len(future_android))} requests** with Android 16+ — may be spoofed or custom AOSP builds")

    high_err = df.groupby(df["_dt"].dt.floor("1h"))["_is_error"].mean()
    spikes = high_err[high_err > 0.1]
    if len(spikes): flags.append(f"🔴 **{len(spikes)} hour(s)** with >10% error rate — check for incidents")

    if flags:
        for f_msg in flags:
            st.markdown(f_msg)
    else:
        st.success("✅ No significant anomalies detected in current selection.")

# ──────────────────────────────────────────────────────────────
# TAB 6 — RAW EXPLORER
# ──────────────────────────────────────────────────────────────
with tabs[6]:
    st.markdown("### Raw Data Explorer")
    st.markdown(f"Showing first **10,000** rows of current filtered selection ({fmt(len(df))} total)")

    cols_to_show = st.multiselect("Columns to display", options=list(df.columns),
        default=["_ua","_platform","_date","_hour","country","state","statusCode",
                 "throughput","timeToFirstByte","cacheStatus","reqHost","reqPath"])

    search_ua = st.text_input("Filter UA contains…", "")
    show_df = df.copy()
    if search_ua:
        show_df = show_df[show_df["_ua"].str.contains(search_ua, case=False, na=False)]

    st.dataframe(show_df[cols_to_show].head(10000), use_container_width=True, hide_index=True)

    # Download
    csv = show_df[cols_to_show].head(100000).to_csv(index=False).encode()
    st.download_button("⬇️ Download filtered data (CSV)", csv, "cdn_filtered.csv", "text/csv")

# ──────────────────────────────────────────────────────────────
# FOOTER
# ──────────────────────────────────────────────────────────────
st.markdown("---")
st.markdown(
    f'<div style="text-align:center;color:#6b7280;font-size:12px">'
    f'CDN Log Intelligence · {fmt(len(df))} rows in current view · '
    f'Built for Akamai parquet logs'
    f'</div>', unsafe_allow_html=True
)
