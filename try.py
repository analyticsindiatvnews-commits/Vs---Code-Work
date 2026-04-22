import re
from pathlib import Path
from datetime import time
from urllib.parse import unquote_plus

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st


st.set_page_config(
    page_title="User Behavior Dashboard - Parquet",
    page_icon="📺",
    layout="wide"
)

DEFAULT_PARQUET_PATH = r"Z:\Veto Logs Parquet\01.parquet"
WATCH_GAP_CAP_SECONDS = 60


# =================================================
# Helpers
# =================================================
def extract_channel_from_path_series(path_s: pd.Series) -> pd.Series:
    return path_s.astype(str).str.extract(r"(vglive-sk-\d+)", expand=False)


def extract_quality_from_path_series(path_s: pd.Series) -> pd.Series:
    s = path_s.fillna("").astype(str)

    quality = s.str.extract(r"(\d+p)", expand=False)
    variant = s.str.extract(r"main_(\d+)\.m3u8", expand=False)

    out = quality.copy()
    out = out.where(out.notna(), "variant_" + variant.astype(str))
    out = out.where(~s.str.contains("main.m3u8", na=False), "master")
    out = out.where(~s.str.contains(r"\.ts", na=False), "segment")
    out = out.fillna("unknown")
    out = out.replace("variant_nan", "unknown")
    return out


def infer_device_type_vectorized(ua_s: pd.Series, platform_s: pd.Series) -> pd.Series:
    ua = ua_s.fillna("").astype(str).str.lower()
    platform = platform_s.fillna("").astype(str).str.lower()

    out = pd.Series("Other", index=ua.index)

    smart_tv_mask = (
        platform.str.contains("android_tv", na=False)
        | ua.str.contains("smarttv|hismarttv|bravia", na=False)
        | ua.str.contains(r"\btv\b", na=False)
    )
    android_mask = ua.str.contains("android", na=False)
    iphone_mask = ua.str.contains("iphone", na=False)
    ipad_mask = ua.str.contains("ipad", na=False)
    windows_mask = ua.str.contains("windows", na=False)
    mac_mask = ua.str.contains("mac", na=False)

    out = out.mask(smart_tv_mask, "Smart TV")
    out = out.mask(~smart_tv_mask & android_mask, "Android")
    out = out.mask(iphone_mask, "iPhone")
    out = out.mask(ipad_mask, "iPad")
    out = out.mask(windows_mask, "Windows")
    out = out.mask(mac_mask, "Mac")

    return out


def build_sessions(df: pd.DataFrame, gap_minutes: int = 20) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()

    grp = df.groupby("device_id", sort=False)

    df["prev_time"] = grp["event_time"].shift(1)
    df["gap_min"] = (df["event_time"] - df["prev_time"]).dt.total_seconds() / 60

    first_row = df["prev_time"].isna()
    long_gap = df["gap_min"] > gap_minutes

    if "session_id" in df.columns:
        df["prev_session_id"] = grp["session_id"].shift(1)
        session_id_changed = (
            df["session_id"].fillna("__null__") != df["prev_session_id"].fillna("__null__")
        )
    else:
        session_id_changed = pd.Series(False, index=df.index)

    df["prev_channel"] = grp["channel_name"].shift(1)
    channel_changed = df["channel_name"].fillna("__null__") != df["prev_channel"].fillna("__null__")

    if "cliIP" in df.columns:
        df["prev_ip"] = grp["cliIP"].shift(1)
        ip_changed = df["cliIP"].astype(str).fillna("__null__") != df["prev_ip"].astype(str).fillna("__null__")
    else:
        ip_changed = pd.Series(False, index=df.index)

    if "asn" in df.columns:
        df["prev_asn"] = grp["asn"].shift(1)
        asn_changed = df["asn"].astype(str).fillna("__null__") != df["prev_asn"].astype(str).fillna("__null__")
    else:
        asn_changed = pd.Series(False, index=df.index)

    df["prev_platform"] = grp["platform"].shift(1)
    platform_changed = df["platform"].fillna("__null__") != df["prev_platform"].fillna("__null__")

    medium_gap = df["gap_min"] > 8
    context_score = (
        channel_changed.astype(int)
        + ip_changed.astype(int)
        + asn_changed.astype(int)
        + platform_changed.astype(int)
    )
    smart_break = medium_gap & (context_score >= 2)

    df["new_session"] = first_row | long_gap | session_id_changed | smart_break
    df["session_no"] = grp["new_session"].cumsum()
    df["session_key"] = df["device_id"].astype(str) + "_S" + df["session_no"].astype(str)

    return df


def estimate_watch_minutes(df: pd.DataFrame, cap_seconds: int = WATCH_GAP_CAP_SECONDS) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()
    next_time = df.groupby("device_id", sort=False)["event_time"].shift(-1)
    gap_sec = (next_time - df["event_time"]).dt.total_seconds()
    df["watch_gap_sec_est"] = gap_sec.clip(lower=0, upper=cap_seconds).fillna(0)
    df["watch_min_est"] = df["watch_gap_sec_est"] / 60
    return df


def session_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("session_key", sort=False)
        .agg(
            device_id=("device_id", "first"),
            start=("event_time", "min"),
            end=("event_time", "max"),
            requests=("session_key", "size"),
            unique_paths=("reqPath", "nunique"),
            unique_channels=("channel_name", "nunique"),
            est_watch_min=("watch_min_est", "sum"),
            top_channel=("channel_name", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
            top_title=("content_label", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
        )
        .reset_index()
    )
    out["session_duration_min"] = (out["end"] - out["start"]).dt.total_seconds() / 60
    return out.sort_values("start").reset_index(drop=True)


@st.cache_resource
def get_conn():
    con = duckdb.connect(database=":memory:")
    con.execute("PRAGMA threads=4")
    return con


@st.cache_data(show_spinner="Scanning device IDs from parquet...", ttl=1800)
def get_device_ids(parquet_path: str) -> list[str]:
    con = get_conn()
    query = f"""
    SELECT DISTINCT regexp_extract(queryStr, '(?:^|&)device_id=([^&]+)', 1) AS device_id
    FROM read_parquet('{parquet_path}')
    WHERE queryStr IS NOT NULL
      AND regexp_extract(queryStr, '(?:^|&)device_id=([^&]+)', 1) <> ''
    ORDER BY 1
    """
    df = con.execute(query).df()
    return df["device_id"].astype(str).tolist()


@st.cache_data(show_spinner="Loading selected device from parquet...", ttl=600)
def load_device_slice(parquet_path: str, selected_device: str, start_date, end_date) -> pd.DataFrame:
    con = get_conn()
    query = f"""
    SELECT
        queryStr,
        reqTimeSec,
        reqPath,
        UA,
        cliIP,
        asn,
        statusCode,
        transferTimeMSec,
        downloadTime,
        regexp_extract(queryStr, '(?:^|&)device_id=([^&]+)', 1) AS device_id,
        regexp_extract(queryStr, '(?:^|&)session_id=([^&]+)', 1) AS session_id,
        regexp_extract(queryStr, '(?:^|&)channel=([^&]+)', 1) AS channel_param,
        regexp_extract(queryStr, '(?:^|&)content_type=([^&]+)', 1) AS content_type,
        regexp_extract(queryStr, '(?:^|&)content_title=([^&]+)', 1) AS content_title,
        regexp_extract(queryStr, '(?:^|&)platform=([^&]+)', 1) AS platform,
        regexp_extract(queryStr, '(?:^|&)device=([^&]+)', 1) AS device_name_qs,
        regexp_extract(queryStr, '(?:^|&)category_name=([^&]+)', 1) AS category_name
    FROM read_parquet('{parquet_path}')
    WHERE regexp_extract(queryStr, '(?:^|&)device_id=([^&]+)', 1) = ?
      AND to_timestamp(TRY_CAST(reqTimeSec AS BIGINT))::DATE BETWEEN ? AND ?
    ORDER BY TRY_CAST(reqTimeSec AS BIGINT)
    """
    return con.execute(query, [selected_device, str(start_date), str(end_date)]).df()


def enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()

    out["reqTimeSec"] = pd.to_numeric(out["reqTimeSec"], errors="coerce")
    out["event_time"] = pd.to_datetime(out["reqTimeSec"], unit="s", errors="coerce")
    out = out.dropna(subset=["event_time"]).copy()

    for col in ["channel_param", "content_type", "content_title", "platform", "device_name_qs", "category_name", "device_id", "session_id"]:
        if col in out.columns:
            out[col] = out[col].fillna("").astype(str).map(unquote_plus)

    out["channel_from_path"] = extract_channel_from_path_series(out["reqPath"])
    out["quality"] = extract_quality_from_path_series(out["reqPath"])

    out["channel_name"] = out["channel_param"].replace("", pd.NA).fillna(out["channel_from_path"]).fillna("Unknown")
    out["content_label"] = out["content_title"].replace("", pd.NA).fillna(out["channel_name"]).fillna(out["reqPath"])

    ua_col = out["UA"] if "UA" in out.columns else pd.Series("", index=out.index)
    out["device_type"] = infer_device_type_vectorized(ua_col, out["platform"])

    for c in ["transferTimeMSec", "downloadTime", "statusCode", "asn"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    out["event_date"] = out["event_time"].dt.date
    out["time_only"] = out["event_time"].dt.time
    out["watch_date"] = out["event_date"]
    out["watch_time_min"] = (
        out["event_time"].dt.hour * 60
        + out["event_time"].dt.minute
        + out["event_time"].dt.second / 60
    )

    return out.sort_values(["device_id", "event_time"]).reset_index(drop=True)


# =================================================
# Sidebar
# =================================================
with st.sidebar:
    st.header("Settings")
    parquet_path = st.text_input("Parquet file path", value=DEFAULT_PARQUET_PATH)
    session_gap_minutes = st.number_input("Session gap (minutes)", min_value=5, max_value=180, value=20, step=5)
    load_btn = st.button("Load Dashboard", type="primary")

if not load_btn:
    st.info("Enter parquet path and click Load Dashboard.")
    st.stop()

parquet_file = Path(parquet_path)
if not parquet_file.exists():
    st.error(f"File not found: {parquet_path}")
    st.stop()

# =================================================
# Device selector
# =================================================
try:
    device_ids = get_device_ids(parquet_path)
except Exception as e:
    st.error(f"Could not scan device IDs: {e}")
    st.stop()

if not device_ids:
    st.error("No device_id found inside queryStr.")
    st.stop()

st.title("📺 User Behavior Dashboard - Parquet")

selected_device = st.selectbox("Select device_id", device_ids)

tmp_df = load_device_slice(parquet_path, selected_device, "1970-01-01", "2100-01-01")
tmp_df = enrich_df(tmp_df)

if tmp_df.empty:
    st.warning("No rows found for selected device.")
    st.stop()

all_dates = tmp_df["event_date"]

date_range = st.date_input(
    "Select date range",
    value=(all_dates.min(), all_dates.max()),
    min_value=all_dates.min(),
    max_value=all_dates.max()
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = all_dates.min(), all_dates.max()

c1, c2, c3 = st.columns(3)
with c1:
    start_time = st.time_input("Start time", value=time(0, 0))
with c2:
    end_time = st.time_input("End time", value=time(23, 59))
with c3:
    min_requests_per_content = st.number_input("Min requests per content", min_value=1, max_value=1000, value=1, step=1)

device_df = load_device_slice(parquet_path, selected_device, start_date, end_date)
device_df = enrich_df(device_df)

device_df = build_sessions(device_df, gap_minutes=session_gap_minutes)
device_df = estimate_watch_minutes(device_df, cap_seconds=WATCH_GAP_CAP_SECONDS)

window_df = device_df[
    (device_df["time_only"] >= start_time) &
    (device_df["time_only"] <= end_time)
].copy()

if window_df.empty:
    st.warning("No activity found in selected date + time range.")
    st.stop()

sess_df = session_summary(window_df)

# =================================================
# KPIs
# =================================================
first_seen = window_df["event_time"].min()
last_seen = window_df["event_time"].max()
sessions = window_df["session_key"].nunique()
est_watch_hours = round(window_df["watch_min_est"].sum() / 60, 2)
top_content = window_df["content_label"].mode().iloc[0] if not window_df["content_label"].mode().empty else "Unknown"
top_channel = window_df["channel_name"].mode().iloc[0] if not window_df["channel_name"].mode().empty else "Unknown"
top_platform = window_df["platform"].mode().iloc[0] if not window_df["platform"].mode().empty else "Unknown"
top_device_type = window_df["device_type"].mode().iloc[0] if not window_df["device_type"].mode().empty else "Unknown"

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Rows", f"{len(window_df):,}")
k2.metric("Sessions", f"{sessions:,}")
k3.metric("Est. Watch Hours", f"{est_watch_hours}")
k4.metric("Top Channel", str(top_channel))
k5.metric("Top Content", str(top_content)[:25])
k6.metric("Device Type", str(top_device_type))

with st.expander("Window summary", expanded=True):
    s1, s2 = st.columns(2)
    with s1:
        st.write(f"**device_id:** {selected_device}")
        st.write(f"**Selected window:** {start_date} {start_time} to {end_date} {end_time}")
        st.write(f"**First event:** {first_seen}")
        st.write(f"**Last event:** {last_seen}")
    with s2:
        st.write(f"**Platform:** {top_platform}")
        if "asn" in window_df.columns and not window_df["asn"].mode().empty:
            st.write(f"**Most common ASN:** {window_df['asn'].mode().iloc[0]}")
        if "cliIP" in window_df.columns and not window_df["cliIP"].mode().empty:
            st.write(f"**Most common IP:** {window_df['cliIP'].mode().iloc[0]}")
        if "device_name_qs" in window_df.columns and not window_df["device_name_qs"].mode().empty:
            st.write(f"**Device name:** {window_df['device_name_qs'].mode().iloc[0]}")

st.markdown("---")
st.subheader("1) What the user watched in the selected time range")
fig_timeline = px.scatter(
    window_df,
    x="event_time",
    y="channel_name",
    color="content_label",
    hover_data=["reqPath", "content_title", "quality", "session_key", "asn", "platform"],
    title="Watching history over time"
)
fig_timeline.update_traces(marker=dict(opacity=0.85, size=9))
fig_timeline.update_layout(height=520)
st.plotly_chart(fig_timeline, use_container_width=True)

st.subheader("1B) Date vs Time of Day (behavior pattern)")
fig_time_map = px.scatter(
    window_df,
    x="watch_date",
    y="watch_time_min",
    color="channel_name",
    hover_data=["event_time", "content_label", "reqPath", "quality", "session_key", "asn", "platform"],
    title="User behavior by date and time of day"
)
fig_time_map.update_traces(marker=dict(size=8, opacity=0.8))
fig_time_map.update_layout(height=550, xaxis_title="Date", yaxis_title="Time of Day")
tick_vals = list(range(0, 1441, 60))
tick_text = [f"{h:02d}:00" for h in range(25)]
fig_time_map.update_yaxes(tickvals=tick_vals, ticktext=tick_text, range=[0, 1440])
st.plotly_chart(fig_time_map, use_container_width=True)

st.subheader("2) Minute-by-minute activity")
minute_activity = (
    window_df.set_index("event_time")
    .resample("1min")
    .size()
    .rename("requests")
    .reset_index()
)
fig_minute = px.line(minute_activity, x="event_time", y="requests", title="Activity intensity")
fig_minute.update_layout(height=350)
st.plotly_chart(fig_minute, use_container_width=True)

st.subheader("3) Content watched in this time range")
content_window = (
    window_df.groupby(["content_label", "channel_name"])
    .agg(
        requests=("reqPath", "size"),
        est_watch_min=("watch_min_est", "sum"),
        first_seen=("event_time", "min"),
        last_seen=("event_time", "max"),
        sessions=("session_key", "nunique")
    )
    .reset_index()
    .sort_values(["est_watch_min", "requests"], ascending=False)
)
content_window = content_window[content_window["requests"] >= min_requests_per_content]
st.dataframe(content_window, use_container_width=True, height=350, hide_index=True)

fig_content = px.bar(
    content_window.head(15),
    x="est_watch_min",
    y="content_label",
    color="channel_name",
    orientation="h",
    title="Top content by estimated watch minutes"
)
fig_content.update_layout(height=520, yaxis={"categoryorder": "total ascending"})
st.plotly_chart(fig_content, use_container_width=True)

st.subheader("4) Where the user visited (paths/endpoints)")
paths_window = (
    window_df.groupby("reqPath")
    .agg(
        requests=("reqPath", "size"),
        first_seen=("event_time", "min"),
        last_seen=("event_time", "max"),
        est_watch_min=("watch_min_est", "sum")
    )
    .reset_index()
    .sort_values(["requests", "est_watch_min"], ascending=False)
    .head(100)
)
st.dataframe(paths_window, use_container_width=True, height=350, hide_index=True)

st.subheader("5) Session drill-down")
session_options = sorted(window_df["session_key"].unique().tolist())
selected_session = st.selectbox("Select session", session_options)
session_window_df = window_df[window_df["session_key"] == selected_session].copy()

s1, s2, s3, s4 = st.columns(4)
s1.metric("Session Rows", f"{len(session_window_df):,}")
s2.metric(
    "Session Duration Min",
    round((session_window_df["event_time"].max() - session_window_df["event_time"].min()).total_seconds() / 60, 2)
    if len(session_window_df) > 0 else 0
)
s3.metric("Unique Content", f"{session_window_df['content_label'].nunique():,}")
s4.metric("Est. Watch Min", round(session_window_df["watch_min_est"].sum(), 2))

session_cols = ["event_time", "channel_name", "content_label", "reqPath", "quality", "platform", "asn", "statusCode", "watch_min_est", "session_key"]
session_cols = [c for c in session_cols if c in session_window_df.columns]
st.dataframe(session_window_df[session_cols].sort_values("event_time"), use_container_width=True, height=350, hide_index=True)

st.subheader("6) Content switching moments")
switch_df = window_df.sort_values("event_time").copy()
switch_df["prev_content"] = switch_df["content_label"].shift(1)
switches = switch_df[switch_df["content_label"] != switch_df["prev_content"]][
    ["event_time", "prev_content", "content_label", "channel_name", "session_key"]
].copy()
st.dataframe(switches, use_container_width=True, height=250, hide_index=True)

st.subheader("7) Likely watch starts")
watch_starts = (
    window_df.sort_values("event_time")
    .groupby("session_key")
    .first()
    .reset_index()[["session_key", "event_time", "content_label", "channel_name", "platform"]]
)
st.dataframe(watch_starts, use_container_width=True, height=250, hide_index=True)

if "asn" in window_df.columns:
    st.subheader("8) Network usage in selected window")
    asn_df = window_df["asn"].value_counts(dropna=False).reset_index()
    asn_df.columns = ["asn", "requests"]
    st.dataframe(asn_df, use_container_width=True, hide_index=True)

st.subheader("9) Raw events in selected time range")
raw_cols = [
    "event_time", "session_key", "channel_name", "content_label", "content_title",
    "platform", "device_type", "reqPath", "quality", "asn", "cliIP",
    "statusCode", "transferTimeMSec", "downloadTime", "queryStr"
]
raw_cols = [c for c in raw_cols if c in window_df.columns]
st.dataframe(window_df[raw_cols].sort_values("event_time"), use_container_width=True, height=450, hide_index=True)

st.markdown("---")
d1, d2, d3 = st.columns(3)
date_label = f"{start_date}_to_{end_date}"

with d1:
    st.download_button(
        "Download window raw events CSV",
        data=window_df.to_csv(index=False).encode("utf-8"),
        file_name=f"user_window_{selected_device}_{date_label}.csv",
        mime="text/csv"
    )
with d2:
    st.download_button(
        "Download session summary CSV",
        data=sess_df.to_csv(index=False).encode("utf-8"),
        file_name=f"user_sessions_{selected_device}_{date_label}.csv",
        mime="text/csv"
    )
with d3:
    st.download_button(
        "Download content summary CSV",
        data=content_window.to_csv(index=False).encode("utf-8"),
        file_name=f"user_content_{selected_device}_{date_label}.csv",
        mime="text/csv"
    )

with st.expander("Notes / how to read this"):
    st.markdown("""
- **device_id** is extracted from `queryStr`.
- **Date range + time range** lets you inspect recurring behavior across multiple days.
- **Visited paths** means the exact request paths/endpoints hit by this user/device.
- **Content watched** is inferred from `content_title`, `channel`, and `reqPath`.
- **Estimated watch minutes** are based on time gaps between events and are capped to reduce fake inflation.
- This is strong behavioral estimation, not perfect player telemetry.
""")