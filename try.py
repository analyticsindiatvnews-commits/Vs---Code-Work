# import re
# from pathlib import Path
# from urllib.parse import parse_qs, unquote_plus
# from datetime import datetime, time

# import numpy as np
# import pandas as pd
# import plotly.express as px
# import plotly.graph_objects as go
# import streamlit as st


# st.set_page_config(
#     page_title="User Behavior Dashboard",
#     page_icon="📺",
#     layout="wide"
# )


# # -------------------------------------------------
# # Helpers
# # -------------------------------------------------
# def safe_read_csv(path: str) -> pd.DataFrame:
#     return pd.read_csv(path, low_memory=False)


# def extract_qs_param(qs: str, key: str):
#     if pd.isna(qs):
#         return None
#     qs = str(qs)
#     try:
#         parsed = parse_qs(qs, keep_blank_values=True)
#         vals = parsed.get(key)
#         if vals:
#             return vals[0]
#     except Exception:
#         pass

#     m = re.search(rf"(?:^|&){re.escape(key)}=([^&]+)", qs)
#     if m:
#         return unquote_plus(m.group(1))
#     return None


# def extract_channel_from_path(path: str):
#     if pd.isna(path):
#         return None
#     m = re.search(r"(vglive-sk-\d+)", str(path))
#     return m.group(1) if m else None


# def extract_quality_from_path(path: str):
#     if pd.isna(path):
#         return None

#     path = str(path)

#     m = re.search(r"(\d+p)", path)
#     if m:
#         return m.group(1)

#     m2 = re.search(r"main_(\d+)\.m3u8", path)
#     if m2:
#         return f"variant_{m2.group(1)}"

#     if "main.m3u8" in path:
#         return "master"

#     if ".ts" in path:
#         return "segment"

#     return "unknown"


# def infer_device_type(ua: str, platform: str = None):
#     u = "" if pd.isna(ua) else str(ua).lower()
#     p = "" if pd.isna(platform) else str(platform).lower()

#     if "android_tv" in p or "smarttv" in u or "hismarttv" in u or "bravia" in u or "tv" in u:
#         return "Smart TV"
#     if "android" in u:
#         return "Android"
#     if "iphone" in u:
#         return "iPhone"
#     if "ipad" in u:
#         return "iPad"
#     if "windows" in u:
#         return "Windows"
#     if "mac" in u:
#         return "Mac"
#     return "Other"


# def build_sessions(df: pd.DataFrame, gap_minutes: int = 20) -> pd.DataFrame:
#     df = df.sort_values(["device_id", "event_time"]).copy()

#     df["prev_time"] = df.groupby("device_id")["event_time"].shift(1)
#     df["gap_min"] = (df["event_time"] - df["prev_time"]).dt.total_seconds() / 60

#     df["new_session_from_gap"] = df["gap_min"].isna() | (df["gap_min"] > gap_minutes)

#     if "session_id" in df.columns and df["session_id"].notna().any():
#         prev_sid = df.groupby("device_id")["session_id"].shift(1)
#         sid_changed = df["session_id"].fillna("__null__") != prev_sid.fillna("__null__")
#         df["new_session"] = df["new_session_from_gap"] | sid_changed
#     else:
#         df["new_session"] = df["new_session_from_gap"]

#     df["session_no"] = df.groupby("device_id")["new_session"].cumsum()
#     df["session_key"] = df["device_id"].astype(str) + "_S" + df["session_no"].astype(str)

#     return df


# def estimate_watch_minutes(df: pd.DataFrame, cap_seconds: int = 60) -> pd.DataFrame:
#     df = df.sort_values(["device_id", "event_time"]).copy()

#     next_time = df.groupby("device_id")["event_time"].shift(-1)
#     gap_sec = (next_time - df["event_time"]).dt.total_seconds()

#     df["watch_gap_sec_est"] = gap_sec.clip(lower=0, upper=cap_seconds).fillna(0)
#     df["watch_min_est"] = df["watch_gap_sec_est"] / 60

#     for c in ["transferTimeMSec", "downloadTime", "timeToFirstByte", "rspContentLen", "statusCode", "asn"]:
#         if c in df.columns:
#             df[c] = pd.to_numeric(df[c], errors="coerce")

#     return df


# def session_summary(df: pd.DataFrame) -> pd.DataFrame:
#     out = (
#         df.groupby("session_key")
#         .agg(
#             device_id=("device_id", "first"),
#             start=("event_time", "min"),
#             end=("event_time", "max"),
#             requests=("session_key", "size"),
#             unique_paths=("reqPath", "nunique"),
#             unique_channels=("channel_name", "nunique"),
#             est_watch_min=("watch_min_est", "sum"),
#             top_channel=("channel_name", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
#             top_title=("content_label", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
#             top_quality=("quality", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
#             top_asn=("asn", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
#         )
#         .reset_index()
#     )

#     out["session_duration_min"] = (out["end"] - out["start"]).dt.total_seconds() / 60
#     out["date"] = out["start"].dt.date
#     return out.sort_values("start")


# # -------------------------------------------------
# # Sidebar
# # -------------------------------------------------
# with st.sidebar:
#     st.header("Settings")

#     csv_path = st.text_input(
#         "CSV file path",
#         value=r"D:\full_rows_device_id_4cda938608ed98a8.csv"
#     )

#     session_gap_minutes = st.number_input(
#         "Session gap (minutes)",
#         min_value=5,
#         max_value=180,
#         value=20,
#         step=5
#     )

#     load_btn = st.button("Load Dashboard", type="primary")


# if not load_btn:
#     st.info("Enter CSV path and click Load Dashboard.")
#     st.stop()

# csv_file = Path(csv_path)
# if not csv_file.exists():
#     st.error(f"File not found: {csv_path}")
#     st.stop()


# # -------------------------------------------------
# # Load data
# # -------------------------------------------------
# try:
#     raw = safe_read_csv(csv_path)
# except Exception as e:
#     st.error(f"Could not read CSV: {e}")
#     st.stop()

# if raw.empty:
#     st.warning("CSV is empty.")
#     st.stop()

# required_cols = ["queryStr", "reqTimeSec", "reqPath"]
# missing = [c for c in required_cols if c not in raw.columns]
# if missing:
#     st.error(f"Missing required columns: {missing}")
#     st.stop()

# df = raw.copy()

# df["reqTimeSec"] = pd.to_numeric(df["reqTimeSec"], errors="coerce")
# df["event_time"] = pd.to_datetime(df["reqTimeSec"], unit="s", errors="coerce")
# df = df.dropna(subset=["event_time"]).copy()

# # query string params
# df["device_id"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "device_id"))
# df["session_id"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "session_id"))
# df["channel_param"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "channel"))
# df["content_type"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "content_type"))
# df["content_title"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "content_title"))
# df["platform"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "platform"))
# df["device_name_qs"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "device"))
# df["category_name"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "category_name"))

# # path-derived fields
# df["channel_from_path"] = df["reqPath"].apply(extract_channel_from_path)
# df["quality"] = df["reqPath"].apply(extract_quality_from_path)

# df["channel_name"] = df["channel_param"].fillna(df["channel_from_path"]).fillna("Unknown")
# df["content_label"] = df["content_title"].fillna(df["channel_name"]).fillna(df["reqPath"])
# df["device_type"] = df.apply(lambda r: infer_device_type(r.get("UA"), r.get("platform")), axis=1)

# device_ids = sorted([x for x in df["device_id"].dropna().astype(str).unique().tolist()])
# if not device_ids:
#     st.error("No device_id found inside queryStr.")
#     st.stop()


# # -------------------------------------------------
# # Main filters
# # -------------------------------------------------
# st.title("📺 User Behavior Dashboard")

# f1, f2 = st.columns(2)
# with f1:
#     selected_device = st.selectbox("Select device_id", device_ids)
# with f2:
#     device_df_all = df[df["device_id"].astype(str) == str(selected_device)].copy()
#     available_days = sorted(device_df_all["event_time"].dt.date.unique().tolist())
#     selected_day = st.selectbox("Select day", available_days)

# f3, f4, f5 = st.columns(3)
# with f3:
#     start_time = st.time_input("Start time", value=time(0, 0))
# with f4:
#     end_time = st.time_input("End time", value=time(23, 59))
# with f5:
#     min_requests_per_content = st.number_input(
#         "Min requests per content",
#         min_value=1,
#         max_value=1000,
#         value=1,
#         step=1
#     )

# device_df = device_df_all.copy()
# device_df = build_sessions(device_df, gap_minutes=session_gap_minutes)
# device_df = estimate_watch_minutes(device_df, cap_seconds=60)

# start_dt = pd.Timestamp(datetime.combine(selected_day, start_time))
# end_dt = pd.Timestamp(datetime.combine(selected_day, end_time))

# window_df = device_df[
#     (device_df["event_time"] >= start_dt) &
#     (device_df["event_time"] <= end_dt)
# ].copy()

# if window_df.empty:
#     st.warning("No activity found in this date/time range.")
#     st.stop()

# sess_df = session_summary(window_df)


# # -------------------------------------------------
# # KPIs
# # -------------------------------------------------
# first_seen = window_df["event_time"].min()
# last_seen = window_df["event_time"].max()
# sessions = window_df["session_key"].nunique()
# est_watch_hours = round(window_df["watch_min_est"].sum() / 60, 2)
# top_content = window_df["content_label"].mode().iloc[0] if not window_df["content_label"].mode().empty else "Unknown"
# top_channel = window_df["channel_name"].mode().iloc[0] if not window_df["channel_name"].mode().empty else "Unknown"
# top_platform = window_df["platform"].mode().iloc[0] if "platform" in window_df.columns and not window_df["platform"].mode().empty else "Unknown"
# top_device_type = window_df["device_type"].mode().iloc[0] if not window_df["device_type"].mode().empty else "Unknown"

# k1, k2, k3, k4, k5, k6 = st.columns(6)
# k1.metric("Rows", f"{len(window_df):,}")
# k2.metric("Sessions", f"{sessions:,}")
# k3.metric("Est. Watch Hours", f"{est_watch_hours}")
# k4.metric("Top Channel", str(top_channel))
# k5.metric("Top Content", str(top_content)[:25])
# k6.metric("Device Type", str(top_device_type))

# with st.expander("Window summary", expanded=True):
#     c1, c2 = st.columns(2)
#     with c1:
#         st.write(f"**device_id:** {selected_device}")
#         st.write(f"**Selected window:** {start_dt} to {end_dt}")
#         st.write(f"**First event:** {first_seen}")
#         st.write(f"**Last event:** {last_seen}")
#     with c2:
#         st.write(f"**Platform:** {top_platform}")
#         if "asn" in window_df.columns and not window_df["asn"].mode().empty:
#             st.write(f"**Most common ASN:** {window_df['asn'].mode().iloc[0]}")
#         if "cliIP" in window_df.columns and not window_df["cliIP"].mode().empty:
#             st.write(f"**Most common IP:** {window_df['cliIP'].mode().iloc[0]}")
#         if "device_name_qs" in window_df.columns and not window_df["device_name_qs"].mode().empty:
#             st.write(f"**Device name:** {window_df['device_name_qs'].mode().iloc[0]}")


# # -------------------------------------------------
# # 1 Timeline
# # -------------------------------------------------
# st.markdown("---")
# st.subheader("1) What the user watched in the selected time range")

# fig_timeline = px.scatter(
#     window_df.sort_values("event_time"),
#     x="event_time",
#     y="channel_name",
#     color="content_label",
#     hover_data=["reqPath", "content_title", "quality", "session_key", "asn", "platform"],
#     title="Watching history over time"
# )
# fig_timeline.update_traces(marker=dict(opacity=0.85, size=9))
# fig_timeline.update_layout(height=520)
# st.plotly_chart(fig_timeline, use_container_width=True)


# # -------------------------------------------------
# # 2 Minute activity
# # -------------------------------------------------
# st.subheader("2) Minute-by-minute activity")

# minute_activity = (
#     window_df.set_index("event_time")
#     .resample("1min")
#     .size()
#     .rename("requests")
#     .reset_index()
# )

# fig_minute = px.line(
#     minute_activity,
#     x="event_time",
#     y="requests",
#     title="Activity intensity"
# )
# fig_minute.update_layout(height=350)
# st.plotly_chart(fig_minute, use_container_width=True)


# # -------------------------------------------------
# # 3 Content summary
# # -------------------------------------------------
# st.subheader("3) Content watched in this time range")

# content_window = (
#     window_df.groupby(["content_label", "channel_name"])
#     .agg(
#         requests=("reqPath", "size"),
#         est_watch_min=("watch_min_est", "sum"),
#         first_seen=("event_time", "min"),
#         last_seen=("event_time", "max"),
#         sessions=("session_key", "nunique")
#     )
#     .reset_index()
#     .sort_values(["est_watch_min", "requests"], ascending=False)
# )

# content_window = content_window[content_window["requests"] >= min_requests_per_content]

# st.dataframe(content_window, use_container_width=True, height=350, hide_index=True)

# fig_content = px.bar(
#     content_window.head(15),
#     x="est_watch_min",
#     y="content_label",
#     color="channel_name",
#     orientation="h",
#     title="Top content by estimated watch minutes"
# )
# fig_content.update_layout(height=520, yaxis={"categoryorder": "total ascending"})
# st.plotly_chart(fig_content, use_container_width=True)


# # -------------------------------------------------
# # 4 Visited paths
# # -------------------------------------------------
# st.subheader("4) Where the user visited (paths/endpoints)")

# paths_window = (
#     window_df.groupby("reqPath")
#     .agg(
#         requests=("reqPath", "size"),
#         first_seen=("event_time", "min"),
#         last_seen=("event_time", "max"),
#         est_watch_min=("watch_min_est", "sum")
#     )
#     .reset_index()
#     .sort_values(["requests", "est_watch_min"], ascending=False)
#     .head(100)
# )

# st.dataframe(paths_window, use_container_width=True, height=350, hide_index=True)


# # -------------------------------------------------
# # 5 Session drill-down
# # -------------------------------------------------
# st.subheader("5) Session drill-down")

# session_options = sorted(window_df["session_key"].unique().tolist())
# selected_session = st.selectbox("Select session", session_options)

# session_window_df = window_df[window_df["session_key"] == selected_session].copy()

# s1, s2, s3, s4 = st.columns(4)
# s1.metric("Session Rows", f"{len(session_window_df):,}")
# s2.metric(
#     "Session Duration Min",
#     round((session_window_df["event_time"].max() - session_window_df["event_time"].min()).total_seconds() / 60, 2)
#     if len(session_window_df) > 0 else 0
# )
# s3.metric("Unique Content", f"{session_window_df['content_label'].nunique():,}")
# s4.metric("Est. Watch Min", round(session_window_df["watch_min_est"].sum(), 2))

# session_cols = [
#     "event_time", "channel_name", "content_label", "reqPath",
#     "quality", "platform", "asn", "statusCode", "watch_min_est", "session_key"
# ]
# session_cols = [c for c in session_cols if c in session_window_df.columns]

# st.dataframe(
#     session_window_df[session_cols].sort_values("event_time"),
#     use_container_width=True,
#     height=350,
#     hide_index=True
# )


# # -------------------------------------------------
# # 6 Content switching
# # -------------------------------------------------
# st.subheader("6) Content switching moments")

# switch_df = window_df.sort_values("event_time").copy()
# switch_df["prev_content"] = switch_df["content_label"].shift(1)
# switches = switch_df[switch_df["content_label"] != switch_df["prev_content"]][
#     ["event_time", "prev_content", "content_label", "channel_name", "session_key"]
# ].copy()

# st.dataframe(switches, use_container_width=True, height=250, hide_index=True)


# # -------------------------------------------------
# # 7 Likely watch starts
# # -------------------------------------------------
# st.subheader("7) Likely watch starts")

# watch_starts = (
#     window_df.sort_values("event_time")
#     .groupby("session_key")
#     .first()
#     .reset_index()[["session_key", "event_time", "content_label", "channel_name", "platform"]]
# )

# st.dataframe(watch_starts, use_container_width=True, height=250, hide_index=True)


# # -------------------------------------------------
# # 8 Network use
# # -------------------------------------------------
# if "asn" in window_df.columns:
#     st.subheader("8) Network usage in selected window")
#     asn_df = window_df["asn"].value_counts(dropna=False).reset_index()
#     asn_df.columns = ["asn", "requests"]
#     st.dataframe(asn_df, use_container_width=True, hide_index=True)


# # -------------------------------------------------
# # 9 Raw events
# # -------------------------------------------------
# st.subheader("9) Raw events in selected time range")

# raw_cols = [
#     "event_time", "session_key", "channel_name", "content_label", "content_title",
#     "platform", "device_type", "reqPath", "quality", "asn", "cliIP",
#     "statusCode", "transferTimeMSec", "downloadTime", "queryStr"
# ]
# raw_cols = [c for c in raw_cols if c in window_df.columns]

# st.dataframe(
#     window_df[raw_cols].sort_values("event_time"),
#     use_container_width=True,
#     height=450,
#     hide_index=True
# )


# # -------------------------------------------------
# # Downloads
# # -------------------------------------------------
# st.markdown("---")
# d1, d2, d3 = st.columns(3)

# with d1:
#     st.download_button(
#         "Download window raw events CSV",
#         data=window_df.to_csv(index=False).encode("utf-8"),
#         file_name=f"user_window_{selected_device}_{selected_day}.csv",
#         mime="text/csv"
#     )

# with d2:
#     st.download_button(
#         "Download session summary CSV",
#         data=sess_df.to_csv(index=False).encode("utf-8"),
#         file_name=f"user_sessions_{selected_device}_{selected_day}.csv",
#         mime="text/csv"
#     )

# with d3:
#     st.download_button(
#         "Download content summary CSV",
#         data=content_window.to_csv(index=False).encode("utf-8"),
#         file_name=f"user_content_{selected_device}_{selected_day}.csv",
#         mime="text/csv"
#     )


# # -------------------------------------------------
# # Notes
# # -------------------------------------------------
# with st.expander("Notes / how to read this"):
#     st.markdown("""
# - **device_id** is extracted from `queryStr`.
# - **Selected day + start/end time** lets you inspect a very specific window.
# - **Visited paths** means the exact request paths/endpoints hit by this user/device.
# - **Content watched** is inferred from `content_title`, `channel`, and `reqPath`.
# - **Estimated watch minutes** are based on time gaps between events and are capped to reduce fake inflation.
# - This is strong behavioral estimation, not perfect player telemetry.
# """)

import re
from pathlib import Path
from urllib.parse import parse_qs, unquote_plus
from datetime import datetime, time

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


st.set_page_config(
    page_title="User Behavior Dashboard",
    page_icon="📺",
    layout="wide"
)


# -------------------------------------------------
# Helpers
# -------------------------------------------------
def safe_read_csv(path: str) -> pd.DataFrame:
    return pd.read_csv(path, low_memory=False)


def extract_qs_param(qs: str, key: str):
    if pd.isna(qs):
        return None
    qs = str(qs)
    try:
        parsed = parse_qs(qs, keep_blank_values=True)
        vals = parsed.get(key)
        if vals:
            return vals[0]
    except Exception:
        pass

    m = re.search(rf"(?:^|&){re.escape(key)}=([^&]+)", qs)
    if m:
        return unquote_plus(m.group(1))
    return None


def extract_channel_from_path(path: str):
    if pd.isna(path):
        return None
    m = re.search(r"(vglive-sk-\d+)", str(path))
    return m.group(1) if m else None


def extract_quality_from_path(path: str):
    if pd.isna(path):
        return None

    path = str(path)

    m = re.search(r"(\d+p)", path)
    if m:
        return m.group(1)

    m2 = re.search(r"main_(\d+)\.m3u8", path)
    if m2:
        return f"variant_{m2.group(1)}"

    if "main.m3u8" in path:
        return "master"

    if ".ts" in path:
        return "segment"

    return "unknown"


def infer_device_type(ua: str, platform: str = None):
    u = "" if pd.isna(ua) else str(ua).lower()
    p = "" if pd.isna(platform) else str(platform).lower()

    if "android_tv" in p or "smarttv" in u or "hismarttv" in u or "bravia" in u or "tv" in u:
        return "Smart TV"
    if "android" in u:
        return "Android"
    if "iphone" in u:
        return "iPhone"
    if "ipad" in u:
        return "iPad"
    if "windows" in u:
        return "Windows"
    if "mac" in u:
        return "Mac"
    return "Other"


def build_sessions(df: pd.DataFrame, gap_minutes: int = 20) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()

    df["prev_time"] = df.groupby("device_id")["event_time"].shift(1)
    df["gap_min"] = (df["event_time"] - df["prev_time"]).dt.total_seconds() / 60

    df["new_session_from_gap"] = df["gap_min"].isna() | (df["gap_min"] > gap_minutes)

    if "session_id" in df.columns and df["session_id"].notna().any():
        prev_sid = df.groupby("device_id")["session_id"].shift(1)
        sid_changed = df["session_id"].fillna("__null__") != prev_sid.fillna("__null__")
        df["new_session"] = df["new_session_from_gap"] | sid_changed
    else:
        df["new_session"] = df["new_session_from_gap"]

    df["session_no"] = df.groupby("device_id")["new_session"].cumsum()
    df["session_key"] = df["device_id"].astype(str) + "_S" + df["session_no"].astype(str)

    return df


def estimate_watch_minutes(df: pd.DataFrame, cap_seconds: int = 60) -> pd.DataFrame:
    df = df.sort_values(["device_id", "event_time"]).copy()

    next_time = df.groupby("device_id")["event_time"].shift(-1)
    gap_sec = (next_time - df["event_time"]).dt.total_seconds()

    df["watch_gap_sec_est"] = gap_sec.clip(lower=0, upper=cap_seconds).fillna(0)
    df["watch_min_est"] = df["watch_gap_sec_est"] / 60

    for c in ["transferTimeMSec", "downloadTime", "timeToFirstByte", "rspContentLen", "statusCode", "asn"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def session_summary(df: pd.DataFrame) -> pd.DataFrame:
    out = (
        df.groupby("session_key")
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
            top_quality=("quality", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
            top_asn=("asn", lambda s: s.mode().iloc[0] if not s.mode().empty else None),
        )
        .reset_index()
    )

    out["session_duration_min"] = (out["end"] - out["start"]).dt.total_seconds() / 60
    out["date"] = out["start"].dt.date
    return out.sort_values("start")


# -------------------------------------------------
# Sidebar
# -------------------------------------------------
with st.sidebar:
    st.header("Settings")

    csv_path = st.text_input(
        "CSV file path",
        value=r"D:\full_rows_device_id_4cda938608ed98a8.csv"
    )

    session_gap_minutes = st.number_input(
        "Session gap (minutes)",
        min_value=5,
        max_value=180,
        value=20,
        step=5
    )

    load_btn = st.button("Load Dashboard", type="primary")


if not load_btn:
    st.info("Enter CSV path and click Load Dashboard.")
    st.stop()

csv_file = Path(csv_path)
if not csv_file.exists():
    st.error(f"File not found: {csv_path}")
    st.stop()


# -------------------------------------------------
# Load data
# -------------------------------------------------
try:
    raw = safe_read_csv(csv_path)
except Exception as e:
    st.error(f"Could not read CSV: {e}")
    st.stop()

if raw.empty:
    st.warning("CSV is empty.")
    st.stop()

required_cols = ["queryStr", "reqTimeSec", "reqPath"]
missing = [c for c in required_cols if c not in raw.columns]
if missing:
    st.error(f"Missing required columns: {missing}")
    st.stop()

df = raw.copy()

df["reqTimeSec"] = pd.to_numeric(df["reqTimeSec"], errors="coerce")
df["event_time"] = pd.to_datetime(df["reqTimeSec"], unit="s", errors="coerce")
df = df.dropna(subset=["event_time"]).copy()

# query string params
df["device_id"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "device_id"))
df["session_id"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "session_id"))
df["channel_param"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "channel"))
df["content_type"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "content_type"))
df["content_title"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "content_title"))
df["platform"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "platform"))
df["device_name_qs"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "device"))
df["category_name"] = df["queryStr"].apply(lambda x: extract_qs_param(x, "category_name"))

# path-derived fields
df["channel_from_path"] = df["reqPath"].apply(extract_channel_from_path)
df["quality"] = df["reqPath"].apply(extract_quality_from_path)

df["channel_name"] = df["channel_param"].fillna(df["channel_from_path"]).fillna("Unknown")
df["content_label"] = df["content_title"].fillna(df["channel_name"]).fillna(df["reqPath"])
df["device_type"] = df.apply(lambda r: infer_device_type(r.get("UA"), r.get("platform")), axis=1)

device_ids = sorted([x for x in df["device_id"].dropna().astype(str).unique().tolist()])
if not device_ids:
    st.error("No device_id found inside queryStr.")
    st.stop()


st.title("📺 User Behavior Dashboard")

# -----------------------------
# Device selection
# -----------------------------
selected_device = st.selectbox("Select device_id", device_ids)

device_df_all = df[df["device_id"].astype(str) == str(selected_device)].copy()

# -----------------------------
# DATE RANGE (IMPORTANT FIX)
# -----------------------------
all_dates = device_df_all["event_time"].dt.date

date_range = st.date_input(
    "Select date range",
    value=(all_dates.min(), all_dates.max()),
    min_value=all_dates.min(),
    max_value=all_dates.max()
)

# handle selection
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = all_dates.min()
    end_date = all_dates.max()

# -----------------------------
# TIME RANGE (per day)
# -----------------------------
c1, c2 = st.columns(2)

with c1:
    start_time = st.time_input("Start time", value=time(0, 0))

with c2:
    end_time = st.time_input("End time", value=time(23, 59))

# -----------------------------
# APPLY FILTERS
# -----------------------------
device_df = device_df_all[
    (device_df_all["event_time"].dt.date >= start_date) &
    (device_df_all["event_time"].dt.date <= end_date)
].copy()

# Apply session + watch logic AFTER filtering
device_df = build_sessions(device_df, gap_minutes=session_gap_minutes)
device_df = estimate_watch_minutes(device_df, cap_seconds=60)

# 🔥 IMPORTANT: apply TIME filter inside each day
device_df["time_only"] = device_df["event_time"].dt.time

window_df = device_df[
    (device_df["time_only"] >= start_time) &
    (device_df["time_only"] <= end_time)
].copy()

if window_df.empty:
    st.warning("No activity found in selected date + time range.")
    st.stop()

# -------------------------------------------------
# KPIs
# -------------------------------------------------
first_seen = window_df["event_time"].min()
last_seen = window_df["event_time"].max()
sessions = window_df["session_key"].nunique()
est_watch_hours = round(window_df["watch_min_est"].sum() / 60, 2)
top_content = window_df["content_label"].mode().iloc[0] if not window_df["content_label"].mode().empty else "Unknown"
top_channel = window_df["channel_name"].mode().iloc[0] if not window_df["channel_name"].mode().empty else "Unknown"
top_platform = window_df["platform"].mode().iloc[0] if "platform" in window_df.columns and not window_df["platform"].mode().empty else "Unknown"
top_device_type = window_df["device_type"].mode().iloc[0] if not window_df["device_type"].mode().empty else "Unknown"

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Rows", f"{len(window_df):,}")
k2.metric("Sessions", f"{sessions:,}")
k3.metric("Est. Watch Hours", f"{est_watch_hours}")
k4.metric("Top Channel", str(top_channel))
k5.metric("Top Content", str(top_content)[:25])
k6.metric("Device Type", str(top_device_type))

with st.expander("Window summary", expanded=True):
    c1, c2 = st.columns(2)
    with c1:
        st.write(f"**device_id:** {selected_device}")
        st.write(f"**Selected window:** {start_dt} to {end_dt}")
        st.write(f"**First event:** {first_seen}")
        st.write(f"**Last event:** {last_seen}")
    with c2:
        st.write(f"**Platform:** {top_platform}")
        if "asn" in window_df.columns and not window_df["asn"].mode().empty:
            st.write(f"**Most common ASN:** {window_df['asn'].mode().iloc[0]}")
        if "cliIP" in window_df.columns and not window_df["cliIP"].mode().empty:
            st.write(f"**Most common IP:** {window_df['cliIP'].mode().iloc[0]}")
        if "device_name_qs" in window_df.columns and not window_df["device_name_qs"].mode().empty:
            st.write(f"**Device name:** {window_df['device_name_qs'].mode().iloc[0]}")


# -------------------------------------------------
# 1 Timeline
# -------------------------------------------------
st.markdown("---")
st.subheader("1) What the user watched in the selected time range")

fig_timeline = px.scatter(
    window_df.sort_values("event_time"),
    x="event_time",
    y="channel_name",
    color="content_label",
    hover_data=["reqPath", "content_title", "quality", "session_key", "asn", "platform"],
    title="Watching history over time"
)
fig_timeline.update_traces(marker=dict(opacity=0.85, size=9))
fig_timeline.update_layout(height=520)
st.plotly_chart(fig_timeline, use_container_width=True)


# -------------------------------------------------
# 2 Minute activity
# -------------------------------------------------
st.subheader("2) Minute-by-minute activity")

minute_activity = (
    window_df.set_index("event_time")
    .resample("1min")
    .size()
    .rename("requests")
    .reset_index()
)

fig_minute = px.line(
    minute_activity,
    x="event_time",
    y="requests",
    title="Activity intensity"
)
fig_minute.update_layout(height=350)
st.plotly_chart(fig_minute, use_container_width=True)


# -------------------------------------------------
# 3 Content summary
# -------------------------------------------------
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


# -------------------------------------------------
# 4 Visited paths
# -------------------------------------------------
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


# -------------------------------------------------
# 5 Session drill-down
# -------------------------------------------------
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

session_cols = [
    "event_time", "channel_name", "content_label", "reqPath",
    "quality", "platform", "asn", "statusCode", "watch_min_est", "session_key"
]
session_cols = [c for c in session_cols if c in session_window_df.columns]

st.dataframe(
    session_window_df[session_cols].sort_values("event_time"),
    use_container_width=True,
    height=350,
    hide_index=True
)


# -------------------------------------------------
# 6 Content switching
# -------------------------------------------------
st.subheader("6) Content switching moments")

switch_df = window_df.sort_values("event_time").copy()
switch_df["prev_content"] = switch_df["content_label"].shift(1)
switches = switch_df[switch_df["content_label"] != switch_df["prev_content"]][
    ["event_time", "prev_content", "content_label", "channel_name", "session_key"]
].copy()

st.dataframe(switches, use_container_width=True, height=250, hide_index=True)


# -------------------------------------------------
# 7 Likely watch starts
# -------------------------------------------------
st.subheader("7) Likely watch starts")

watch_starts = (
    window_df.sort_values("event_time")
    .groupby("session_key")
    .first()
    .reset_index()[["session_key", "event_time", "content_label", "channel_name", "platform"]]
)

st.dataframe(watch_starts, use_container_width=True, height=250, hide_index=True)


# -------------------------------------------------
# 8 Network use
# -------------------------------------------------
if "asn" in window_df.columns:
    st.subheader("8) Network usage in selected window")
    asn_df = window_df["asn"].value_counts(dropna=False).reset_index()
    asn_df.columns = ["asn", "requests"]
    st.dataframe(asn_df, use_container_width=True, hide_index=True)


# -------------------------------------------------
# 9 Raw events
# -------------------------------------------------
st.subheader("9) Raw events in selected time range")

raw_cols = [
    "event_time", "session_key", "channel_name", "content_label", "content_title",
    "platform", "device_type", "reqPath", "quality", "asn", "cliIP",
    "statusCode", "transferTimeMSec", "downloadTime", "queryStr"
]
raw_cols = [c for c in raw_cols if c in window_df.columns]

st.dataframe(
    window_df[raw_cols].sort_values("event_time"),
    use_container_width=True,
    height=450,
    hide_index=True
)


# -------------------------------------------------
# Downloads
# -------------------------------------------------
st.markdown("---")
d1, d2, d3 = st.columns(3)

with d1:
    st.download_button(
        "Download window raw events CSV",
        data=window_df.to_csv(index=False).encode("utf-8"),
        file_name=f"user_window_{selected_device}_{selected_day}.csv",
        mime="text/csv"
    )

with d2:
    st.download_button(
        "Download session summary CSV",
        data=sess_df.to_csv(index=False).encode("utf-8"),
        file_name=f"user_sessions_{selected_device}_{selected_day}.csv",
        mime="text/csv"
    )

with d3:
    st.download_button(
        "Download content summary CSV",
        data=content_window.to_csv(index=False).encode("utf-8"),
        file_name=f"user_content_{selected_device}_{selected_day}.csv",
        mime="text/csv"
    )


# -------------------------------------------------
# Notes
# -------------------------------------------------
with st.expander("Notes / how to read this"):
    st.markdown("""
- **device_id** is extracted from `queryStr`.
- **Selected day + start/end time** lets you inspect a very specific window.
- **Visited paths** means the exact request paths/endpoints hit by this user/device.
- **Content watched** is inferred from `content_title`, `channel`, and `reqPath`.
- **Estimated watch minutes** are based on time gaps between events and are capped to reduce fake inflation.
- This is strong behavioral estimation, not perfect player telemetry.
""")