"""
parquet_explorer.py  —  Visual Parquet Explorer (multi-folder)
==============================================================
Run:  streamlit run parquet_explorer.py

Features
--------
✓ Add multiple folders (same or different column sets)
✓ See every column + data type across all folders
✓ Unique values + counts across all folders combined
✓ Filter rows by column values
✓ Preview rows (shows which folder each row came from)
✓ Export filtered result to CSV
✓ Lazy reading — works with millions of rows

pip install streamlit pandas pyarrow
"""

import time
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc
import streamlit as st

# ─────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────
st.set_page_config(
    page_title="Parquet Explorer",
    page_icon="🗂️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    .stMetric label  { font-size: 0.78rem; color: #888; }
    div[data-testid="stSidebarContent"] { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Session state init
# ─────────────────────────────────────────────
if "folders" not in st.session_state:
    st.session_state.folders = [""]          # list of folder path strings
if "last_unique_col" not in st.session_state:
    st.session_state.last_unique_col = None
if "last_unique_vals" not in st.session_state:
    st.session_state.last_unique_vals = []


# ─────────────────────────────────────────────
# Helpers — all accept list[Path] of files
# ─────────────────────────────────────────────

def collect_files(folders: list[str]) -> list[Path]:
    """Gather all .parquet files from all valid folders."""
    files = []
    for f in folders:
        p = Path(f.strip())
        if p.is_dir():
            files.extend(sorted(p.glob("*.parquet")))
    return files


@st.cache_data(show_spinner="Reading schema …")
def load_schema(folder_key: str) -> tuple[list[str], dict]:
    """
    folder_key is a |-joined string of all folder paths (used as cache key).
    Returns union of columns across all folders.
    """
    folders = folder_key.split("|")
    files   = collect_files(folders)
    if not files:
        return [], {}

    all_cols: dict[str, str] = {}
    # read schema from first file of each folder to get full union
    seen_folders = set()
    for f in files:
        folder_str = str(f.parent)
        if folder_str in seen_folders:
            continue
        seen_folders.add(folder_str)
        try:
            schema = pq.read_schema(f)
            for name in schema.names:
                if name not in all_cols:
                    all_cols[name] = str(schema.field(name).type)
        except Exception:
            continue

    return list(all_cols.keys()), all_cols


@st.cache_data(show_spinner="Counting files and rows …")
def count_stats(folder_key: str) -> tuple[int, int, int]:
    folders    = folder_key.split("|")
    files      = collect_files(folders)
    total_rows = 0
    for f in files:
        try:
            total_rows += pq.read_metadata(f).num_rows
        except Exception:
            pass
    return len(files), total_rows, len(set(str(Path(f).parent) for f in files))


@st.cache_data(show_spinner="Finding unique values …", max_entries=32)
def unique_values(folder_key: str, column: str) -> pd.DataFrame:
    folders = folder_key.split("|")
    files   = collect_files(folders)
    counter: dict = {}

    for f in files:
        try:
            schema = pq.read_schema(f)
            if column not in schema.names:
                continue
            tbl = pq.read_table(f, columns=[column])
            vc  = tbl.column(column).value_counts()
            for item in vc:
                val   = item["values"].as_py()
                val   = str(val) if val is not None else "(null)"
                counter[val] = counter.get(val, 0) + item["counts"].as_py()
        except Exception:
            continue

    if not counter:
        return pd.DataFrame(columns=["value", "count", "% of rows"])

    df    = pd.DataFrame(list(counter.items()), columns=["value", "count"])
    df    = df.sort_values("count", ascending=False).reset_index(drop=True)
    total = df["count"].sum()
    df["% of rows"] = (df["count"] / total * 100).round(2)
    return df  # return all — caller slices with .head(n)


@st.cache_data(show_spinner="Loading preview …", max_entries=16)
def load_preview(
    folder_key: str,
    columns: list[str],
    filters: dict,
    n_rows: int = 500,
    add_folder_col: bool = True,
) -> pd.DataFrame:
    folders   = folder_key.split("|")
    files     = collect_files(folders)
    frames    = []
    collected = 0

    for f in files:
        if collected >= n_rows:
            break
        try:
            schema      = pq.read_schema(f)
            avail_cols  = [c for c in columns if c in schema.names]
            if not avail_cols:
                continue

            tbl = pq.read_table(f, columns=avail_cols)

            # apply filters
            mask = None
            for col, vals in filters.items():
                if not vals or col not in tbl.schema.names:
                    continue
                col_arr  = tbl.column(col).cast(pa.string())
                sub_mask = None
                for v in vals:
                    eq       = pc.equal(col_arr, pa.scalar(v, pa.string()))
                    sub_mask = eq if sub_mask is None else pc.or_(sub_mask, eq)
                mask = sub_mask if mask is None else pc.and_(mask, sub_mask)

            if mask is not None:
                tbl = tbl.filter(mask)

            need  = n_rows - collected
            chunk = tbl.slice(0, need).to_pandas()

            if add_folder_col:
                chunk.insert(0, "_folder", f.parent.name)

            frames.append(chunk)
            collected += len(chunk)
        except Exception:
            continue

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def export_csv(folder_key: str, columns: list[str], filters: dict) -> bytes:
    folders = folder_key.split("|")
    files   = collect_files(folders)
    frames  = []

    for f in files:
        try:
            schema     = pq.read_schema(f)
            avail_cols = [c for c in columns if c in schema.names]
            if not avail_cols:
                continue

            tbl  = pq.read_table(f, columns=avail_cols)
            mask = None
            for col, vals in filters.items():
                if not vals or col not in tbl.schema.names:
                    continue
                col_arr  = tbl.column(col).cast(pa.string())
                sub_mask = None
                for v in vals:
                    eq       = pc.equal(col_arr, pa.scalar(v, pa.string()))
                    sub_mask = eq if sub_mask is None else pc.or_(sub_mask, eq)
                mask = sub_mask if mask is None else pc.and_(mask, sub_mask)

            if mask is not None:
                tbl = tbl.filter(mask)

            chunk = tbl.to_pandas()
            chunk.insert(0, "_folder", f.parent.name)
            frames.append(chunk)
        except Exception:
            continue

    if not frames:
        return b""
    return pd.concat(frames, ignore_index=True).to_csv(index=False).encode()


# ─────────────────────────────────────────────
# Sidebar — multi-folder manager
# ─────────────────────────────────────────────

with st.sidebar:
    st.title("🗂️ Parquet Explorer")
    st.markdown("---")
    st.markdown("**📁 Parquet Folders**")
    st.caption("Add one or more folders. Files with the same column names are merged automatically.")

    # render one text_input per folder slot
    for i in range(len(st.session_state.folders)):
        st.session_state.folders[i] = st.text_input(
            f"Folder {i+1}",
            value=st.session_state.folders[i],
            placeholder="e.g. D:\\data\\parquet",
            key=f"folder_input_{i}",
        )

    c1, c2 = st.columns(2)
    with c1:
        if st.button("➕ Add folder"):
            st.session_state.folders.append("")
            st.rerun()
    with c2:
        if st.button("➖ Remove last") and len(st.session_state.folders) > 1:
            st.session_state.folders.pop()
            st.rerun()

    # validate each folder
    valid_folders = []
    for f in st.session_state.folders:
        f = f.strip()
        if not f:
            continue
        p = Path(f)
        if not p.is_dir():
            st.error(f"Not found: {f}")
        elif not list(p.glob("*.parquet")):
            st.warning(f"No parquet files: {f}")
        else:
            valid_folders.append(f)

    st.markdown("---")
    st.caption("How to use")
    st.caption("1. Add one or more Parquet folders")
    st.caption("2. Browse columns in the Columns tab")
    st.caption("3. Explore unique values per column")
    st.caption("4. Filter rows and export CSV")


# ─────────────────────────────────────────────
# Main area
# ─────────────────────────────────────────────

if not valid_folders:
    st.title("Welcome to Parquet Explorer")
    st.info("👈  Add at least one Parquet folder in the sidebar to get started.")
    st.markdown("""
    **Supports multiple folders!**
    - Add folders from different days, regions, or batches
    - Columns are merged across folders automatically
    - Unique value counts span all folders combined
    - The `_folder` column in previews shows which folder each row came from
    """)
    st.stop()


# build a stable cache key from sorted valid folders
folder_key = "|".join(sorted(valid_folders))

columns, col_types     = load_schema(folder_key)
n_files, n_rows, n_fol = count_stats(folder_key)

if not columns:
    st.error("Could not read any schema from the provided folders.")
    st.stop()

st.title("🗂️ Parquet Explorer")

m1, m2, m3, m4 = st.columns(4)
m1.metric("Folders",       f"{n_fol}")
m2.metric("Parquet files", f"{n_files:,}")
m3.metric("Total rows",    f"{n_rows:,}")
m4.metric("Columns",       f"{len(columns)}")

# show which folders are loaded
with st.expander(f"📂 {n_fol} folder(s) loaded", expanded=False):
    for f in valid_folders:
        fpath  = Path(f)
        ffiles = list(fpath.glob("*.parquet"))
        st.markdown(f"- **{fpath.name}** — `{f}` &nbsp;&nbsp; ({len(ffiles):,} files)")

st.markdown("---")

tab1, tab2, tab3 = st.tabs(["📋 Columns", "🔍 Unique Values", "🎯 Filter & Export"])


# ══════════════════════════════
# TAB 1 — Column browser
# ══════════════════════════════
with tab1:
    st.subheader("All Columns")
    st.caption("Union of columns across all loaded folders.")

    search  = st.text_input("Search columns", placeholder="type to filter…")
    cols_df = pd.DataFrame({
        "Column":    columns,
        "Data type": [col_types.get(c, "") for c in columns],
    })
    if search:
        cols_df = cols_df[cols_df["Column"].str.contains(search, case=False)]

    st.dataframe(cols_df, use_container_width=True, height=520, hide_index=True)


# ══════════════════════════════
# TAB 2 — Unique values
# ══════════════════════════════
with tab2:
    st.subheader("Explore Unique Values")
    st.caption("Counts are combined across **all** loaded folders.")

    col_pick = st.selectbox("Choose a column", options=columns)

    # Step 1 — quick count (no data loaded, just cardinality)
    if st.button("1️⃣  Count unique values first", type="secondary"):
        with st.spinner("Counting …"):
            udf_all = unique_values(folder_key, col_pick)
        total_unique = len(udf_all)
        st.session_state[f"counted_{col_pick}"]   = total_unique
        st.session_state[f"udf_all_{col_pick}"]   = udf_all
        st.info(f"**{total_unique:,}** unique values found in `{col_pick}` — now choose how many to display below.")

    # Show count result if already done
    already_counted = st.session_state.get(f"counted_{col_pick}")
    udf_all_cached  = st.session_state.get(f"udf_all_{col_pick}")

    if already_counted is not None:
        st.markdown(f"Total unique values in **`{col_pick}`**: **{already_counted:,}**")

        # Step 2 — now the slider is meaningful
        slider_max = min(already_counted, 10_000)
        if slider_max <= 1:
            # only 1 unique value — no point in a slider
            show_top = already_counted
            st.info(f"Only {already_counted} unique value(s) — showing all.")
        else:
            show_top = st.slider(
                "How many to show (sorted by count)",
                min_value=1,
                max_value=slider_max,
                value=min(already_counted, 200),
                step=max(1, slider_max // 100),  # sensible step size
            )

        if st.button("2️⃣  Show values", type="primary"):
            udf = udf_all_cached.head(show_top)

            c1, c2 = st.columns([2, 1])
            with c1:
                st.dataframe(udf, use_container_width=True, height=450, hide_index=True)
            with c2:
                top10 = udf.head(10)
                st.bar_chart(top10.set_index("value")["count"])

            st.session_state.last_unique_col  = col_pick
            st.session_state.last_unique_vals = udf["value"].tolist()
            st.caption(f"Showing top {show_top:,} of {already_counted:,} unique values")


# ══════════════════════════════
# TAB 3 — Filter & export
# ══════════════════════════════
with tab3:
    st.subheader("Filter & Preview Data")
    st.caption("Rows are pulled from **all** loaded folders. A `_folder` column shows the source.")

    sel_cols = st.multiselect(
        "Columns to show",
        options=columns,
        default=columns[:min(8, len(columns))],
    )

    st.markdown("#### Filters")
    st.caption("Multiple values in one filter = OR.  Multiple filters = AND.")

    filters: dict = {}
    n_filter_rows = st.number_input("Number of filters", 0, 6, 1, step=1)

    for i in range(int(n_filter_rows)):
        fc1, fc2 = st.columns([1, 2])
        with fc1:
            fcol = st.selectbox(f"Filter {i+1} — column", options=columns, key=f"fcol_{i}")
        with fc2:
            suggestions = []
            if st.session_state.last_unique_col == fcol:
                suggestions = st.session_state.last_unique_vals[:500]

            fvals = st.multiselect(
                f"Filter {i+1} — values",
                options=suggestions,
                key=f"fval_{i}",
                placeholder="Select or type values…",
            )
            if fvals:
                filters[fcol] = fvals

    st.markdown("---")
    n_preview = st.slider("Preview rows", 50, 2000, 200, step=50)

    col_a, col_b = st.columns([1, 4])
    with col_a:
        run_preview = st.button("👀 Preview", type="primary")
    with col_b:
        run_export = st.button("💾 Export full CSV")

    if run_preview or run_export:
        if not sel_cols:
            st.warning("Pick at least one column to show.")
        else:
            if run_preview:
                with st.spinner("Loading …"):
                    df = load_preview(folder_key, sel_cols, filters, n_preview)
                if df.empty:
                    st.warning("No rows match your filters.")
                else:
                    st.caption(f"Showing {len(df):,} rows")
                    st.dataframe(df, use_container_width=True, height=450)

            if run_export:
                with st.spinner("Scanning all folders …"):
                    csv_bytes = export_csv(folder_key, sel_cols, filters)
                if not csv_bytes:
                    st.warning("No rows match your filters.")
                else:
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv_bytes,
                        file_name="export.csv",
                        mime="text/csv",
                    )
                    st.success("CSV ready — click Download above.")