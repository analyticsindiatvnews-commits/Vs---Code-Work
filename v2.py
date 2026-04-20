import gzip
import json
from pathlib import Path
from urllib.parse import unquote
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
from tqdm import tqdm

# ---------------- CONFIG DEFAULTS ----------------

DEFAULT_PLACEHOLDERS = {"-", "^"}
IST_TZ = ZoneInfo("Asia/Kolkata")


# ---------------- HELPER FUNCTIONS ----------------

def ask_yes_no(prompt, default="y"):
    default = default.lower().strip()
    while True:
        raw = input(f"{prompt} ({'Y/n' if default == 'y' else 'y/N'}): ").strip().lower()

        if not raw:
            return default == "y"

        if raw in ("y", "yes"):
            return True
        if raw in ("n", "no"):
            return False

        print("Please enter y or n.")


def ask_text(prompt, default=""):
    raw = input(f"{prompt}: ").strip()
    return raw if raw else default


def parse_column_list(raw_text):
    if not raw_text.strip():
        return []
    return [x.strip() for x in raw_text.split(",") if x.strip()]


def print_column_block(title, columns, max_show=200):
    columns = sorted(columns)
    print(f"\n{title} ({len(columns)}):")
    if not columns:
        print("  [none]")
        return

    for col in columns[:max_show]:
        print(f"  - {col}")

    if len(columns) > max_show:
        print(f"  ... and {len(columns) - max_show} more")


def safe_none_if_placeholder(value, placeholders, convert_placeholders):
    if not convert_placeholders:
        return value

    if value is None:
        return None

    if str(value) in placeholders:
        return None

    return value


def parse_epoch_seconds(value):
    if value is None:
        return None

    try:
        num = float(str(value).strip())
        return datetime.fromtimestamp(num, tz=timezone.utc)
    except Exception:
        return None


def auto_decode_url_if_needed(value):
    """
    URL-decodes strings that appear encoded, such as %20.
    Leaves non-strings and normal strings unchanged.
    """
    if value is None:
        return value

    if not isinstance(value, str):
        return value

    if "%" not in value and "+" not in value:
        return value

    try:
        return unquote(value)
    except Exception:
        return value


def build_output_record(
    record,
    kept_columns,
    placeholders,
    convert_placeholders,
    convert_reqtimesec_to_ist=False,
    decode_url_text=False
):
    output = {}

    for col in kept_columns:
        value = record.get(col, None)

        # Step 1: placeholder cleanup
        value = safe_none_if_placeholder(value, placeholders, convert_placeholders)

        # Step 2: reqTimeSec in-place conversion to IST
        if convert_reqtimesec_to_ist and col == "reqTimeSec":
            dt_utc = parse_epoch_seconds(value)
            value = dt_utc.astimezone(IST_TZ).replace(tzinfo=None) if dt_utc else None

        # Step 3: URL decode text columns if enabled
        if decode_url_text:
            value = auto_decode_url_if_needed(value)

        output[col] = value

    return output


def read_profile_csv(csv_path):
    df = pd.read_csv(csv_path)

    required_cols = {
        "Column Name",
        "Status",
        "Filled Rows",
        "Present Rows",
        "Total Rows",
        "% Filled (All Rows)",
        "% Filled (When Present)",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(
            f"CSV is missing required columns: {sorted(missing)}\n"
            f"Make sure you are using the CSV produced by your analyzer script."
        )

    return df


def collect_columns_from_profile(df):
    return set(df["Column Name"].astype(str).tolist())


def collect_blank_columns_from_profile(df):
    status_series = df["Status"].astype(str).str.upper().str.strip()
    col_series = df["Column Name"].astype(str)
    return set(col_series[status_series == "BLANK"].tolist())


def convert_single_gz_to_parquet(
    gz_file,
    output_folder,
    kept_columns,
    placeholders,
    convert_placeholders,
    convert_reqtimesec_to_ist=False,
    decode_url_text=False
):
    rows = []

    try:
        with gzip.open(gz_file, "rt", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                except Exception:
                    continue

                if not isinstance(record, dict):
                    continue

                output_record = build_output_record(
                    record=record,
                    kept_columns=kept_columns,
                    placeholders=placeholders,
                    convert_placeholders=convert_placeholders,
                    convert_reqtimesec_to_ist=convert_reqtimesec_to_ist,
                    decode_url_text=decode_url_text
                )
                rows.append(output_record)

        if not rows:
            return False, f"{gz_file.name}: no valid rows"

        df = pd.DataFrame(rows)

        output_file = output_folder / f"{gz_file.stem}.parquet"
        df.to_parquet(output_file, index=False, engine="pyarrow")

        return True, f"{gz_file.name} -> {output_file.name}"

    except Exception as e:
        return False, f"{gz_file.name}: {e}"


# ---------------- MAIN MENU FLOW ----------------

if __name__ == "__main__":
    print("=" * 80)
    print("GZ TO PARQUET MENU CONVERTER")
    print("=" * 80)

    profile_csv_path = ask_text("Enter analyzer CSV path")
    profile_csv = Path(profile_csv_path)

    if not profile_csv.exists():
        print("CSV file not found.")
        raise SystemExit(1)

    try:
        profile_df = read_profile_csv(profile_csv)
    except Exception as e:
        print(f"Failed to read profile CSV: {e}")
        raise SystemExit(1)

    all_columns = collect_columns_from_profile(profile_df)
    blank_columns = collect_blank_columns_from_profile(profile_df)

    print(f"\nProfile CSV loaded successfully.")
    print(f"Total columns in profile: {len(all_columns)}")
    print(f"Columns marked BLANK:     {len(blank_columns)}")

    # Step 1: drop blank fields?
    drop_blank_fields = ask_yes_no("Should I drop 100% blank fields", default="y")

    # Step 2: placeholders to NULL?
    convert_placeholders = ask_yes_no("Should I convert '-' and '^' to NULL", default="y")

    dropped_columns = set()
    if drop_blank_fields:
        dropped_columns.update(blank_columns)

    remaining_columns = sorted(all_columns - dropped_columns)

    print_column_block("Automatically dropped columns", dropped_columns)
    print_column_block("Columns remaining after automatic rules", remaining_columns)

    # Step 3: manual remove
    if ask_yes_no("Do you want to manually remove more columns", default="n"):
        print_column_block("Remaining columns available for manual removal", remaining_columns)
        raw_remove = ask_text("Enter column names to remove, comma separated", default="")
        manual_remove = set(parse_column_list(raw_remove))

        invalid_remove = manual_remove - set(remaining_columns)
        valid_remove = manual_remove & set(remaining_columns)

        if invalid_remove:
            print_column_block("These columns were not found and were ignored", invalid_remove)

        dropped_columns.update(valid_remove)
        remaining_columns = sorted(all_columns - dropped_columns)

    # Step 4: explicit keep
    if ask_yes_no("Do you want to explicitly choose columns to keep from remaining columns", default="n"):
        print_column_block("Remaining columns available to keep", remaining_columns)
        raw_keep = ask_text("Enter column names to keep, comma separated", default="")
        manual_keep = set(parse_column_list(raw_keep))

        invalid_keep = manual_keep - set(remaining_columns)
        valid_keep = manual_keep & set(remaining_columns)

        if invalid_keep:
            print_column_block("These columns were not found and were ignored", invalid_keep)

        remaining_columns = sorted(valid_keep)
        dropped_columns = all_columns - set(remaining_columns)

    kept_columns = sorted(remaining_columns)

    print_column_block("Final dropped columns", dropped_columns)
    print_column_block("Final kept columns", kept_columns)

    if not kept_columns:
        print("No columns left to convert. Exiting.")
        raise SystemExit(1)

    # Step 5: ask explicitly about reqTimeSec conversion
    convert_reqtimesec_to_ist = False
    if "reqTimeSec" in kept_columns:
        print("\nDetected column: reqTimeSec")
        convert_reqtimesec_to_ist = ask_yes_no(
            "Convert reqTimeSec in place to IST",
            default="y"
        )

    # Step 6: ask explicitly about URL decoding
    decode_url_text = ask_yes_no(
        "Decode URL-encoded text in kept string columns (%20 -> space, etc.)",
        default="y"
    )

    # Final summary
    print("\n" + "=" * 80)
    print("FINAL CONVERSION PLAN")
    print("=" * 80)
    print(f"Drop 100% blank fields:           {'Yes' if drop_blank_fields else 'No'}")
    print(f"Convert '-' and '^' to NULL:      {'Yes' if convert_placeholders else 'No'}")
    print(f"Convert reqTimeSec to IST:        {'Yes' if convert_reqtimesec_to_ist else 'No'}")
    print(f"Decode URL-encoded text:          {'Yes' if decode_url_text else 'No'}")
    print(f"Total dropped columns:            {len(dropped_columns)}")
    print(f"Total kept columns:               {len(kept_columns)}")

    print_column_block("Columns that will be written to parquet", kept_columns)

    # Step 7: confirm conversion
    if not ask_yes_no("Proceed with GZ to Parquet conversion", default="y"):
        print("Conversion cancelled.")
        raise SystemExit(0)

    # Step 8: folders
    input_folder_path = ask_text("Enter input folder containing .gz files")
    output_folder_path = ask_text("Enter output folder for .parquet files")

    input_folder = Path(input_folder_path)
    output_folder = Path(output_folder_path)

    if not input_folder.exists():
        print("Input folder not found.")
        raise SystemExit(1)

    output_folder.mkdir(parents=True, exist_ok=True)

    gz_files = list(input_folder.glob("*.gz"))
    if not gz_files:
        print("No .gz files found in input folder.")
        raise SystemExit(1)

    print(f"\nFound {len(gz_files)} .gz files.")
    print("Starting conversion...")

    success_count = 0
    fail_count = 0
    failures = []

    for gz_file in tqdm(gz_files, desc="Converting", unit="file"):
        ok, msg = convert_single_gz_to_parquet(
            gz_file=gz_file,
            output_folder=output_folder,
            kept_columns=kept_columns,
            placeholders=DEFAULT_PLACEHOLDERS,
            convert_placeholders=convert_placeholders,
            convert_reqtimesec_to_ist=convert_reqtimesec_to_ist,
            decode_url_text=decode_url_text
        )

        if ok:
            success_count += 1
        else:
            fail_count += 1
            failures.append(msg)

    print("\n" + "=" * 80)
    print("CONVERSION COMPLETE")
    print("=" * 80)
    print(f"Successful files: {success_count}")
    print(f"Failed files:     {fail_count}")
    print(f"Output folder:    {output_folder}")

    if failures:
        print("\nSample failures:")
        for item in failures[:20]:
            print(f"  - {item}")