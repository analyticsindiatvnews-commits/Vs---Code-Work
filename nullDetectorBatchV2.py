import gzip
import json
import csv
from pathlib import Path
from multiprocessing import Pool, cpu_count
from collections import defaultdict
from tqdm import tqdm

FOLDER_PATH = r'D:\VETO Logs\08'
PLACEHOLDERS = {"-", "^"}

def process_single_file(file_path):
    """Count presence and filled values per column in one gzip JSONL file."""
    local_total_rows = 0
    local_present_counts = defaultdict(int)
    local_filled_counts = defaultdict(int)
    local_all_columns = set()

    try:
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                data = json.loads(line)
                local_total_rows += 1

                for key, value in data.items():
                    local_all_columns.add(key)
                    local_present_counts[key] += 1

                    if str(value) not in PLACEHOLDERS:
                        local_filled_counts[key] += 1

    except Exception:
        # Skip corrupted/unreadable files
        pass

    return (
        local_total_rows,
        dict(local_present_counts),
        dict(local_filled_counts),
        local_all_columns
    )

if __name__ == '__main__':
    print(f"Searching for .gz files in: {FOLDER_PATH}...")
    files = list(Path(FOLDER_PATH).glob('*.gz'))
    total_files = len(files)

    if total_files == 0:
        print("Error: No .gz files found in that directory. Check the path.")
        exit()

    num_cpus = cpu_count()
    total_rows = 0
    global_present_counts = defaultdict(int)
    global_filled_counts = defaultdict(int)
    all_columns = set()

    print(f"Detected {total_files} files. Starting analysis on {num_cpus} cores...")

    with Pool(num_cpus) as pool:
        results = pool.imap_unordered(process_single_file, files, chunksize=100)

        for file_rows, file_present, file_filled, file_columns in tqdm(
            results, total=total_files, desc="Scanning Logs", unit="file"
        ):
            total_rows += file_rows
            all_columns.update(file_columns)

            for key, count in file_present.items():
                global_present_counts[key] += count

            for key, count in file_filled.items():
                global_filled_counts[key] += count

    if total_rows == 0:
        print("No valid log rows found.")
        exit()

    completely_blank = []
    columns_with_data = []

    for col in sorted(all_columns):
        present_rows = global_present_counts.get(col, 0)
        filled_rows = global_filled_counts.get(col, 0)

        pct_all_rows = (filled_rows / total_rows * 100) if total_rows else 0
        pct_when_present = (filled_rows / present_rows * 100) if present_rows else 0

        if filled_rows == 0:
            completely_blank.append(col)
        else:
            columns_with_data.append((col, filled_rows, present_rows, pct_all_rows, pct_when_present))

    total_col_count = len(all_columns)
    blank_col_count = len(completely_blank)
    used_col_count = len(columns_with_data)

    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE")
    print(f"Total Valid Rows Scanned:         {total_rows}")
    print(f"Total Unique Columns Found:       {total_col_count}")
    print(f"Columns With Data:                {used_col_count}")
    print(f"Completely Blank Columns:         {blank_col_count}")
    print(f"Usage Rate:                       {(used_col_count / total_col_count * 100):.1f}%")
    print("=" * 80)

    print("\nList of columns that are 100% blank/suppressed:")
    for col in completely_blank:
        print(f" - {col}")

    print("\nList of columns with data:")
    print("   (Filled % of all rows | Filled % when present | filled/present/total)")
    for col, filled_rows, present_rows, pct_all_rows, pct_when_present in sorted(
        columns_with_data, key=lambda x: x[3], reverse=True
    ):
        print(
            f" - {col}: "
            f"{pct_all_rows:6.2f}% of all rows | "
            f"{pct_when_present:6.2f}% when present | "
            f"({filled_rows}/{present_rows}/{total_rows})"
        )

    # -------------- CSV EXPORT OPTION ----------------

    choice = input("\nDo you want to export results to CSV? (y/n): ").strip().lower()

    if choice == 'y':
        file_name = input("Enter CSV file name (without extension): ").strip()

        if not file_name:
            file_name = "log_analysis"

        output_path = f"{file_name}.csv"

        try:
            with open(output_path, mode='w', newline='', encoding='utf-8') as csv_file:
                writer = csv.writer(csv_file)

                # Header
                writer.writerow([
                    "Column Name",
                    "Status",
                    "Filled Rows",
                    "Present Rows",
                    "Total Rows",
                    "% Filled (All Rows)",
                    "% Filled (When Present)"
                ])

                # Write blank columns
                for col in completely_blank:
                    writer.writerow([
                        col,
                        "BLANK",
                        0,
                        global_present_counts.get(col, 0),
                        total_rows,
                        0.0,
                        0.0
                    ])

                # Write columns with data
                for col, filled_rows, present_rows, pct_all_rows, pct_when_present in columns_with_data:
                    writer.writerow([
                        col,
                        "HAS_DATA",
                        filled_rows,
                        present_rows,
                        total_rows,
                        round(pct_all_rows, 2),
                        round(pct_when_present, 2)
                    ])

            print(f"\n✅ CSV file saved as: {output_path}")

        except Exception as e:
            print(f"\n❌ Failed to write CSV: {e}")

    else:
        print("CSV export skipped.")