import polars as pl
import glob
import os
from tqdm import tqdm
from urllib.parse import unquote

# ─── CONFIGURATION ───
INPUT_DIR = r"D:\VETO Logs\01"
OUTPUT_DIR = r"D:\VETO Logs\parquet_output"
BATCH_SIZE = 5000  # Number of .gz files to merge into one Parquet file
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Fields in your sample that need URL decoding
URL_COLS = ["UA", "state", "reqPath", "queryStr"]

def process_batch(file_list, batch_index):
    # 1. Load all .gz files in the batch simultaneously
    # Polars handles decompression and multi-threading automatically
    df = pl.read_ndjson(file_list)

    # 2. Cleanup: Replace Akamai null sentinels ("-", "^") with actual Nulls
    # We apply this to all string columns
    df = df.with_columns(
        pl.col(pl.Utf8).replace({"-": None, "^": None})
    )

    # 3. Cleanup: Decode URL-encoded strings (like %20 in 'state')
    df = df.with_columns([
        pl.col(col).map_elements(lambda x: unquote(x) if x else x, return_dtype=pl.Utf8)
        for col in URL_COLS if col in df.columns
    ])

    # 4. Correct Data Types: Cast numeric fields to save space
    # Casting reqTimeSec (string float) to float64, then to Datetime
    if "reqTimeSec" in df.columns:
        df = df.with_columns([
            pl.col("reqTimeSec").cast(pl.Float64).alias("reqTimeFloat")
        ]).with_columns([
            pl.from_epoch("reqTimeFloat", time_unit="s").alias("datetime")
        ]).drop("reqTimeFloat")

    # Cast other obvious metrics to numbers
    numeric_metrics = ["bytes", "objSize", "throughput", "statusCode", "asn", "totalBytes"]
    df = df.with_columns([
        pl.col(col).cast(pl.Float64, strict=False) 
        for col in numeric_metrics if col in df.columns
    ])

    # 5. Write to Parquet with high compression (Snappy)
    output_path = os.path.join(OUTPUT_DIR, f"akamai_batch_{batch_index:05d}.parquet")
    df.write_parquet(output_path, compression="snappy")

def run_conversion():
    files = sorted(glob.glob(os.path.join(INPUT_DIR, "*.gz")))
    if not files:
        print("No files found!")
        return

    # Split files into batches to avoid hitting RAM limits or "too many open files"
    file_batches = [files[i:i + BATCH_SIZE] for i in range(0, len(files), BATCH_SIZE)]
    
    for i, batch in enumerate(tqdm(file_batches, desc="Converting Batches")):
        try:
            process_batch(batch, i)
        except Exception as e:
            print(f"Error processing batch {i}: {e}")

if __name__ == "__main__":
    run_conversion()