import pyarrow.parquet as pq
import pyarrow as pa
from pathlib import Path

input_folder = Path(r"D:\VETO Logs\16 P")
output_file = r"D:\VETO Logs\16Parquet\merged.parquet"

writer = None

for i, file in enumerate(input_folder.glob("*.parquet")):
    print(f"Processing {i+1}: {file}")

    table = pq.read_table(file)

    if writer is None:
        writer = pq.ParquetWriter(output_file, table.schema)

    writer.write_table(table)

if writer:
    writer.close()

print("Merged safely!")