# import gzip

# # This opens the file and decompresses it into text automatically
# with gzip.open('ak-199952-1775615225-577717-ds.gz', 'rt') as f:
#     content = f.read()
#     print(content)
import gzip
import json

# Use a loop to handle every single row without missing any data
with gzip.open('ak-199952-1775615225-577717-ds.gz', 'rt') as f:
    for line in f:
        # data = json.loads(line)  # Use this to turn it into a Python dictionary
        print(line.strip())        # This shows you the raw text of every record