import gzip
import json

with gzip.open('ak-000003-1775056833-221784-ds.gz', 'rt') as f:
    for i, line in enumerate(f):
        data = json.loads(line)
        print(data)
        if i == 5:  # Just read the first 10 lines for demonstration
            break