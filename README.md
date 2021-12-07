# queuery_client_python

Queuery Redshift HTTP API Client for Python

## Installation

`pip install queuery-client`

## Usage

- (a) naive iteration

```python
from queuery_client import QueueryClient

client = QueueryClient(endpoint="https://queuery.example.com")
response = client.run("select column_a, column_b from the_great_table")

# (a) iterate `response` naively
for elems in response:
    print(response)

# (b) invoke `read()` to fetch all records
print(response.read())

# (c) invoke `read()` with `use_pandas=True` (returns `pandas.DataFrame`)
print(response.read(use_pandas=True))
```
