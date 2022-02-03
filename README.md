# queuery_client_python

Queuery Redshift HTTP API Client for Python

## Installation

`pip install queuery-client`

## Usage

### Prerequisites

Set the following envronment variables to connect Queuery server:

- `QUEUERY_TOKEN`: Specify Queuery access token
- `QUEUERY_TOKEN_SECRET`:  Specify Queuery secret access token
- `QUEUERY_ENDPOINT`: Specify a Queuery endpoint URL via environment variables if you don't set the `endpoint` argument of `QueueryClient` in you code

### Basic Usage

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

### Type Cast

By default, `QueueryClient` returns all values as `str` regardless of their definitions on Redshift.
You can use the `enable_cast` option to automatically convert types of the returned values into appropreate ones based on their definitions.

```python
from queuery_client import QueueryClient

client = QueueryClient(
    endpoint="https://queuery.example.com",
    enable_cast=True,   # Cast types of the returned values automatically!
)

sql = "select 1, 1.0, 'hoge', true, date '2021-01-01', timestamp '2021-01-01', null"
response = client.run(sql)
response.read() # => [[1, 1.0, 'hoge', True, datetime.date(2021, 1, 1), datetime.datetime(2021, 1, 1, 0, 0), None]]
```
