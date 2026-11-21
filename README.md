# PySpark provider for Faker

[![PyPI](https://img.shields.io/pypi/v/faker-pyspark?color=blue)](https://pypi.org/project/faker-pyspark/)
[![Python](https://img.shields.io/pypi/pyversions/faker-pyspark)](https://pypi.org/project/faker-pyspark/)
[![CI](https://github.com/spsoni/faker-pyspark/actions/workflows/python-package.yml/badge.svg)](https://github.com/spsoni/faker-pyspark/actions/workflows/python-package.yml)
[![CodeQL](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml/badge.svg)](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/spsoni/faker-pyspark/blob/main/LICENSE)

Give it a Spark schema, get a DataFrame of realistic, reproducible fake data.

```python
from faker import Faker
from faker_pyspark import PySparkProvider

fake = Faker()
fake.add_provider(PySparkProvider)

df = fake.pyspark_dataframe(
    "id long, first_name string, email string, age int, unit_price decimal(10,2), "
    "signup date, address struct<city:string,postcode:string>, tags array<string>",
    rows=1000,
)
```

The data is fake: it looks real, but it is random. Do not use it as real data.

## Installation

Requires Python 3.11+ and a Java runtime for Spark.

If your environment already has PySpark (Databricks, EMR, Glue, or your own install), the
plain package is enough:

```bash
pip install faker-pyspark
```

Otherwise pull PySpark in as well:

```bash
pip install 'faker-pyspark[pyspark]'
```

To generate [large DataFrames](#large-dataframes) on the executors, Spark also needs pandas
and pyarrow. Databricks and most managed runtimes ship them; elsewhere:

```bash
pip install 'faker-pyspark[distributed]'
```

## How columns are filled

For each column, the first of these that applies wins:

1. **An override** you pass for that column.
2. **A `faker` entry in the field's metadata**, so a schema can carry its own instructions.
3. **The column's name.** `email`, `customer_email`, `billing_city`, `fname`, `zip` and
   `uuid` get matching text; `age`, `unit_price`, `latitude`, `rating` and `date_of_birth`
   get plausible ranges.
4. **The column's type.** Every Spark type is covered: strings (including `varchar(n)` and
   `char(n)`), booleans, all integer sizes, float, double, decimal, date, timestamp,
   timestamp_ntz, binary, day-time intervals, and arrays, maps and structs nested to any
   depth.

A type with no Python value Spark accepts (such as year-month intervals) raises
`UnsupportedTypeError` naming the column, and an override for that column resolves it.

## Overrides

```python
df = fake.pyspark_dataframe(
    "status string, quantity int, sku string, note string, address struct<city:string>",
    rows=500,
    overrides={
        "status": ["new", "paid", "shipped"],              # choose from a list
        "quantity": ("pyint", {"min_value": 1, "max_value": 12}),  # Faker method + arguments
        "sku": "ean13",                                    # Faker method by name
        "note": lambda f: f"ref {f.bothify('??-####')}",   # any function of the Faker
        "address.city": ["Melbourne", "Sydney"],           # nested field, dotted path
    },
)
```

Array elements are addressed with `[]`, as in `"tags[]"` or `"people[].role"`. A misspelt
column name or Faker method is an error, not a silent no-op.

The same instructions can live in the schema:

```python
from pyspark.sql.types import StringType, StructField, StructType

schema = StructType([
    StructField("country", StringType(), metadata={"faker": "country_code"}),
    StructField("tier", StringType(), metadata={"faker": {"choices": ["free", "pro"]}}),
    StructField("pin", StringType(), metadata={"faker": {"method": "numerify", "kwargs": {"text": "####"}}}),
])
```

## Reproducible data

All randomness comes from your Faker instance, so seeding it fixes the output:

```python
fake.seed_instance(42)
first = fake.pyspark_dataframe("email string, age int", rows=100)

fake.seed_instance(42)
second = fake.pyspark_dataframe("email string, age int", rows=100)
# first and second hold identical rows
```

Dates and timestamps are drawn from a fixed range (1970 to 2029) rather than from "now", so
a seeded run gives the same values tomorrow as it does today. Timestamps are in UTC, and a
time without a zone that an override puts in a `timestamp` column is taken as UTC too, so
the data does not depend on the time zone of the machine that made it.

## Large DataFrames

Small DataFrames are built on the driver. Above 100,000 rows the rows are generated on the
executors instead, in parallel, so the size is limited by the cluster rather than by the
driver's memory:

```python
df = fake.pyspark_dataframe("id long, email string, signup date", rows=50_000_000)
df.write.parquet("/tmp/customers")
```

The data is the same either way: a row depends only on the seed and its position, not on
where it was generated or how the rows were split up. To choose yourself:

```python
fake.pyspark_dataframe(schema, rows=5_000, distributed=True, partitions=8)  # on the executors
fake.pyspark_dataframe(schema, rows=500_000, distributed=False)             # on the driver
```

What the executors need:

- `faker` and `faker-pyspark` installed, along with pandas and pyarrow.
- Any module of your own that holds a provider or override function the data uses.
  Providers and functions defined in a notebook or script travel with the job.
- The same Python minor version as the driver, as for any Python code Spark runs.

Like any Spark DataFrame, the result is computed each time it is used. Seeded data comes
out the same every time; cache or write it if generating it twice is too slow.

## Spark Connect

Everything works over [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html),
including serverless runtimes built on it. Pass the session, or let the provider find the
active one:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
df = fake.pyspark_dataframe("id long, email string", rows=1_000_000, spark=spark)
```

Here the "executors" belong to the server, so `faker-pyspark` has to be installed there as
well as on your client. The test suite runs over Connect with PySpark 4.

## Other options

```python
fake.pyspark_dataframe(
    schema,              # StructType, DDL string, or omit for a random schema
    rows=10,
    spark=None,          # defaults to the active session, or starts one
    null_ratio=0.1,      # share of nulls in nullable columns
    array_size=(1, 3),   # inclusive length range for arrays and maps
    locale="ja_JP",      # defaults to your Faker's locale
    distributed=None,    # True or False to choose where rows are generated; see above
    partitions=None,     # partitions to generate in, when on the executors
)

fake.pyspark_rows(schema, rows=10)          # the same data as a list of dictionaries
fake.pyspark_schema(columns=(3, 8))         # a random schema of realistically named columns
fake.pyspark_schema(columns=5, nested=True)  # ...allowing structs, arrays and maps
```

Providers you have added to your Faker are available to overrides by method name.

## Command line

```bash
faker pyspark_schema -i faker_pyspark
```

## Upgrading from 0.9

Version 0.10 replaced the API. `pyspark_dataframe` and `pyspark_schema` keep their names
with new arguments; `pyspark_update_dataframe`, `pyspark_column_names`, `pyspark_data_dict`
and `pyspark_data_dict_using_schema` are gone, as is the `fake_type` metadata key (use
`faker`). `pyspark_rows` replaces the two `data_dict` methods.

## Contributing

See [CONTRIBUTING.md](https://github.com/spsoni/faker-pyspark/blob/main/CONTRIBUTING.md)
for the development setup, and
[CHANGELOG.md](https://github.com/spsoni/faker-pyspark/blob/main/CHANGELOG.md) for what
changed between releases.
