# PySpark provider for Faker

[![PyPI](https://img.shields.io/pypi/v/faker-pyspark?color=blue)](https://pypi.org/project/faker-pyspark/)
[![Python](https://img.shields.io/pypi/pyversions/faker-pyspark)](https://pypi.org/project/faker-pyspark/)
[![CI](https://github.com/spsoni/faker-pyspark/actions/workflows/python-package.yml/badge.svg)](https://github.com/spsoni/faker-pyspark/actions/workflows/python-package.yml)
[![CodeQL](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml/badge.svg)](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](https://github.com/spsoni/faker-pyspark/blob/main/LICENSE)

`faker-pyspark` is a PySpark DataFrame and Schema (StructType) provider for the `Faker` Python package.


## Description

`faker-pyspark` provides PySpark based fake data for testing purposes.  The definition of "fake" in this context really means "random," as the data may look real.  However, I make no claims about accuracy, so do not use this as real data!


## Installation

Requires Python 3.11+ and a Java runtime for Spark.

Install with pip. If your environment already has PySpark (Databricks, EMR, Glue, or your
own install), the plain package is enough:

``` bash
pip install faker-pyspark
```

Otherwise pull PySpark in as well:

``` bash
pip install 'faker-pyspark[pyspark]'
```

Add as a provider to your Faker instance:

``` python

from faker import Faker
from faker_pyspark import PySparkProvider
fake = Faker()
fake.add_provider(PySparkProvider)

```

### PySpark DataFrame, Schema and more

``` python
>>> df           = fake.pyspark_dataframe()
>>> schema       = fake.pyspark_schema()
>>> df_updated   = fake.pyspark_update_dataframe(df)
>>> column_names = fake.pyspark_column_names()
>>> data         = fake.pyspark_data_dict_using_schema(schema)
>>> data         = fake.pyspark_data_dict()

```

Pass your own `SparkSession` with `spark=...`; without one, the provider uses
`SparkSession.builder.getOrCreate()`.

### CLI `faker`

```bash
$ faker pyspark_schema       -i faker_pyspark
$ faker pyspark_dataframe    -i faker_pyspark
$ faker pyspark_column_names -i faker_pyspark
$ faker pyspark_data_dict    -i faker_pyspark
```

## Contributing

See [CONTRIBUTING.md](https://github.com/spsoni/faker-pyspark/blob/main/CONTRIBUTING.md)
for the development setup, and
[CHANGELOG.md](https://github.com/spsoni/faker-pyspark/blob/main/CHANGELOG.md) for what
changed between releases.
