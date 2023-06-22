
# PySpark provider for Faker

[![Python package](https://github.com/spsoni/faker_pyspark/actions/workflows/python-package.yml/badge.svg)](https://github.com/spsoni/faker_pyspark/actions/workflows/python-package.yml)
[![CodeQL](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml/badge.svg)](https://github.com/spsoni/faker-pyspark/actions/workflows/codeql.yml)

`faker-pyspark` is a PySpark DataFrame and Schema (StructType) provider for the `Faker` Python package.


## Description

`faker-pyspark` provides PySpark based fake data for testing purposes.  The definition of "fake" in this context really means "random," as the data may look real.  However, I make no claims about accuracy, so do not use this as real data!


## Installation

Install with pip:

``` bash
pip install faker-pyspark

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
