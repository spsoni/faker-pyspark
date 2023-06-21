
# PySpark provider for Faker


## Acknowlegements


`faker_pyspark` is a provider for the `Faker` Python package, and a fork of https://github.com/kennethwsmith/faker_vehicle.  I would like to thank the maintainer of that repository, since I used the structure to create this one.


## Description

`faker_pyspark` provides PySpark based fake data for testing purposes.  The definition of "fake" in this context really means "random," as the data may look real.  However, I make no claims about accuracy, so do not use this as real data!


## Installation

Install with pip:

``` bash
pip install faker_pyspark

```

Add as a provider to your Faker instance:

``` python

from faker import Faker
from faker_pyspark import PySparkProvider
fake.add_provider(PySparkProvider)

```

If you already use faker, you probably know the conventional use is:

```python
fake = Faker()

```


### PySpark DataFrame and Schema (StructType)

``` python
>>> df = fake.create_dataframe()

>>> schema = fake.create_schema()

```
