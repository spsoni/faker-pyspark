import pytest

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


from faker_pyspark import PySparkProvider
from faker_pyspark.pyspark import get_spark, fake_fields_list


fake = Faker()
fake.add_provider(PySparkProvider)


def test_get_spark():
    spark = get_spark()
    assert isinstance(spark, SparkSession)

    df = fake.pyspark_dataframe(spark=spark)
    assert isinstance(df, DataFrame)


def test_fake_fields_list():
    fields = fake_fields_list(fake)
    assert len(fields) > 0


def test_pyspark_dataframe():
    df = fake.pyspark_dataframe()
    assert isinstance(df, DataFrame)


def test_pyspark_update_dataframe():
    df = fake.pyspark_dataframe()
    df2 = fake.pyspark_update_dataframe(df, retain_columns=['uid'])
    assert isinstance(df2, DataFrame)
    assert df2.count() <= df.count()


def test_pyspark_column_names():
    names = fake.pyspark_column_names()
    assert len(names) >= 3

    names = fake.pyspark_column_names(min_columns=5, max_columns=10)
    assert len(names) >= 5
    assert len(names) <= 10


def test_pyspark_data_dict_using_schema():
    schema = fake.pyspark_schema()
    data = fake.pyspark_data_dict_using_schema(schema=schema)
    assert len(data) == len(schema.fields)

    schema = fake.pyspark_schema()
    data = fake.pyspark_data_dict_using_schema(schema=schema)
    assert len(data) == len(schema.fields)


def test_pyspark_data_dict():
    data = fake.pyspark_data_dict(min_columns=5, max_columns=10)
    assert len(data) >= 5
    assert len(data) <= 10


def test_pyspark_schema():
    schema = fake.pyspark_schema()
    assert isinstance(schema, StructType)
    assert len(schema.fields) >= 3
    assert len(schema.fields) <= 10

    spark = get_spark()
    assert isinstance(spark, SparkSession)
    schema = fake.pyspark_schema(spark=spark)
    assert isinstance(schema, StructType)
    assert len(schema.fields) >= 3
    assert len(schema.fields) <= 10
