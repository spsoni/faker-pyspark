import subprocess
import sys

from faker import Faker
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from faker_pyspark import Provider, PySparkProvider
from faker_pyspark.pyspark import fake_fields_list, get_spark


def test_provider_alias() -> None:
    # `faker -i faker_pyspark` looks the provider up by the name `Provider`.
    assert Provider is PySparkProvider


def test_get_spark(spark: SparkSession, fake: Faker) -> None:
    assert get_spark() is spark

    df = fake.pyspark_dataframe(spark=spark)
    assert isinstance(df, DataFrame)


def test_fake_fields_list(fake: Faker) -> None:
    fields = fake_fields_list(fake)
    assert len(fields) > 0
    assert not any(name.startswith("pyspark") for name in fields)

    scalar_fields = fake_fields_list(fake, allow_complex_types=False)
    assert 0 < len(scalar_fields) < len(fields)


def test_pyspark_dataframe(fake: Faker) -> None:
    df = fake.pyspark_dataframe()
    assert isinstance(df, DataFrame)
    assert df.count() == 4
    assert df.columns[0] == "uid"


def test_pyspark_dataframe_rows(fake: Faker) -> None:
    df = fake.pyspark_dataframe(rows=7, allow_complex_types=False)
    assert df.count() == 7


def test_pyspark_dataframe_with_schema(fake: Faker) -> None:
    schema = fake.pyspark_schema(allow_complex_types=False)
    df = fake.pyspark_dataframe(schema=schema, rows=3)
    assert df.schema == schema
    assert df.count() == 3


def test_pyspark_update_dataframe(fake: Faker) -> None:
    df = fake.pyspark_dataframe()
    df2 = fake.pyspark_update_dataframe(df, retain_columns=["uid"])
    assert isinstance(df2, DataFrame)
    assert df2.schema == df.schema
    assert 0 < df2.count() <= df.count()

    original_uids = {row.uid for row in df.collect()}
    assert {row.uid for row in df2.collect()} <= original_uids


def test_pyspark_update_dataframe_without_retained_columns(fake: Faker) -> None:
    df = fake.pyspark_dataframe()
    df2 = fake.pyspark_update_dataframe(df)
    assert df2.schema == df.schema


def test_pyspark_column_names(fake: Faker) -> None:
    names = fake.pyspark_column_names()
    assert 3 <= len(names) <= 10

    names = fake.pyspark_column_names(min_columns=5, max_columns=10)
    assert 5 <= len(names) <= 10


def test_pyspark_data_dict_using_schema(fake: Faker) -> None:
    schema = fake.pyspark_schema()
    data = fake.pyspark_data_dict_using_schema(schema=schema)
    assert list(data) == schema.fieldNames()


def test_pyspark_data_dict_using_schema_without_metadata(fake: Faker) -> None:
    # Without `fake_type` metadata the column name itself is the Faker method.
    schema = StructType.fromDDL("first_name string, last_name string")
    data = fake.pyspark_data_dict_using_schema(schema=schema)
    assert list(data) == ["first_name", "last_name"]
    assert all(isinstance(value, str) for value in data.values())


def test_pyspark_data_dict(fake: Faker) -> None:
    data = fake.pyspark_data_dict(min_columns=5, max_columns=10)
    assert 1 <= len(data) <= 10

    data = fake.pyspark_data_dict(uid_column_name="id")
    assert "id" in data


def test_pyspark_schema(spark: SparkSession, fake: Faker) -> None:
    for schema in (fake.pyspark_schema(), fake.pyspark_schema(spark=spark)):
        assert isinstance(schema, StructType)
        assert 2 <= len(schema.fields) <= 11
        assert schema.fields[0].name == "uid"
        assert schema.fields[0].metadata["fake_type"] == "uuid4"
        for field in schema.fields[1:]:
            assert field.metadata["fake_type"] == field.name.rsplit("__", 1)[0]


def test_pyspark_schema_without_uid(fake: Faker) -> None:
    schema = fake.pyspark_schema(uid_column_name=None)
    assert "uid" not in schema.fieldNames()


def test_faker_cli() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "faker", "pyspark_column_names", "-i", "faker_pyspark"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip().startswith("[")
