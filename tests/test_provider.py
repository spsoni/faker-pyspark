"""The provider against a real Spark session."""

import datetime
import decimal
import subprocess
import sys
from typing import Any

import pytest
from faker import Faker
from faker.providers import BaseProvider
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from faker_pyspark import GeneratorSpecError, Provider, PySparkProvider
from faker_pyspark._catalog import RANDOM_COLUMNS, RANDOM_NESTED_COLUMNS

from .conftest import make_faker

DDL = (
    "id long, email string, age int, price decimal(10,2), active boolean, "
    "signup date, seen timestamp, tags array<string>, attrs map<string,int>, "
    "address struct<city:string,postcode:string>"
)


def collect(df: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.collect()]


def test_provider_alias() -> None:
    # `faker -i faker_pyspark` looks the provider up by the name `Provider`.
    assert Provider is PySparkProvider


def test_dataframe_from_ddl(spark: SparkSession, fake: Faker) -> None:
    df = fake.pyspark_dataframe(DDL, rows=25)
    assert df.count() == 25
    assert df.schema.simpleString() == spark.createDataFrame([], DDL).schema.simpleString()

    for row in collect(df):
        assert "@" in row["email"]
        assert 0 <= row["age"] <= 100
        assert isinstance(row["price"], decimal.Decimal)
        assert isinstance(row["signup"], datetime.date)
        assert isinstance(row["seen"], datetime.datetime)
        assert set(row["address"]) == {"city", "postcode"}
        assert all(isinstance(value, int) for value in row["attrs"].values())


def test_dataframe_from_struct_type_keeps_the_schema(fake: Faker) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("note", T.VarcharType(10), metadata={"faker": "word"}),
        ]
    )
    df = fake.pyspark_dataframe(schema, rows=30)
    assert df.schema["id"].nullable is False
    # Spark stores varchar as string and keeps the declared type in metadata.
    assert df.schema["note"].dataType == T.StringType()
    assert df.schema["note"].metadata == {
        "faker": "word",
        "__CHAR_VARCHAR_TYPE_STRING": "varchar(10)",
    }
    assert all(len(row["note"]) <= 10 for row in collect(df))


def test_every_supported_type_round_trips_through_spark(fake: Faker) -> None:
    ddl = (
        "a string, b boolean, c tinyint, d smallint, e int, f bigint, g float, h double, "
        "i decimal(38,10), j date, k timestamp, l timestamp_ntz, m binary, "
        "n interval day to second, o array<array<int>>, p map<string,array<double>>, "
        "q struct<x:int,y:struct<z:date>>, r varchar(3), s char(2)"
    )
    df = fake.pyspark_dataframe(ddl, rows=40, null_ratio=0.2)
    rows = collect(df)
    assert len(rows) == 40
    assert any(row["a"] is None for row in rows) and any(row["a"] is not None for row in rows)
    assert all(row["s"] is None or len(row["s"]) == 2 for row in rows)


def test_ddl_must_describe_columns(fake: Faker) -> None:
    with pytest.raises(GeneratorSpecError, match="must describe columns"):
        fake.pyspark_dataframe("int")


def test_zero_rows(fake: Faker) -> None:
    df = fake.pyspark_dataframe("a int", rows=0)
    assert df.count() == 0 and df.columns == ["a"]


def test_negative_rows_rejected(fake: Faker) -> None:
    with pytest.raises(ValueError, match="rows"):
        fake.pyspark_dataframe("a int", rows=-1)


def test_same_seed_gives_identical_dataframes(spark: SparkSession) -> None:
    first = make_faker(seed=7).pyspark_dataframe(DDL, rows=20)
    second = make_faker(seed=7).pyspark_dataframe(DDL, rows=20)
    assert collect(first) == collect(second)
    assert collect(first) != collect(make_faker(seed=8).pyspark_dataframe(DDL, rows=20))


def test_class_level_seed_is_honoured(spark: SparkSession) -> None:
    def draw() -> list[dict[str, Any]]:
        Faker.seed(99)
        return collect(make_faker().pyspark_dataframe(DDL, rows=5))

    assert draw() == draw()


def test_two_calls_on_one_faker_differ(fake: Faker) -> None:
    assert fake.pyspark_rows(T.StructType.fromJson(_EMAIL), 5) != fake.pyspark_rows(
        T.StructType.fromJson(_EMAIL), 5
    )


_EMAIL = {
    "type": "struct",
    "fields": [{"name": "email", "type": "string", "nullable": True, "metadata": {}}],
}


def test_rows_match_the_dataframe_for_the_same_seed(spark: SparkSession) -> None:
    rows = make_faker(seed=3).pyspark_rows("email string, age int", rows=6)
    df = make_faker(seed=3).pyspark_dataframe("email string, age int", rows=6)
    assert rows == collect(df)


def test_rows_need_no_session_for_a_struct_type() -> None:
    schema = T.StructType([T.StructField("email", T.StringType())])
    rows = make_faker(seed=1).pyspark_rows(schema, rows=2)
    assert len(rows) == 2 and "@" in rows[0]["email"]


def test_overrides_and_bad_overrides(fake: Faker) -> None:
    df = fake.pyspark_dataframe(
        "status string, qty int",
        rows=30,
        overrides={"status": ["open", "closed"], "qty": ("pyint", {"max_value": 3})},
    )
    rows = collect(df)
    assert {row["status"] for row in rows} == {"open", "closed"}
    assert all(0 <= row["qty"] <= 3 for row in rows)

    with pytest.raises(GeneratorSpecError, match="not in the schema"):
        fake.pyspark_dataframe("status string", overrides={"state": ["x"]})


def test_random_schema_is_distinct_bounded_and_seeded(spark: SparkSession) -> None:
    faker = make_faker(seed=5)
    sizes = set()
    for _ in range(60):
        schema = faker.pyspark_schema(columns=(2, 4))
        names = schema.fieldNames()
        assert len(set(names)) == len(names)
        sizes.add(len(names))
    assert sizes == {2, 3, 4}

    assert make_faker(seed=5).pyspark_schema() == make_faker(seed=5).pyspark_schema()
    assert len(make_faker(seed=5).pyspark_schema(columns=6).fields) == 6

    flat = {name for name, _ in RANDOM_COLUMNS}
    assert set(make_faker(seed=1).pyspark_schema(columns=len(flat)).fieldNames()) == flat


def test_random_schema_can_nest(spark: SparkSession) -> None:
    everything = len(RANDOM_COLUMNS) + len(RANDOM_NESTED_COLUMNS)
    faker = make_faker(seed=2)
    schema = faker.pyspark_schema(columns=everything, nested=True)
    assert {"address", "tags", "attributes", "contacts"} <= set(schema.fieldNames())
    # Every column a random schema can contain is one the engine and Spark both accept.
    assert faker.pyspark_dataframe(schema, rows=5).count() == 5


@pytest.mark.parametrize("columns", [0, (3, 2), 1000])
def test_random_schema_rejects_bad_sizes(columns: Any) -> None:
    with pytest.raises(GeneratorSpecError, match="columns"):
        make_faker(seed=1).pyspark_schema(columns=columns)


def test_dataframe_without_a_schema_uses_a_random_one(fake: Faker) -> None:
    df = fake.pyspark_dataframe(rows=4)
    assert df.count() == 4 and 3 <= len(df.columns) <= 8


def test_locale_follows_the_faker_and_can_be_overridden(spark: SparkSession) -> None:
    def has_kana_or_kanji(rows: list[dict[str, Any]]) -> bool:
        return any(any("぀" <= ch <= "鿿" for ch in row["name"]) for row in rows)

    japanese = make_faker(seed=1, locale="ja_JP")
    assert has_kana_or_kanji(japanese.pyspark_rows("name string", rows=10))
    assert not has_kana_or_kanji(japanese.pyspark_rows("name string", rows=10, locale="en_US"))
    assert has_kana_or_kanji(make_faker(seed=1).pyspark_rows("name string", 10, locale="ja_JP"))


@pytest.mark.parametrize(
    ("locale", "expected"),
    [(None, None), ("en_US", None), ("ja_JP", "ja_JP"), ("en_AU", "en_AU"), ("zu_ZA", "zu_ZA")],
)
def test_locale_is_read_from_the_faker(locale: str | None, expected: str | None) -> None:
    faker = make_faker(locale=locale)
    provider = next(p for p in faker.providers if isinstance(p, PySparkProvider))
    assert provider._locale() == expected


def test_default_locale_data_is_plain_english(spark: SparkSession) -> None:
    # Regression: the locale was once inferred as Latin from Faker's lorem provider.
    rows = make_faker(seed=1).pyspark_rows("postcode string, note string", rows=20)
    assert all(row["postcode"].replace("-", "").isdigit() for row in rows)


def test_providers_added_by_the_caller_work_in_overrides(spark: SparkSession) -> None:
    class PlanetProvider(BaseProvider):
        def planet(self) -> str:
            return "Mars"

    faker = make_faker(seed=1)
    faker.add_provider(PlanetProvider)
    rows = faker.pyspark_rows("home string", rows=3, overrides={"home": "planet"})
    assert [row["home"] for row in rows] == ["Mars", "Mars", "Mars"]


def test_explicit_session_is_used(spark: SparkSession, fake: Faker) -> None:
    df = fake.pyspark_dataframe("a int", rows=1, spark=spark)
    assert df.sparkSession is spark


def test_faker_cli() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "faker", "pyspark_schema", "-i", "faker_pyspark"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert result.stdout.strip().startswith("StructType(")
