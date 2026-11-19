"""Generating on the executors must give exactly what generating on the driver gives."""

from typing import Any

import pytest
from faker import Faker
from faker.providers import BaseProvider
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from faker_pyspark import FakerPySparkError, GeneratorSpecError, _distributed, provider

from .conftest import make_faker

EVERY_TYPE = (
    "a string, b boolean, c tinyint, d smallint, e int, f bigint, g float, h double, "
    "i decimal(38,10), j date, k timestamp, l timestamp_ntz, m binary, "
    "n interval day to second, o array<array<int>>, p map<string,array<double>>, "
    "q struct<x:int,y:struct<z:date,w:binary>>, r varchar(3), s char(2), "
    "t map<string,binary>, email string, age int"
)


def collect(df: DataFrame) -> list[dict[str, Any]]:
    return [row.asDict(recursive=True) for row in df.collect()]


@pytest.mark.parametrize("partitions", [1, 3, 7])
def test_distributed_matches_driver_for_every_type(spark: SparkSession, partitions: int) -> None:
    options: dict[str, Any] = {"rows": 60, "null_ratio": 0.2}
    on_driver = make_faker(seed=11).pyspark_dataframe(EVERY_TYPE, distributed=False, **options)
    on_executors = make_faker(seed=11).pyspark_dataframe(
        EVERY_TYPE, distributed=True, partitions=partitions, **options
    )
    assert on_executors.rdd.getNumPartitions() == partitions
    assert on_executors.schema == on_driver.schema
    assert collect(on_executors) == collect(on_driver)


def test_schema_details_survive(spark: SparkSession, fake: Faker) -> None:
    schema = T.StructType(
        [
            T.StructField("id", T.LongType(), nullable=False),
            T.StructField("code", T.StringType(), metadata={"faker": "country_code"}),
        ]
    )
    df = fake.pyspark_dataframe(schema, rows=20, distributed=True)
    assert df.schema == schema
    assert all(len(row["code"]) == 2 for row in collect(df))


def test_overrides_functions_and_custom_providers_reach_the_executors(
    spark: SparkSession,
) -> None:
    class PlanetProvider(BaseProvider):
        def planet(self) -> str:
            return str(self.random_element(["Mars", "Venus"]))

    faker = make_faker(seed=4)
    faker.add_provider(PlanetProvider)
    df = faker.pyspark_dataframe(
        "home string, status string, label string",
        rows=40,
        distributed=True,
        partitions=4,
        overrides={
            "home": "planet",
            "status": ["new", "paid"],
            "label": lambda f: f"item-{f.pyint(1, 3)}",
        },
    )
    rows = collect(df)
    assert {row["home"] for row in rows} == {"Mars", "Venus"}
    assert {row["status"] for row in rows} == {"new", "paid"}
    assert {row["label"] for row in rows} == {"item-1", "item-2", "item-3"}


def test_locale_reaches_the_executors(spark: SparkSession) -> None:
    df = make_faker(seed=1, locale="ja_JP").pyspark_dataframe(
        "name string", rows=10, distributed=True
    )
    assert any(any("぀" <= ch <= "鿿" for ch in row["name"]) for row in collect(df))


def test_zero_rows(spark: SparkSession, fake: Faker) -> None:
    df = fake.pyspark_dataframe("a int", rows=0, distributed=True)
    assert df.count() == 0 and df.columns == ["a"]


def test_bad_override_fails_on_the_driver_before_any_job(fake: Faker) -> None:
    with pytest.raises(GeneratorSpecError, match="not in the schema"):
        fake.pyspark_dataframe("a int", distributed=True, overrides={"b": [1]})


def test_large_requests_distribute_automatically(
    spark: SparkSession, fake: Faker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(provider, "DISTRIBUTE_ABOVE", 50)
    small = fake.pyspark_dataframe("id long", rows=50, partitions=5)
    large = fake.pyspark_dataframe("id long", rows=51, partitions=5)
    # Only the executor path honours `partitions`.
    assert large.rdd.getNumPartitions() == 5
    assert small.count() == 50 and large.count() == 51

    monkeypatch.setattr(_distributed, "available", lambda: False)
    assert fake.pyspark_dataframe("id long", rows=51).count() == 51


def test_asking_for_distribution_without_arrow_explains_what_to_install(
    fake: Faker, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(_distributed, "available", lambda: False)
    with pytest.raises(FakerPySparkError, match=r"faker-pyspark\[distributed\]"):
        fake.pyspark_dataframe("a int", distributed=True)


def test_unsupported_arrow_type_is_reported() -> None:
    class MysteryType(T.DataType):
        pass

    with pytest.raises(FakerPySparkError, match="distributed=False"):
        _distributed.to_arrow_schema(T.StructType([T.StructField("x", MysteryType())]))


def test_batch_function_in_process_matches_the_row_generator() -> None:
    # The same code the executors run, called directly so that coverage can see it.
    import pyarrow as pa

    schema = T.StructType(
        [
            T.StructField("email", T.StringType()),
            T.StructField("blob", T.BinaryType()),
            T.StructField("blobs", T.ArrayType(T.BinaryType())),
            T.StructField("lookup", T.MapType(T.StringType(), T.BinaryType())),
            T.StructField("inner", T.StructType([T.StructField("b", T.BinaryType())])),
            T.StructField("numbers", T.ArrayType(T.IntegerType())),
        ]
    )
    options: dict[str, Any] = {
        "seed": 9,
        "locale": None,
        "providers": [],
        "overrides": {},
        "null_ratio": 0.3,
        "array_size": (1, 2),
    }
    generate = _distributed._batch_function(schema, schema, options)
    indexes = [pa.record_batch({"id": [0, 1, 2]}), pa.record_batch({"id": [10, 11]})]
    batches = list(generate(iter(indexes)))

    assert [batch.num_rows for batch in batches] == [3, 2]
    assert batches[0].schema == _distributed.to_arrow_schema(schema)

    from faker_pyspark._generate import RowGenerator

    expected = RowGenerator(schema, **options).rows([0, 1, 2, 10, 11])
    produced = [row for batch in batches for row in batch.to_pylist()]
    for want, got in zip(expected, produced, strict=True):
        assert got["email"] == want["email"]
        assert got["numbers"] == want["numbers"]
        assert got["blob"] == (None if want["blob"] is None else bytes(want["blob"]))
        if want["lookup"] is not None:
            assert dict(got["lookup"]) == {
                key: None if value is None else bytes(value)
                for key, value in want["lookup"].items()
            }


def test_a_larger_run_is_complete_and_distinct(spark: SparkSession, fake: Faker) -> None:
    df = fake.pyspark_dataframe("uuid string, n long", rows=20_000, distributed=True, partitions=4)
    assert df.count() == 20_000
    assert df.select("uuid").distinct().count() == 20_000
