"""Seeding and column selection: properties that must hold for every seed."""

import datetime
from collections import Counter
from typing import Any

import pytest
from faker import Faker
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import SparkSession

from faker_pyspark import PySparkProvider
from faker_pyspark.pyspark import fake_fields_list

SEEDS = st.integers(min_value=0, max_value=2**32 - 1)


def seeded(seed: int) -> Faker:
    faker = Faker()
    faker.add_provider(PySparkProvider)
    faker.seed_instance(seed)
    return faker


def stable_values(row: dict[str, Any]) -> dict[str, Any]:
    # Faker anchors most date and time methods to the current clock, so those values
    # move between two calls even under one seed. Everything else must match exactly.
    temporal = (datetime.date, datetime.time, datetime.timedelta)
    return {key: value for key, value in row.items() if not isinstance(value, temporal)}


def test_fake_fields_list_is_sorted_and_unique() -> None:
    fields = fake_fields_list(seeded(0))
    assert fields == sorted(set(fields))


@settings(max_examples=15, deadline=None)
@given(seed=SEEDS, bounds=st.tuples(st.integers(0, 12), st.integers(0, 12)).map(sorted))
def test_column_names_are_distinct_and_within_inclusive_bounds(
    seed: int, bounds: list[int]
) -> None:
    low, high = bounds
    names = seeded(seed).pyspark_column_names(min_columns=low, max_columns=high)
    assert low <= len(names) <= high
    assert len(set(names)) == len(names)


def test_column_count_reaches_both_bounds() -> None:
    faker = seeded(7)
    counts = Counter(
        len(faker.pyspark_column_names(min_columns=2, max_columns=4)) for _ in range(60)
    )
    assert set(counts) == {2, 3, 4}


@pytest.mark.parametrize(("low", "high"), [(5, 3), (-1, 3)])
def test_column_names_rejects_bad_bounds(low: int, high: int) -> None:
    with pytest.raises(ValueError, match="min_columns"):
        seeded(0).pyspark_column_names(min_columns=low, max_columns=high)


@settings(max_examples=15, deadline=None)
@given(seed=SEEDS)
def test_same_seed_gives_same_column_names(seed: int) -> None:
    first = seeded(seed).pyspark_column_names(allow_complex_types=True)
    second = seeded(seed).pyspark_column_names(allow_complex_types=True)
    assert first == second


def test_different_seeds_give_different_column_names() -> None:
    draws = {tuple(seeded(seed).pyspark_column_names()) for seed in range(8)}
    assert len(draws) > 1


@pytest.mark.parametrize("seed", [0, 1, 20261119])
def test_same_seed_gives_same_schema_and_data(spark: SparkSession, seed: int) -> None:
    first = seeded(seed).pyspark_dataframe(spark=spark, rows=5)
    second = seeded(seed).pyspark_dataframe(spark=spark, rows=5)

    assert first.schema == second.schema
    first_rows = [stable_values(row.asDict(recursive=True)) for row in first.collect()]
    second_rows = [stable_values(row.asDict(recursive=True)) for row in second.collect()]
    assert first_rows == second_rows


def test_class_level_seed_is_honoured(spark: SparkSession) -> None:
    def draw() -> list[str]:
        Faker.seed(99)
        faker = Faker()
        faker.add_provider(PySparkProvider)
        names: list[str] = faker.pyspark_schema(spark=spark).fieldNames()
        return names

    assert draw() == draw()
