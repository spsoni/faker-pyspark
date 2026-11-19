import os
import sys
from collections.abc import Iterator

import pytest
from faker import Faker
from pyspark.sql import SparkSession

from faker_pyspark import PySparkProvider

# Spark workers default to whatever `python3` is on PATH, which need not be the
# interpreter running the tests (for example `.venv/bin/python -m pytest`).
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = (
        SparkSession.builder.master("local[2]")
        .appName("faker_pyspark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def make_faker(seed: int | None = None, locale: str | None = None) -> Faker:
    faker = Faker(locale)
    faker.add_provider(PySparkProvider)
    if seed is not None:
        faker.seed_instance(seed)
    return faker


@pytest.fixture
def fake(spark: SparkSession) -> Faker:
    # Depends on `spark` so the provider's own session lookup finds the test session.
    return make_faker(seed=1234)
