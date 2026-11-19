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
        SparkSession.builder.master("local[1]")
        .appName("faker_pyspark-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def fake(spark: SparkSession) -> Faker:
    # Depends on `spark` so the provider's own getOrCreate() reuses the test session.
    faker = Faker()
    faker.add_provider(PySparkProvider)
    return faker
