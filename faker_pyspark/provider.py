"""The Faker provider: the public face of faker-pyspark."""

from collections.abc import Mapping, Sequence
from typing import Any

from faker.providers import BaseProvider
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, _parse_datatype_string

from ._generate import Override, RowGenerator, random_schema, spark_schema
from .errors import GeneratorSpecError

Schema = StructType | str


class PySparkProvider(BaseProvider):
    """Generate PySpark schemas, rows and DataFrames of fake data.

    >>> from faker import Faker
    >>> from faker_pyspark import PySparkProvider
    >>> fake = Faker()
    >>> fake.add_provider(PySparkProvider)
    >>> df = fake.pyspark_dataframe("id long, email string, signup date", rows=100)

    Every value comes from the Faker instance's own random generator, so
    ``Faker.seed(n)`` or ``fake.seed_instance(n)`` makes the output repeatable.
    """

    def pyspark_schema(
        self, columns: int | tuple[int, int] = (3, 8), *, nested: bool = False
    ) -> StructType:
        """A random schema of distinct, realistically named columns.

        :param columns: how many columns, or an inclusive ``(min, max)`` range.
        :param nested: also allow struct, array and map columns.
        """
        return random_schema(self.generator, columns, nested)

    def pyspark_rows(
        self,
        schema: Schema,
        rows: int = 10,
        *,
        overrides: Mapping[str, Override] | None = None,
        null_ratio: float = 0.0,
        array_size: tuple[int, int] = (1, 3),
        locale: str | Sequence[str] | None = None,
        spark: SparkSession | None = None,
    ) -> list[dict[str, Any]]:
        """Rows of fake data for ``schema``, as plain dictionaries.

        Takes the same options as :meth:`pyspark_dataframe`. A Spark session is only
        needed when ``schema`` is a DDL string, to parse it.
        """
        generator = self._row_generator(
            self._struct(schema, spark),
            overrides=overrides,
            null_ratio=null_ratio,
            array_size=array_size,
            locale=locale,
        )
        return generator.rows(range(_row_count(rows)))

    def pyspark_dataframe(
        self,
        schema: Schema | None = None,
        rows: int = 10,
        *,
        spark: SparkSession | None = None,
        overrides: Mapping[str, Override] | None = None,
        null_ratio: float = 0.0,
        array_size: tuple[int, int] = (1, 3),
        locale: str | Sequence[str] | None = None,
    ) -> DataFrame:
        """A DataFrame of fake data.

        :param schema: a ``StructType``, a DDL string such as ``"id long, email string"``,
            or ``None`` for a random schema.
        :param rows: how many rows.
        :param spark: the session to use. Defaults to the active session, or a new one.
        :param overrides: how to fill particular columns, by name (dotted for nested
            fields, ``tags[]`` for array elements). Each value is a Faker method name, a
            ``(method, kwargs)`` tuple, a list of values to choose from, or a function
            that takes the Faker and returns a value.
        :param null_ratio: the share of values, between 0 and 1, left null in nullable
            columns.
        :param array_size: inclusive ``(min, max)`` length of arrays and maps.
        :param locale: Faker locale or locales for the data. Defaults to the locale of
            the Faker this provider is attached to.

        Columns without an override are filled from a ``faker`` entry in the field's
        metadata if there is one, then by what the column name suggests (``email``,
        ``billing_city``, ``unit_price``), then by the column's type.
        """
        session = spark or _session()
        struct = self.pyspark_schema() if schema is None else self._struct(schema, session)
        generator = self._row_generator(
            struct,
            overrides=overrides,
            null_ratio=null_ratio,
            array_size=array_size,
            locale=locale,
        )
        data = generator.rows(range(_row_count(rows)))
        return session.createDataFrame(data, schema=spark_schema(struct))

    # -- helpers ---------------------------------------------------------------------

    def _row_generator(
        self,
        schema: StructType,
        *,
        overrides: Mapping[str, Override] | None,
        null_ratio: float,
        array_size: tuple[int, int],
        locale: str | Sequence[str] | None,
    ) -> RowGenerator:
        return RowGenerator(
            schema,
            # One draw from the caller's Faker seeds the whole run, so a seeded Faker
            # gives the same data and two calls in a row give different data.
            seed=self.generator.random.getrandbits(64),
            locale=locale or self._locale(),
            providers=self._extra_providers(),
            overrides=overrides,
            null_ratio=null_ratio,
            array_size=array_size,
        )

    def _locale(self) -> str | None:
        # A Faker does not expose the locale it was created with to its providers. It does
        # record, on each provider, the locale that provider was loaded for, and falls
        # back to en_US where a locale has no version of one. Names and addresses are the
        # providers nearly every locale defines, so they say what the caller asked for.
        loaded = {
            type(provider).__module__: getattr(provider, "__lang__", None)
            for provider in self.generator.providers
        }
        for kind in ("address", "person"):
            for module, language in loaded.items():
                if module.startswith(f"faker.providers.{kind}") and language not in (None, "en_US"):
                    return str(language)
        return None

    def _extra_providers(self) -> list[type]:
        # Providers the caller added themselves, so their methods work in overrides.
        return [
            type(provider)
            for provider in reversed(self.generator.providers)
            if not type(provider).__module__.startswith("faker.providers")
            and not isinstance(provider, PySparkProvider)
        ]

    @staticmethod
    def _struct(schema: Schema, spark: SparkSession | None) -> StructType:
        if isinstance(schema, StructType):
            return schema
        # Spark parses the DDL, so every type Spark accepts is accepted here. That needs
        # a session, which is why one is started if none is running.
        _ = spark or _session()
        parsed = _parse_datatype_string(schema)
        if not isinstance(parsed, StructType):
            raise GeneratorSpecError(
                f"schema must describe columns, such as 'id long, name string'; got {schema!r}"
            )
        return parsed


def _session() -> SparkSession:
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def _row_count(rows: int) -> int:
    if rows < 0:
        raise ValueError(f"rows must not be negative, got {rows}")
    return rows


Provider = PySparkProvider
