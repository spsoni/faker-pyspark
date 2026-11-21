"""The row engine on its own: no Spark session needed."""

import datetime
import decimal
import re
from typing import Any

import pytest
from faker import Faker
from faker.providers import BaseProvider
from hypothesis import given, settings
from hypothesis import strategies as st
from pyspark.sql import types as T

from faker_pyspark import GeneratorSpecError, UnsupportedTypeError
from faker_pyspark._catalog import string_method
from faker_pyspark._generate import RowGenerator, row_seed

SEEDS = st.integers(min_value=0, max_value=2**64 - 1)


def struct(*fields: tuple[str, T.DataType] | T.StructField) -> T.StructType:
    return T.StructType(
        [f if isinstance(f, T.StructField) else T.StructField(f[0], f[1]) for f in fields]
    )


def one(schema: T.StructType, seed: int = 1, **options: Any) -> dict[str, Any]:
    return RowGenerator(schema, seed=seed, **options).row(0)


def many(schema: T.StructType, count: int = 200, **options: Any) -> list[dict[str, Any]]:
    return RowGenerator(schema, seed=options.pop("seed", 1), **options).rows(range(count))


ALL_TYPES = struct(
    ("a_string", T.StringType()),
    ("a_varchar", T.VarcharType(5)),
    ("a_char", T.CharType(4)),
    ("a_bool", T.BooleanType()),
    ("a_byte", T.ByteType()),
    ("a_short", T.ShortType()),
    ("an_int", T.IntegerType()),
    ("a_long", T.LongType()),
    ("a_float", T.FloatType()),
    ("a_double", T.DoubleType()),
    ("a_decimal", T.DecimalType(7, 3)),
    ("a_date", T.DateType()),
    ("a_timestamp", T.TimestampType()),
    ("a_timestamp_ntz", T.TimestampNTZType()),
    ("an_interval", T.DayTimeIntervalType()),
    ("some_bytes", T.BinaryType()),
    ("nothing", T.NullType()),
    ("an_array", T.ArrayType(T.IntegerType())),
    ("a_map", T.MapType(T.StringType(), T.DoubleType())),
    ("a_struct", struct(("x", T.IntegerType()), ("y", T.ArrayType(T.StringType())))),
)


def test_every_supported_type_gets_a_python_value_of_the_right_kind() -> None:
    for row in many(ALL_TYPES, count=50):
        assert isinstance(row["a_string"], str)
        assert isinstance(row["a_varchar"], str) and 1 <= len(row["a_varchar"]) <= 5
        assert isinstance(row["a_char"], str) and len(row["a_char"]) == 4
        assert isinstance(row["a_bool"], bool)
        assert 0 <= row["a_byte"] <= 127
        assert 0 <= row["a_short"] <= 2**15 - 1
        assert 0 <= row["an_int"] <= 2**31 - 1
        assert 0 <= row["a_long"] <= 2**63 - 1
        assert isinstance(row["a_float"], float)
        assert isinstance(row["a_double"], float)
        assert isinstance(row["a_decimal"], decimal.Decimal)
        assert row["a_decimal"].as_tuple().exponent == -3
        assert len(row["a_decimal"].as_tuple().digits) <= 7
        assert type(row["a_date"]) is datetime.date
        assert row["a_timestamp"].tzinfo is datetime.UTC
        assert row["a_timestamp_ntz"].tzinfo is None
        assert isinstance(row["an_interval"], datetime.timedelta)
        assert isinstance(row["some_bytes"], bytearray) and 1 <= len(row["some_bytes"]) <= 16
        assert row["nothing"] is None
        assert 1 <= len(row["an_array"]) <= 3
        assert all(isinstance(item, int) for item in row["an_array"])
        assert all(isinstance(key, str) for key in row["a_map"])
        assert set(row["a_struct"]) == {"x", "y"}


def test_both_boolean_values_and_a_spread_of_dates_appear() -> None:
    rows = many(struct(("flag", T.BooleanType()), ("day", T.DateType())))
    assert {row["flag"] for row in rows} == {True, False}
    years = {row["day"].year for row in rows}
    assert min(years) >= 1970 and max(years) <= 2029 and len(years) > 20


@settings(max_examples=50, deadline=None)
@given(seed=SEEDS, index=st.integers(0, 10**9))
def test_a_row_depends_only_on_seed_and_index(seed: int, index: int) -> None:
    first = RowGenerator(ALL_TYPES, seed=seed)
    second = RowGenerator(ALL_TYPES, seed=seed)
    second.row(index + 1)  # generating other rows first must not matter
    assert first.row(index) == second.row(index)


def test_different_seeds_and_indexes_give_different_rows() -> None:
    schema = struct(("email", T.StringType()), ("n", T.LongType()))
    assert one(schema, seed=1) != one(schema, seed=2)
    generator = RowGenerator(schema, seed=1)
    assert generator.row(0) != generator.row(1)


def test_row_seeds_do_not_collide_for_neighbouring_runs() -> None:
    seeds = {row_seed(seed, index) for seed in range(50) for index in range(200)}
    assert len(seeds) == 50 * 200


@pytest.mark.parametrize(
    ("name", "method"),
    [
        ("email", "email"),
        ("EMAIL", "email"),
        ("customer_email", "email"),
        ("billing_city", "city"),
        ("fname", "first_name"),
        ("contact_phone", "phone_number"),
        ("zip", "postcode"),
        ("uuid", "uuid4"),
        ("name", "name"),
        ("product_name", None),
        ("order_state", None),
        ("widget", None),
    ],
)
def test_column_names_map_to_faker_methods(name: str, method: str | None) -> None:
    assert string_method(name) == method


def test_named_string_columns_look_like_what_they_are_called() -> None:
    schema = struct(
        ("email", T.StringType()),
        ("customer_uuid", T.StringType()),
        ("ipv4", T.StringType()),
        ("emails", T.ArrayType(T.StringType())),
    )
    for row in many(schema, count=30):
        assert re.fullmatch(r"[^@\s]+@[^@\s]+\.[a-z]+", row["email"])
        assert re.fullmatch(r"[0-9a-f-]{36}", row["customer_uuid"])
        assert re.fullmatch(r"\d+\.\d+\.\d+\.\d+", row["ipv4"])
        assert all("@" in item for item in row["emails"])


def test_named_numeric_and_date_columns_stay_in_plausible_ranges() -> None:
    schema = struct(
        ("age", T.IntegerType()),
        ("unit_price", T.DecimalType(10, 2)),
        ("latitude", T.DoubleType()),
        ("rating", T.FloatType()),
        ("year", T.ShortType()),
        ("date_of_birth", T.DateType()),
        ("salary", T.DecimalType(4, 2)),
        ("tiny_salary", T.ByteType()),
    )
    for row in many(schema):
        assert 0 <= row["age"] <= 100
        assert 0 <= row["unit_price"] <= 10_000
        assert -90 <= row["latitude"] <= 90
        assert 0 <= row["rating"] <= 5
        assert 1970 <= row["year"] <= 2030
        assert 1940 <= row["date_of_birth"].year <= 2010
        # The named range does not fit decimal(4,2); the type's own limit wins.
        assert row["salary"] <= decimal.Decimal("99.99")
        assert 0 <= row["tiny_salary"] <= 127


def test_overrides_by_method_kwargs_choices_and_function() -> None:
    schema = struct(
        ("code", T.StringType()),
        ("level", T.IntegerType()),
        ("status", T.StringType()),
        ("label", T.StringType()),
        ("picked", T.IntegerType()),
    )
    rows = many(
        schema,
        overrides={
            "code": "country_code",
            "level": ("pyint", {"min_value": 5, "max_value": 7}),
            "status": ["new", "paid", "shipped"],
            "label": lambda faker: f"item-{faker.pyint(1, 9)}",
            "picked": range(3),
        },
    )
    assert all(re.fullmatch(r"[A-Z]{2}", row["code"]) for row in rows)
    assert {row["level"] for row in rows} == {5, 6, 7}
    assert {row["status"] for row in rows} == {"new", "paid", "shipped"}
    assert all(re.fullmatch(r"item-[1-9]", row["label"]) for row in rows)
    assert {row["picked"] for row in rows} == {0, 1, 2}


def test_overrides_reach_nested_fields_and_array_elements() -> None:
    schema = struct(
        ("address", struct(("city", T.StringType()), ("zone", T.StringType()))),
        ("tags", T.ArrayType(T.StringType())),
        ("people", T.ArrayType(struct(("role", T.StringType())))),
    )
    row = one(
        schema,
        overrides={"address.zone": ["north"], "people[].role": ["admin"]},
    )
    assert row["address"]["zone"] == "north"
    assert {person["role"] for person in row["people"]} == {"admin"}


def test_metadata_drives_generation_and_overrides_beat_it() -> None:
    schema = struct(
        T.StructField("a", T.StringType(), metadata={"faker": "country_code"}),
        T.StructField(
            "b",
            T.IntegerType(),
            metadata={"faker": {"method": "pyint", "kwargs": {"min_value": 3, "max_value": 3}}},
        ),
        T.StructField("c", T.StringType(), metadata={"faker": {"choices": ["x"]}}),
        T.StructField("email", T.StringType(), metadata={"faker": "country_code"}),
    )
    row = one(schema, overrides={"email": ["override@example.com"]})
    assert re.fullmatch(r"[A-Z]{2}", row["a"])
    assert row["b"] == 3
    assert row["c"] == "x"
    assert row["email"] == "override@example.com"


def test_more_override_values_are_fitted_or_passed_through() -> None:
    schema = struct(
        ("flag", T.BooleanType()),
        ("blob", T.BinaryType()),
        ("seen", T.TimestampNTZType()),
        ("missing", T.StringType()),
        ("code", T.CharType(3)),
    )
    moment = datetime.datetime(2024, 1, 2, 3, 4, 5)
    row = one(
        schema,
        overrides={
            "flag": [1],
            "blob": ["abc"],
            "seen": [moment],
            "missing": lambda faker: None,
            "code": ["abcdef"],
        },
    )
    assert row == {
        "flag": True,
        "blob": bytearray(b"abc"),
        "seen": moment,
        "missing": None,
        "code": "abc",
    }


def test_declared_char_and_varchar_lengths_in_metadata_are_honoured() -> None:
    # This is how Spark hands back a table column declared as varchar(4) or char(6).
    schema = struct(
        T.StructField("v", T.StringType(), metadata={"__CHAR_VARCHAR_TYPE_STRING": "varchar(4)"}),
        T.StructField("c", T.StringType(), metadata={"__CHAR_VARCHAR_TYPE_STRING": "char(6)"}),
    )
    for row in many(schema, count=40):
        assert 1 <= len(row["v"]) <= 4
        assert len(row["c"]) == 6


@pytest.mark.skipif(not hasattr(T, "TimeType"), reason="TimeType needs PySpark 4.1+")
def test_time_type() -> None:
    row = one(struct(("at", T.TimeType())))
    assert isinstance(row["at"], datetime.time)


def test_override_values_are_fitted_to_the_column_type() -> None:
    schema = struct(
        ("as_text", T.StringType()),
        ("as_decimal", T.DecimalType(6, 2)),
        ("as_date", T.DateType()),
        ("as_timestamp", T.TimestampNTZType()),
        ("as_double", T.DoubleType()),
    )
    row = one(
        schema,
        overrides={
            "as_text": [42],
            "as_decimal": [3.14159],
            "as_date": [datetime.datetime(2024, 5, 6, 7, 8)],
            "as_timestamp": [datetime.date(2024, 5, 6)],
            "as_double": [decimal.Decimal("1.5")],
        },
    )
    assert row == {
        "as_text": "42",
        "as_decimal": decimal.Decimal("3.14"),
        "as_date": datetime.date(2024, 5, 6),
        "as_timestamp": datetime.datetime(2024, 5, 6),
        "as_double": 1.5,
    }


def test_override_times_without_a_zone_are_utc_in_a_timestamp_column() -> None:
    # Otherwise the driver would read them in its own zone and an executor in UTC.
    melbourne = datetime.timezone(datetime.timedelta(hours=10))
    schema = struct(
        ("bare", T.TimestampType()),
        ("from_date", T.TimestampType()),
        ("zoned", T.TimestampType()),
        ("no_zone_column", T.TimestampNTZType()),
    )
    row = one(
        schema,
        overrides={
            "bare": [datetime.datetime(2024, 5, 6, 7, 8)],
            "from_date": [datetime.date(2024, 5, 6)],
            "zoned": [datetime.datetime(2024, 5, 6, 7, 8, tzinfo=melbourne)],
            "no_zone_column": [datetime.datetime(2024, 5, 6, 7, 8)],
        },
    )
    assert row == {
        "bare": datetime.datetime(2024, 5, 6, 7, 8, tzinfo=datetime.UTC),
        "from_date": datetime.datetime(2024, 5, 6, tzinfo=datetime.UTC),
        "zoned": datetime.datetime(2024, 5, 6, 7, 8, tzinfo=melbourne),
        "no_zone_column": datetime.datetime(2024, 5, 6, 7, 8),
    }
    assert row["zoned"].utcoffset() == datetime.timedelta(hours=10)


@pytest.mark.parametrize(
    ("schema", "options", "message"),
    [
        (struct(("a", T.StringType())), {"overrides": {"b": "email"}}, "not in the schema: b"),
        (struct(("a", T.StringType())), {"overrides": {"a": "no_such_method"}}, "no method"),
        (struct(("a", T.StringType())), {"overrides": {"a": 3.5}}, "cannot use 3.5"),
        (struct(("a", T.StringType())), {"overrides": {"a": []}}, "empty"),
        (struct(("a", T.ByteType())), {"overrides": {"a": [999]}}, "outside the range"),
        (struct(("a", T.DecimalType(3, 1))), {"overrides": {"a": [1234]}}, "more than 3 digits"),
        (struct(("a", T.IntegerType())), {"overrides": {"a": ["x"]}}, "cannot use 'x'"),
        (struct(("a", T.StringType())), {"null_ratio": 1.5}, "null_ratio"),
        (struct(("a", T.StringType())), {"array_size": (3, 1)}, "array_size"),
        (
            struct(T.StructField("a", T.StringType(), metadata={"faker": {"nope": 1}})),
            {},
            "'method' or 'choices'",
        ),
    ],
)
def test_bad_specifications_are_rejected_up_front(
    schema: T.StructType, options: dict[str, Any], message: str
) -> None:
    with pytest.raises(GeneratorSpecError, match=message):
        RowGenerator(schema, seed=1, **options).row(0)


class MysteryType(T.DataType):
    """Stands in for any Spark type the engine has no generator for."""


def test_unsupported_type_names_the_column_and_can_be_overridden() -> None:
    schema = struct(("gap", MysteryType()))
    with pytest.raises(UnsupportedTypeError, match=r"column 'gap'.*overrides"):
        RowGenerator(schema, seed=1)
    assert one(schema, overrides={"gap": lambda faker: 14}) == {"gap": 14}


def test_null_ratio_applies_to_nullable_columns_only() -> None:
    schema = struct(
        T.StructField("optional", T.StringType(), nullable=True),
        T.StructField("required", T.StringType(), nullable=False),
        T.StructField("inner", struct(T.StructField("deep", T.IntegerType())), nullable=False),
    )
    rows = many(schema, count=400, null_ratio=0.5)
    nulls = sum(row["optional"] is None for row in rows)
    assert 140 < nulls < 260
    assert all(row["required"] is not None for row in rows)
    assert any(row["inner"]["deep"] is None for row in rows)

    assert all(row["optional"] is None for row in many(schema, count=20, null_ratio=1.0))
    assert all(row["optional"] is not None for row in many(schema, count=20))


def test_array_size_bounds_arrays_and_maps() -> None:
    schema = struct(
        ("items", T.ArrayType(T.LongType())), ("lookup", T.MapType(T.LongType(), T.LongType()))
    )
    for row in many(schema, count=50, array_size=(0, 2)):
        assert len(row["items"]) <= 2 and len(row["lookup"]) <= 2
    assert all(row["items"] == [] for row in many(schema, count=5, array_size=(0, 0)))


def test_locale_changes_the_data() -> None:
    schema = struct(("name", T.StringType()))
    japanese = many(schema, count=20, locale="ja_JP")
    assert any(re.search(r"[぀-ヿ一-鿿]", row["name"]) for row in japanese)
    assert not any(re.search(r"[぀-ヿ一-鿿]", row["name"]) for row in many(schema, count=20))


def test_extra_providers_are_usable_in_overrides() -> None:
    class PlanetProvider(BaseProvider):
        def planet(self) -> str:
            return str(self.random_element(["Mars", "Venus"]))

    schema = struct(("home", T.StringType()))
    rows = many(schema, count=20, providers=[PlanetProvider], overrides={"home": "planet"})
    assert {row["home"] for row in rows} == {"Mars", "Venus"}


def test_a_faker_instance_is_what_override_functions_receive() -> None:
    seen: list[Any] = []

    def record(faker: Faker) -> str:
        seen.append(faker)
        return "x"

    one(struct(("a", T.StringType())), overrides={"a": record})
    assert isinstance(seen[0], Faker)
