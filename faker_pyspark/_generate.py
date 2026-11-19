"""Turn a Spark schema into rows of fake data.

Nothing here needs a Spark session: it works on ``pyspark.sql.types`` objects and plain
Python values, so the same code runs on the driver and inside executors.

Each row is generated from its own seed, derived from the run's seed and the row's index.
Row 7 is therefore the same whether it is produced first, last, alone, or on another
machine. That is what makes distributed generation reproducible.
"""

import datetime
import decimal
import re
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from faker import Faker
from pyspark.sql import types as T

from . import _catalog
from .errors import GeneratorSpecError, UnsupportedTypeError

#: How one column's values are produced: a Faker method name, a method name with
#: keyword arguments, a list of values to choose from, or a function of the Faker.
Override = str | tuple[str, Mapping[str, Any]] | Sequence[Any] | Callable[[Faker], Any]

Generator = Callable[[Faker], Any]

METADATA_KEY = "faker"

_CHAR_VARCHAR_KEY = "__CHAR_VARCHAR_TYPE_STRING"
_CHAR_VARCHAR = re.compile(r"(char|varchar)\((\d+)\)")

_INTEGRAL_RANGES: dict[str, tuple[int, int]] = {
    "ByteType": (-(2**7), 2**7 - 1),
    "ShortType": (-(2**15), 2**15 - 1),
    "IntegerType": (-(2**31), 2**31 - 1),
    "LongType": (-(2**63), 2**63 - 1),
}
# Defaults stay non-negative: ids, counts and amounts are what integer columns usually
# hold, and a caller who wants the full signed range can ask for it with an override.
_INTEGRAL_DEFAULTS: dict[str, tuple[int, int]] = {
    "ByteType": (0, 2**7 - 1),
    "ShortType": (0, 2**15 - 1),
    "IntegerType": (0, 2**31 - 1),
    "LongType": (0, 2**63 - 1),
}
_FRACTIONAL = ("FloatType", "DoubleType")
_STRINGS = ("StringType", "VarcharType", "CharType")

_EPOCH = datetime.date(1970, 1, 1)
_DEFAULT_YEARS = (1970, 2029)
_GOLDEN = 0x9E3779B97F4A7C15
_MASK = 2**64 - 1


def row_seed(seed: int, index: int) -> int:
    """The seed for one row. Distinct for every (seed, index) pair that matters."""
    return (seed + (index + 1) * _GOLDEN) & _MASK


class RowGenerator:
    """Generates rows for one schema. Build once, then call :meth:`row` per index."""

    def __init__(
        self,
        schema: T.StructType,
        *,
        seed: int,
        locale: str | Sequence[str] | None = None,
        providers: Iterable[Any] = (),
        overrides: Mapping[str, Override] | None = None,
        null_ratio: float = 0.0,
        array_size: tuple[int, int] = (1, 3),
    ) -> None:
        if not 0.0 <= null_ratio <= 1.0:
            raise GeneratorSpecError(f"null_ratio must be between 0 and 1, got {null_ratio}")
        low, high = array_size
        if not 0 <= low <= high:
            raise GeneratorSpecError(
                f"array_size must be (min, max) with 0 <= min <= max: {array_size}"
            )

        self.schema = schema
        self._seed = seed
        self._faker = Faker(locale)
        for provider in providers:
            self._faker.add_provider(provider)
        self._null_ratio = null_ratio
        self._array_size = (low, high)

        self._overrides = dict(overrides or {})
        self._used_overrides: set[str] = set()
        self._fields = [(field.name, self._field(field, field.name)) for field in schema.fields]

        unknown = sorted(set(self._overrides) - self._used_overrides)
        if unknown:
            raise GeneratorSpecError(
                f"overrides name columns that are not in the schema: {', '.join(unknown)}. "
                "Use dotted paths for nested fields, for example 'address.city'."
            )

    def row(self, index: int) -> dict[str, Any]:
        """The row at ``index``. The same index always gives the same row."""
        self._faker.seed_instance(row_seed(self._seed, index))
        return {name: generate(self._faker) for name, generate in self._fields}

    def rows(self, indexes: Iterable[int]) -> list[dict[str, Any]]:
        return [self.row(index) for index in indexes]

    # -- building the per-column generators ------------------------------------------

    def _field(self, field: T.StructField, path: str) -> Generator:
        generate = self._field_values(field, path)
        if not field.nullable or self._null_ratio == 0.0:
            return generate

        null_ratio = self._null_ratio

        def nullable(faker: Faker) -> Any:
            if faker.random.random() < null_ratio:
                return None
            return generate(faker)

        return nullable

    def _field_values(self, field: T.StructField, path: str) -> Generator:
        data_type = field.dataType
        metadata = field.metadata or {}

        # Spark turns `varchar(n)` and `char(n)` in DDL into a string column and keeps
        # the declared type in metadata. Honour the declared length.
        declared = _CHAR_VARCHAR.fullmatch(str(metadata.get(_CHAR_VARCHAR_KEY, "")))
        if declared and type(data_type).__name__ == "StringType":
            kind = T.CharType if declared.group(1) == "char" else T.VarcharType
            data_type = kind(int(declared.group(2)))

        if path in self._overrides:
            self._used_overrides.add(path)
            return self._from_spec(self._overrides[path], data_type, path, "override")

        if METADATA_KEY in metadata:
            return self._from_metadata(metadata[METADATA_KEY], data_type, path)

        return self._for_type(data_type, path, name=field.name)

    def _from_metadata(self, spec: Any, data_type: T.DataType, path: str) -> Generator:
        # Schema metadata is JSON, so it cannot hold a tuple or a function.
        if isinstance(spec, Mapping):
            if "choices" in spec:
                return self._from_spec(list(spec["choices"]), data_type, path, "metadata")
            if "method" in spec:
                method_spec = (spec["method"], dict(spec.get("kwargs", {})))
                return self._from_spec(method_spec, data_type, path, "metadata")
            raise GeneratorSpecError(
                f"column '{path}': '{METADATA_KEY}' metadata must have 'method' or 'choices'"
            )
        return self._from_spec(spec, data_type, path, "metadata")

    def _from_spec(self, spec: Any, data_type: T.DataType, path: str, source: str) -> Generator:
        if callable(spec):
            function: Callable[[Faker], Any] = spec
            return lambda faker: _coerce(function(faker), data_type, path)

        if isinstance(spec, str):
            return self._method(spec, {}, data_type, path)

        if (
            isinstance(spec, tuple)
            and len(spec) == 2
            and isinstance(spec[0], str)
            and isinstance(spec[1], Mapping)
        ):
            return self._method(spec[0], dict(spec[1]), data_type, path)

        if isinstance(spec, Sequence | set | frozenset):
            choices = sorted(spec, key=repr) if isinstance(spec, set | frozenset) else list(spec)
            if not choices:
                raise GeneratorSpecError(f"column '{path}': the list of choices is empty")
            coerced = [_coerce(choice, data_type, path) for choice in choices]
            return lambda faker: faker.random.choice(coerced)

        raise GeneratorSpecError(
            f"column '{path}': cannot use {spec!r} as an {source}. Give a Faker method name, "
            "a (method, kwargs) tuple, a list of values, or a function taking the Faker."
        )

    def _method(
        self, method: str, kwargs: dict[str, Any], data_type: T.DataType, path: str
    ) -> Generator:
        if not callable(getattr(self._faker, method, None)):
            raise GeneratorSpecError(f"column '{path}': Faker has no method '{method}'")
        return lambda faker: _coerce(faker.format(method, **kwargs), data_type, path)

    def _for_type(self, data_type: T.DataType, path: str, name: str | None = None) -> Generator:
        kind = type(data_type).__name__

        if kind in _STRINGS:
            return self._string(data_type, name)
        if kind == "BooleanType":
            return lambda faker: faker.random.random() < 0.5
        if kind in _INTEGRAL_DEFAULTS:
            return _integral(kind, name)
        if kind in _FRACTIONAL:
            return _fractional(name)
        if isinstance(data_type, T.DecimalType):
            return _decimal(data_type, name)
        if kind == "DateType":
            return _date(name)
        if kind == "TimestampType":
            return lambda faker: _timestamp(faker).replace(tzinfo=datetime.UTC)
        if kind == "TimestampNTZType":
            return _timestamp
        if kind == "TimeType":
            return lambda faker: _timestamp(faker).time()
        if kind == "DayTimeIntervalType":
            return lambda faker: datetime.timedelta(
                seconds=faker.random.randint(0, 30 * 86_400),
                microseconds=faker.random.randint(0, 999_999),
            )
        if kind == "BinaryType":
            return lambda faker: bytearray(faker.random.randbytes(faker.random.randint(1, 16)))
        if kind == "NullType":
            return lambda faker: None
        if isinstance(data_type, T.ArrayType):
            return self._array(data_type, path, name)
        if isinstance(data_type, T.MapType):
            return self._map(data_type, path)
        if isinstance(data_type, T.StructType):
            return self._struct(data_type, path)

        raise UnsupportedTypeError(
            f"column '{path}' has type {data_type.simpleString()}, which faker-pyspark cannot "
            f"generate. Supply its values with overrides={{'{path}': ...}}."
        )

    def _string(self, data_type: T.DataType, name: str | None) -> Generator:
        method = _catalog.string_method(name) if name else None
        if method is None or not callable(getattr(self._faker, method, None)):
            method = "word"
        return lambda faker: _fit_text(faker.format(method), data_type)

    def _array(self, data_type: T.ArrayType, path: str, name: str | None) -> Generator:
        # An element takes the column's name, so `emails array<string>` holds emails.
        element = self._for_type(data_type.elementType, f"{path}[]", name=_singular(name))
        low, high = self._array_size
        return lambda faker: [element(faker) for _ in range(faker.random.randint(low, high))]

    def _map(self, data_type: T.MapType, path: str) -> Generator:
        key = self._for_type(data_type.keyType, f"{path}.key")
        value = self._for_type(data_type.valueType, f"{path}.value")
        low, high = self._array_size

        def entries(faker: Faker) -> dict[Any, Any]:
            return {key(faker): value(faker) for _ in range(faker.random.randint(low, high))}

        return entries

    def _struct(self, data_type: T.StructType, path: str) -> Generator:
        fields = [
            (field.name, self._field(field, f"{path}.{field.name}")) for field in data_type.fields
        ]
        return lambda faker: {name: generate(faker) for name, generate in fields}


def _singular(name: str | None) -> str | None:
    if name and name.endswith("s") and not name.endswith("ss"):
        return name[:-1]
    return name


def _integral(kind: str, name: str | None) -> Generator:
    type_low, type_high = _INTEGRAL_RANGES[kind]
    default = _INTEGRAL_DEFAULTS[kind]
    low, high = (name and _catalog.lookup(name, _catalog.NUMERIC_RANGES)) or default
    low, high = max(low, type_low), min(high, type_high)
    if low > high:
        # The usual range for this name does not fit the column at all.
        low, high = default
    return lambda faker: faker.random.randint(low, high)


def _fractional(name: str | None) -> Generator:
    low, high = (name and _catalog.lookup(name, _catalog.NUMERIC_RANGES)) or (0, 1_000_000)
    return lambda faker: round(faker.random.uniform(low, high), 6)


def _decimal(data_type: T.DecimalType, name: str | None) -> Generator:
    precision, scale = data_type.precision, data_type.scale
    largest = 10**precision - 1
    low, high = 0, largest
    named = name and _catalog.lookup(name, _catalog.NUMERIC_RANGES)
    if named:
        low = max(named[0] * 10**scale, -largest)
        high = min(named[1] * 10**scale, largest)
        if low > high:
            # The usual range for this name does not fit the column at all.
            low, high = 0, largest

    def value(faker: Faker) -> decimal.Decimal:
        return decimal.Decimal(faker.random.randint(low, high)).scaleb(-scale)

    return value


def _date(name: str | None) -> Generator:
    first_year, last_year = (
        name and _catalog.lookup(name, _catalog.DATE_YEAR_RANGES)
    ) or _DEFAULT_YEARS
    start = datetime.date(first_year, 1, 1)
    span = (datetime.date(last_year, 12, 31) - start).days
    return lambda faker: start + datetime.timedelta(days=faker.random.randint(0, span))


def _timestamp(faker: Faker) -> datetime.datetime:
    # Drawn from the RNG over a fixed range. Faker's own date methods are anchored to the
    # current clock, which would make a seeded run differ from one second to the next.
    start = datetime.datetime(_DEFAULT_YEARS[0], 1, 1)
    span = int((datetime.datetime(_DEFAULT_YEARS[1] + 1, 1, 1) - start).total_seconds())
    return start + datetime.timedelta(
        seconds=faker.random.randint(0, span - 1), microseconds=faker.random.randint(0, 999_999)
    )


def _fit_text(value: Any, data_type: T.DataType) -> str:
    """Text for a string column, cut to a varchar's length or padded to a char's."""
    text = value if isinstance(value, str) else str(value)
    length: int | None = getattr(data_type, "length", None)
    if length is None:
        return text
    text = text[:length]
    return text.ljust(length) if type(data_type).__name__ == "CharType" else text


def _coerce(value: Any, data_type: T.DataType, path: str) -> Any:
    """Fit a value from an override to the column's type, or say why it does not fit."""
    if value is None:
        return None
    kind = type(data_type).__name__
    try:
        if kind in _STRINGS:
            return _fit_text(value, data_type)
        if kind == "BooleanType":
            return bool(value)
        if kind in _INTEGRAL_RANGES:
            number = int(value)
            low, high = _INTEGRAL_RANGES[kind]
            if not low <= number <= high:
                raise ValueError(f"{number} is outside the range of {data_type.simpleString()}")
            return number
        if kind in _FRACTIONAL:
            return float(value)
        if isinstance(data_type, T.DecimalType):
            return _to_decimal(value, data_type)
        if kind == "DateType":
            return value.date() if isinstance(value, datetime.datetime) else value
        if kind in ("TimestampType", "TimestampNTZType"):
            if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
                return datetime.datetime(value.year, value.month, value.day)
            return value
        if kind == "BinaryType":
            return bytearray(value.encode() if isinstance(value, str) else value)
    except (TypeError, ValueError, ArithmeticError) as error:
        raise GeneratorSpecError(
            f"column '{path}': cannot use {value!r} as {data_type.simpleString()}: {error}"
        ) from error
    return value


def _to_decimal(value: Any, data_type: T.DecimalType) -> decimal.Decimal:
    number = value if isinstance(value, decimal.Decimal) else decimal.Decimal(str(value))
    quantum = decimal.Decimal(1).scaleb(-data_type.scale)
    number = number.quantize(quantum, rounding=decimal.ROUND_HALF_EVEN)
    if len(number.as_tuple().digits) > data_type.precision:
        raise ValueError(f"needs more than {data_type.precision} digits")
    return number


def spark_schema(schema: T.StructType) -> T.StructType:
    """The schema as Spark stores it: ``char``/``varchar`` become ``string``.

    Spark cannot build a DataFrame from Python rows when the schema still names a char or
    varchar type. It records the declared type in the field's metadata instead, and so
    does this.
    """
    return T.StructType([_spark_field(field) for field in schema.fields])


def _spark_field(field: T.StructField) -> T.StructField:
    metadata = dict(field.metadata or {})
    if type(field.dataType).__name__ in ("CharType", "VarcharType"):
        metadata[_CHAR_VARCHAR_KEY] = field.dataType.simpleString()
    return T.StructField(field.name, _spark_type(field.dataType), field.nullable, metadata)


def _spark_type(data_type: T.DataType) -> T.DataType:
    if type(data_type).__name__ in ("CharType", "VarcharType"):
        return T.StringType()
    if isinstance(data_type, T.StructType):
        return spark_schema(data_type)
    if isinstance(data_type, T.ArrayType):
        return T.ArrayType(_spark_type(data_type.elementType), data_type.containsNull)
    if isinstance(data_type, T.MapType):
        return T.MapType(
            _spark_type(data_type.keyType),
            _spark_type(data_type.valueType),
            data_type.valueContainsNull,
        )
    return data_type


def random_schema(faker: Faker, columns: int | tuple[int, int], nested: bool) -> T.StructType:
    """A schema of distinct, sensibly named columns, chosen with the Faker's own RNG."""
    low, high = (columns, columns) if isinstance(columns, int) else columns
    pool = list(_catalog.RANDOM_COLUMNS)
    if nested:
        pool += list(_catalog.RANDOM_NESTED_COLUMNS)
    if not 1 <= low <= high <= len(pool):
        raise GeneratorSpecError(
            f"columns must be between 1 and {len(pool)} (min <= max), got {columns}"
        )
    count = faker.random.randint(low, high)
    chosen = faker.random.sample(pool, k=count)
    return T.StructType([T.StructField(name, data_type) for name, data_type in chosen])
