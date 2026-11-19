"""Generate rows on the executors instead of the driver.

``spark.range`` hands every executor a slice of row indexes, and each executor builds its
own rows for them. Because a row depends only on the run's seed and its index (see
``_generate``), the result is identical to generating everything on the driver, however
many partitions or machines are involved.

Uses only the DataFrame API, so it also works over Spark Connect.
"""

from collections.abc import Callable, Iterator, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from ._generate import Override, RowGenerator
from .errors import FakerPySparkError

if TYPE_CHECKING:
    import pyarrow as pa


def available() -> bool:
    """Whether the packages Spark needs for Arrow batches are installed."""
    try:
        import pandas  # noqa: F401
        import pyarrow  # noqa: F401
    except ImportError:
        return False
    return True


def distributed_dataframe(
    spark: SparkSession,
    schema: T.StructType,
    output_schema: T.StructType,
    *,
    rows: int,
    partitions: int | None,
    seed: int,
    locale: str | Sequence[str] | None,
    providers: Sequence[Any],
    overrides: Mapping[str, Override] | None,
    null_ratio: float,
    array_size: tuple[int, int],
) -> DataFrame:
    if not available():
        raise FakerPySparkError(
            "distributed generation needs pandas and pyarrow: "
            "pip install 'faker-pyspark[distributed]'"
        )

    options: dict[str, Any] = {
        "seed": seed,
        "locale": locale,
        "providers": list(providers),
        "overrides": dict(overrides or {}),
        "null_ratio": null_ratio,
        "array_size": array_size,
    }
    # Build the generator once here so a bad override fails now, with a clear message,
    # rather than inside an executor.
    RowGenerator(schema, **options)

    indexes = spark.range(0, rows, 1, partitions) if partitions else spark.range(0, rows)
    return indexes.mapInArrow(_batch_function(schema, output_schema, options), output_schema)


def _batch_function(
    schema: T.StructType, output_schema: T.StructType, options: dict[str, Any]
) -> Callable[[Iterator["pa.RecordBatch"]], Iterator["pa.RecordBatch"]]:
    def generate(batches: Iterator["pa.RecordBatch"]) -> Iterator["pa.RecordBatch"]:
        import pyarrow as pa

        generator = RowGenerator(schema, **options)
        arrow_schema = to_arrow_schema(output_schema)
        convert = _arrow_converter(output_schema)
        for batch in batches:
            data = [convert(generator.row(index)) for index in batch.column(0).to_pylist()]
            yield pa.RecordBatch.from_pylist(data, schema=arrow_schema)

    return generate


def to_arrow_schema(schema: T.StructType) -> "pa.Schema":
    """The Arrow schema Spark expects back for ``schema``."""
    import pyarrow as pa

    return pa.schema(
        [pa.field(field.name, _arrow_type(field.dataType), field.nullable) for field in schema]
    )


def _arrow_type(data_type: T.DataType) -> "pa.DataType":
    import pyarrow as pa

    simple: dict[str, pa.DataType] = {
        "StringType": pa.string(),
        "BooleanType": pa.bool_(),
        "ByteType": pa.int8(),
        "ShortType": pa.int16(),
        "IntegerType": pa.int32(),
        "LongType": pa.int64(),
        "FloatType": pa.float32(),
        "DoubleType": pa.float64(),
        "DateType": pa.date32(),
        "TimestampType": pa.timestamp("us", tz="UTC"),
        "TimestampNTZType": pa.timestamp("us"),
        "TimeType": pa.time64("ns"),
        "DayTimeIntervalType": pa.duration("us"),
        "BinaryType": pa.binary(),
        "NullType": pa.null(),
    }
    kind = type(data_type).__name__
    if kind in simple:
        return simple[kind]
    if isinstance(data_type, T.DecimalType):
        return pa.decimal128(data_type.precision, data_type.scale)
    if isinstance(data_type, T.ArrayType):
        return pa.list_(pa.field("element", _arrow_type(data_type.elementType)))
    if isinstance(data_type, T.MapType):
        return pa.map_(_arrow_type(data_type.keyType), _arrow_type(data_type.valueType))
    if isinstance(data_type, T.StructType):
        return pa.struct(
            [pa.field(f.name, _arrow_type(f.dataType), f.nullable) for f in data_type.fields]
        )
    raise FakerPySparkError(
        f"distributed generation does not support {data_type.simpleString()}; "
        "use distributed=False for this schema"
    )


def _arrow_converter(schema: T.StructType) -> Callable[[dict[str, Any]], dict[str, Any]]:
    """Adjust the few Python values Arrow wants in another shape: maps and bytes."""
    converters = {field.name: _value_converter(field.dataType) for field in schema.fields}
    changed = {name: convert for name, convert in converters.items() if convert is not None}
    if not changed:
        return lambda row: row

    def convert_row(row: dict[str, Any]) -> dict[str, Any]:
        for name, convert in changed.items():
            if row[name] is not None:
                row[name] = convert(row[name])
        return row

    return convert_row


def _value_converter(data_type: T.DataType) -> Callable[[Any], Any] | None:
    if type(data_type).__name__ == "BinaryType":
        return bytes
    if isinstance(data_type, T.ArrayType):
        element = _value_converter(data_type.elementType)
        if element is None:
            return None
        return lambda values: [None if v is None else element(v) for v in values]
    if isinstance(data_type, T.MapType):
        key = _value_converter(data_type.keyType) or _same
        value = _value_converter(data_type.valueType) or _same
        return lambda entries: [
            (key(k), None if v is None else value(v)) for k, v in entries.items()
        ]
    if isinstance(data_type, T.StructType):
        return _arrow_converter(data_type)
    return None


def _same(value: Any) -> Any:
    return value
