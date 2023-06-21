import random
from collections import OrderedDict
from faker.providers import BaseProvider
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType


def get_spark(app_name:str = 'faker_pyspark') -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()


def fake_fields_list(generator, allow_complex_types: bool = True):
    fields = list()
    nested_fields = list()
    for cls in BaseProvider.__subclasses__():
        if cls.__name__ in ('PySparkProvider', ):
            continue
        for attr in dir(cls):
            if (
                    attr in ["seed", "seed_instance", "random", "enum", "image", "xml", "date_time_ad"]
                    or attr.startswith("random_digit_")
                    or attr.startswith("py")
                    or not callable(getattr(cls, attr))
                    or attr.startswith("_")
                    or not attr.islower()
                    or not hasattr(generator, attr)
                    or attr in fields):
                # case when attr is not a provider method
                continue

            val = generator.format(attr)
            val_type = val.__class__.__name__
            if val_type.lower() not in (
                    'str', 'bool', 'float', 'int', 'decimal', 'datetime',
                    'date', 'list', 'tuple', 'dict', 'set'):
                # we do not allow other complex types
                continue

            if val_type.lower() in ('list', 'tuple', 'dict'):
                nested_fields.append(attr)
            else:
                fields.append(attr)

    if allow_complex_types:
        return fields + nested_fields

    return fields


class PySparkProvider(BaseProvider):
    """
    A Provider for PySpark based test data.

    >>> from faker import Faker
    >>> from faker_pyspark import PySparkProvider
    >>> fake = Faker()
    >>> fake.add_provider(PySparkProvider)
    """

    def pyspark_dataframe(
            self,
            spark: SparkSession = None,
            schema: StructType = None,
            rows: int = 4,
            min_columns: int = 2,
            max_columns: int = 10,
            allow_complex_types: bool = True,
            *args, **kwargs) -> DataFrame:
        # Returns a random PySpark DataFrame
        if spark is None:
            app_name = kwargs.get('app_name', 'faker_pyspark')
            spark = get_spark(app_name)

        if schema is None:
            schema = self.pyspark_schema(spark, min_columns, max_columns, allow_complex_types)

        records = [self.pyspark_data_dict_using_schema(schema=schema) for _ in range(rows)]
        return spark.createDataFrame(records, schema=schema)

    def pyspark_update_dataframe(self, df: DataFrame, retain_columns: list = None):
        rows = df.collect()
        random_rows = random.choices(rows)
        updated_rows = list()
        if retain_columns is None:
            retain_columns = list()

        for row in random_rows:
            data = row.asDict()
            updated_data = self.pyspark_data_dict_using_schema(df.schema)

            for retain_column in retain_columns:
                updated_data[retain_column] = data[retain_column]

            updated_rows.append(updated_data)

        spark = df.sparkSession

        return spark.createDataFrame(updated_rows, schema=df.schema)

    def pyspark_column_names(
            self,
            min_columns: int = 3,
            max_columns: int = 10,
            allow_complex_types: bool = False) -> list:
        cols_count = int(random.uniform(min_columns, max_columns))
        fields = fake_fields_list(self.generator, allow_complex_types)
        return random.choices(fields, k=cols_count)

    def pyspark_data_dict_using_schema(self, schema: StructType) -> dict:
        data = OrderedDict()

        for field in schema.fields:
            name = field.name
            if field.metadata and 'fake_type' in field.metadata:
                val = self.generator.format(field.metadata['fake_type'])
            else:
                val = self.generator.format(name)
            data[name] = val

        return data

    def pyspark_data_dict(
            self,
            min_columns: int = 3,
            max_columns: int = 10,
            nested_allowed: bool = False,
            uid_column_name: str = None) -> dict:
        data = OrderedDict()

        if uid_column_name is not None:
            data[uid_column_name] = self.generator.uuid4()

        columns = sorted(self.pyspark_column_names(min_columns, max_columns, nested_allowed))
        for column in columns:
            key = column + self.generator.bothify('__#??').lower()
            data[key] = self.generator.format(column)

        return data

    @staticmethod
    def _add_metadata_to_schema(schema: StructType, uid_column_name: str = None) -> StructType:
        for field in schema.fields:
            if uid_column_name and field.name == uid_column_name:
                field.metadata = dict(fake_type='uuid4', hash_it=True)
            elif '__' not in field.name:
                field.metadata = dict(fake_type=field.name, hash_it=True)
            else:
                fake_type, _ = field.name.rsplit('__', 1)
                field.metadata = dict(fake_type=fake_type, hash_it=True)
        return schema

    def pyspark_schema(
            self,
            spark: SparkSession = None,
            min_columns: int = 3,
            max_columns: int = 10,
            allow_complex_types: bool = True,
            uid_column_name: str = 'uid') -> StructType:
        if spark is None:
            spark = get_spark()

        data = self.pyspark_data_dict(min_columns, max_columns, allow_complex_types, uid_column_name)
        sorted_columns = list(sorted(data.keys()))

        if uid_column_name:
            sorted_columns.remove(uid_column_name)
            sorted_columns.insert(0, uid_column_name)

        schema = spark.createDataFrame([data, ]).select(sorted_columns).schema
        return self._add_metadata_to_schema(schema, uid_column_name)
