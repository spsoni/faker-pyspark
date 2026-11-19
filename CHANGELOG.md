# Changelog

## [0.10.0](https://github.com/spsoni/faker-pyspark/compare/v0.9.0...v0.10.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* the API is replaced. `pyspark_dataframe` and `pyspark_schema` keep their names with new arguments; `pyspark_update_dataframe`, `pyspark_column_names`, `pyspark_data_dict` and `pyspark_data_dict_using_schema` are removed, as is the `fake_type` metadata key (use `faker`). `pyspark_rows` replaces the `data_dict` methods.

### Features

* schema-driven API for every Spark type ([#12](https://github.com/spsoni/faker-pyspark/issues/12)) ([92aa11d](https://github.com/spsoni/faker-pyspark/commit/92aa11d95a4d71466efd5253d81a99f7b2b7f7a6))


### Bug Fixes

* honour Faker seeding and make random column selection fair ([#10](https://github.com/spsoni/faker-pyspark/issues/10)) ([547dca7](https://github.com/spsoni/faker-pyspark/commit/547dca7812585827ed5372f45bb4cf323f4a9cad))

## [0.9.0](https://github.com/spsoni/faker-pyspark/compare/v0.8.0...v0.9.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* requires Python 3.11 or newer (3.8-3.10 are end-of-life).

### Features

* add a `pyspark` extra (`pip install 'faker-pyspark[pyspark]'`); PySpark stays optional so clusters keep their own Spark ([8bfe0d2](https://github.com/spsoni/faker-pyspark/commit/8bfe0d207ec77fdbc170eb9d777f795bc7997d74))
* add type hints and a `py.typed` marker ([8bfe0d2](https://github.com/spsoni/faker-pyspark/commit/8bfe0d207ec77fdbc170eb9d777f795bc7997d74))
* require Python 3.11+ and modernize packaging, tests and CI ([#3](https://github.com/spsoni/faker-pyspark/issues/3)) ([8bfe0d2](https://github.com/spsoni/faker-pyspark/commit/8bfe0d207ec77fdbc170eb9d777f795bc7997d74))


### Bug Fixes

* declare `faker` as a dependency so a bare `pip install faker-pyspark` can be imported ([8bfe0d2](https://github.com/spsoni/faker-pyspark/commit/8bfe0d207ec77fdbc170eb9d777f795bc7997d74))
* stop random schemas picking the Faker methods `profile` and `null_boolean`, which made `pyspark_schema()` and `pyspark_dataframe()` fail on a few percent of calls ([8bfe0d2](https://github.com/spsoni/faker-pyspark/commit/8bfe0d207ec77fdbc170eb9d777f795bc7997d74))

## 0.8.0

- Made the library compatible with the `faker` command line.
