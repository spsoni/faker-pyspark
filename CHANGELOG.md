# Changelog

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
