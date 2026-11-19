# Story 003 — schema-api

**Status:** done
**Depends on:** 002

## Goal

Replace the API with one built around the caller's schema: pass a `StructType` or a DDL
string and get realistic data for every Spark type, with column-name matching, per-column
overrides and null ratios. Interpretation: the owner chose "free to break", so this is a
rewrite, not an extension. The old random-schema behaviour survives as one case (no schema
given) rather than being the whole library.

Design decisions:

- **Each row has its own seed**, derived from the run's seed and the row's index. Row 7 is
  the same whether generated first, last or on another machine. This is what Story 004
  needs for distributed generation, so it is built in now.
- **Fill order per column:** override → `faker` metadata on the field → what the column
  name suggests → the column's type.
- **Curated name catalog** (`_catalog.py`) instead of probing all ~330 Faker methods at
  run time. Generic words (`name`, `state`) match only as a whole column name, so
  `product_name` does not get a person's name.
- **Temporal values come from the RNG over a fixed range** (1970–2029), which removes
  Story 002's limit: a seeded run is now identical on any day.
- **Mistakes fail early:** unknown override column, unknown Faker method, value that does
  not fit the type, unsupported type — each raises with the column named.

## Scope — touch ONLY these

- `faker_pyspark/_generate.py`, `_catalog.py`, `errors.py` — new.
- `faker_pyspark/provider.py` — replaces `pyspark.py`.
- `faker_pyspark/__init__.py` — exports.
- `tests/test_generate.py`, `tests/test_provider.py`, `tests/__init__.py` — new;
  `tests/conftest.py` — updated; `tests/test_pyspark.py`, `tests/test_reproducible.py` — removed.
- `README.md` — rewritten for the new API.
- `docs/compass/STATUS.md`, this file.

## Acceptance check (how we verify)

- [x] **Every Spark type:** `test_every_supported_type_gets_a_python_value_of_the_right_kind`
  (engine) and `test_every_supported_type_round_trips_through_spark` (19 types including
  nested arrays, maps and structs, through a real session with nulls).
- [x] **DDL and StructType both accepted;** nullability and metadata preserved.
- [x] **Name matching:** table-driven test of 12 names, plus value checks (emails look like
  emails, ages are 0–100, latitudes −90–90).
- [x] **Overrides** by method, method + kwargs, choices, function; nested and array paths;
  metadata; ten kinds of bad specification each rejected with a clear message.
- [x] **Reproducibility:** hypothesis property (50 seeds × indexes) that a row depends only
  on seed and index; identical DataFrames for one seed through Spark; class-level
  `Faker.seed()` honoured.
- [x] **README examples executed as written** against a real session; output inspected.
- [x] **Whole suite:** 71 passed, 100% coverage, repeated runs, about 11 s.
- [x] `prek`, strict `mypy` clean; CI green including the PySpark 3.4 minimum job.

## Notes

- **Bugs the tests found while writing this:** a name-based numeric range that did not fit
  a small column (`salary decimal(4,2)`, `salary tinyint`) crashed; Spark refuses to build
  a DataFrame from Python rows when the schema names `char`/`varchar`, so those are passed
  as strings with the declared type in metadata, exactly as Spark does for tables.
- **Bug found by running the README examples:** the default locale was inferred as Latin
  (from Faker's lorem provider), giving British postcodes and Latin words for a plain
  `Faker()`. The unit tests had not caught it. Locale is now read from the address and
  person providers, with regression tests.
- **Throughput:** about 7,000 rows/s per core for an 8-column schema. Faker itself is the
  cost; per-row seeding is not measurable against it.
- **Not supported:** year-month and calendar intervals, variant, geometry/geography and
  user-defined types have no Python value `createDataFrame` accepts across the supported
  versions. They raise `UnsupportedTypeError`; an override resolves it.
- **Locale inference** reads the locale Faker recorded on each provider. An explicit
  `locale=` always wins.
- Removed from the public API: `pyspark_update_dataframe`, `pyspark_column_names`,
  `pyspark_data_dict`, `pyspark_data_dict_using_schema`, `get_spark`, `fake_fields_list`,
  and the `fake_type` metadata key. Update-style data returns in Story 005.
