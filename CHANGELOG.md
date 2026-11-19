# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- **Breaking:** requires Python 3.11+ (3.8–3.10 are end-of-life).
- `faker` is now a declared dependency. Previously nothing was declared, so a bare
  `pip install faker-pyspark` could not be imported.
- PySpark stays out of the required dependencies, so installing on a cluster that ships
  its own Spark does not replace it. Use `pip install 'faker-pyspark[pyspark]'` to pull it
  in. Tested against PySpark 4.3 and Faker 40.
- Packaging moved from Poetry to PEP 621 metadata, the hatchling build backend and uv.

### Added
- Type hints throughout, plus a `py.typed` marker.
- A wider test suite, ruff, mypy (strict), pre-commit, Dependabot, CI on Python 3.11–3.14,
  and a trusted-publishing release workflow.
- `CONTRIBUTING.md`, `SECURITY.md` and this changelog.

### Fixed
- Random schemas no longer pick the Faker methods `profile` and `null_boolean`. Spark
  cannot infer a column type from their values, which made `pyspark_schema()` and
  `pyspark_dataframe()` fail intermittently (about one call in four with the defaults).

## [0.8.0]

- Made the library compatible with the `faker` command line.
