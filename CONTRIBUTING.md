# Contributing

Thanks for your interest in improving `faker-pyspark`! Bug reports, docs fixes and pull
requests are all welcome.

## Development setup

The project uses [uv](https://docs.astral.sh/uv/) for environments and dependencies. The
tests start a local Spark session, so you also need a JDK (17 or 21) on your `PATH`.

```bash
git clone https://github.com/spsoni/faker-pyspark.git
cd faker-pyspark
uv sync                       # creates .venv with the dev dependencies, including PySpark
uv run pre-commit install     # run ruff on every commit
```

## Checks

CI runs all of these, and a pull request needs them to pass:

```bash
uv run pytest                          # tests + coverage (local Spark session)
uv run ruff check .                    # lint
uv run ruff format --check .           # formatting
uv run mypy faker_pyspark tests        # strict type checking
```

## Guidelines

- **Add tests** for bug fixes and features, and keep coverage where it is.
- **Tests must be deterministic in outcome.** The provider generates random schemas, so
  assert on properties that hold for every draw, and re-run a new test several times
  before trusting it.
- **Document user-facing changes** in `README.md` and add an entry under *Unreleased* in
  `CHANGELOG.md`.
- **Public API:** anything exported from `faker_pyspark/__init__.py`, plus the
  `pyspark_*` provider methods. Avoid breaking it without a good reason, and call breaks
  out in the changelog.

## Releasing

1. Bump the version (`uv version --bump minor`, or edit `pyproject.toml`), and move the
   *Unreleased* changelog entries under it with today's date.
2. Merge to `main`.
3. Publish a GitHub release tagged `vX.Y.Z`, for example
   `gh release create vX.Y.Z --generate-notes`. The *Release* workflow checks that the tag
   matches the package version, runs the tests, and publishes to PyPI through trusted
   publishing. No token is needed.
