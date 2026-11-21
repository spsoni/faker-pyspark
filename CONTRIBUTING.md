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
uv run prek install           # run the hooks on every commit
```

[prek](https://prek.j178.dev) is a faster drop-in replacement for pre-commit and reads the
same `.pre-commit-config.yaml`.

## Checks

CI runs all of these, and a pull request needs them to pass:

```bash
uv run pytest                          # tests + coverage (local Spark session)
SPARK_REMOTE="local[2]" uv run pytest  # the same tests over Spark Connect
uv run prek run --all-files            # ruff lint + format, typos, zizmor, actionlint
uv run mypy faker_pyspark tests        # strict type checking
```

Spark Connect has no RDDs and no JVM on the client, so library code and tests use the
DataFrame API only.

Dependencies resolve with a seven-day cooldown (`exclude-newer` in `pyproject.toml`), so a
release published in the last week is not picked up. GitHub Actions are pinned to commit
SHAs; Dependabot keeps both current.

## Guidelines

- **Add tests** for bug fixes and features, and keep coverage where it is.
- **Tests must be deterministic in outcome.** The provider generates random schemas, so
  assert on properties that hold for every draw, and re-run a new test several times
  before trusting it.
- **Document user-facing changes** in `README.md`.
- **Use [Conventional Commits](https://www.conventionalcommits.org/)** for anything that
  lands on `main` (pull requests are squash-merged, so the PR title is what counts):
  `fix: ...` for a patch release, `feat: ...` for a minor one, and `feat!: ...` or a
  `BREAKING CHANGE:` footer for a breaking change. The changelog and the next version
  number are generated from these, so do not edit `CHANGELOG.md` or the version by hand.
- **Public API:** anything exported from `faker_pyspark/__init__.py`, plus the
  `pyspark_*` provider methods. Avoid breaking it without a good reason, and call breaks
  out in the changelog.

## Releasing

Releases are automated with [release-please](https://github.com/googleapis/release-please).

1. Every push to `main` updates a release pull request titled `chore(main): release X.Y.Z`,
   containing the version bump and the generated changelog.
2. Merging that pull request tags `vX.Y.Z` and creates the GitHub release. The same
   *Release* workflow run then runs the tests, builds the package and publishes it to PyPI
   through trusted publishing. No token is needed.
