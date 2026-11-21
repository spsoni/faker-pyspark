# faker-pyspark

A Faker provider that generates PySpark DataFrames and schemas. Vision and roadmap:
`PLAN.md`.

## How work is planned

This repo uses **Compass** (`docs/compass/`) for feature arcs: a written story is approved
before code, and the build touches only the files the story names. Status board:
`docs/compass/STATUS.md`. Small fixes and chores need no story.

## Checks

All of these must pass before a change is done; CI runs the same ones.

```bash
uv sync
uv run pytest                      # starts a local Spark session; needs a JDK (17 or 21)
SPARK_REMOTE="local[2]" uv run pytest   # the same suite over Spark Connect
uv run prek run --all-files        # ruff, typos, zizmor, actionlint, file checks
uv run mypy faker_pyspark tests    # strict
```

## Conventions

- DataFrame API only, in the library and the tests: no `.rdd`, no `sparkContext`, no JVM
  access. Spark Connect has none of them.
- Tests must hold for every random draw. Re-run a new test several times before trusting it.
- Commits that land on `main` use Conventional Commits; release-please derives the version
  and `CHANGELOG.md` from them, so never edit those by hand. `feat`/`fix` trigger a release.
- GitHub Actions are pinned to commit SHAs with a `# vX.Y.Z` comment.
- Dependencies resolve with a 7-day cooldown (`exclude-newer` in `pyproject.toml`).
