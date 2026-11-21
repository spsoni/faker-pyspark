# Story 004 — scale

**Status:** in progress
**Depends on:** 003

## Goal

Generate rows on the executors instead of the driver, so the size of a DataFrame is limited
by the cluster and not by the driver's memory, and make the whole library work over Spark
Connect (which is what serverless Spark offers). Interpretation: "scale" means the same
data, produced elsewhere. A seeded run must give identical rows whether they are built on
the driver, on executors, in 1 partition or in 50, over a classic session or over Connect.

Design decisions:

- **`spark.range(rows).mapInArrow(...)`**: Spark hands each executor a slice of row
  indexes and the executor builds those rows. Story 003 already seeds every row from
  (run seed, row index), so no coordination between executors is needed.
- **DataFrame API only**, no RDDs and no JVM access, because Connect has neither.
- **Automatic above 100,000 rows** when pandas and pyarrow are installed; `distributed=`
  forces it either way. Below that, starting Python workers costs more than it saves.
- **pandas and pyarrow are an extra** (`faker-pyspark[distributed]`), not a requirement:
  small test suites should not need them.
- **Mistakes still fail on the driver.** The generator is built once before the job is
  submitted, so a bad override raises immediately rather than inside an executor.
- **The suite runs twice in CI**: on a classic session, and on a Connect session selected
  by the `SPARK_REMOTE` environment variable (the two cannot share a process).

## Scope — touch ONLY these

- `faker_pyspark/_distributed.py` — new.
- `faker_pyspark/provider.py` — `distributed` and `partitions` arguments.
- `faker_pyspark/_generate.py` — only the zone of timestamps that come from overrides
  (added to scope during the build; see Notes).
- `pyproject.toml`, `uv.lock` — `distributed` extra; Connect client packages for tests.
- `tests/test_distributed.py` — new. `tests/conftest.py`, `tests/test_provider.py`,
  `tests/test_generate.py` — only what a Connect session needs.
- `.github/workflows/python-package.yml` — Connect job.
- `README.md` — "Large DataFrames" and Spark Connect sections.
- `CONTRIBUTING.md`, `CLAUDE.md` — how to run the suite over Connect.
- `docs/compass/STATUS.md`, this file.

## Acceptance check (how we verify)

- [ ] **Identical data:** executor output equals driver output for every supported type,
  with nulls, in 1, 3 and 7 partitions.
- [ ] **Everything reaches the executors:** overrides (method, choices, function), custom
  providers, locale, field metadata, nullability.
- [ ] **Automatic switch** above the threshold; clear error when Arrow is missing.
- [ ] **Spark Connect:** the whole suite passes on a Connect session.
- [ ] **Size:** a run larger than the driver path could comfortably hold, timed.
- [ ] **README examples executed as written.**
- [ ] `pytest`, `prek`, strict `mypy` clean; CI green including the PySpark 3.4 minimum
  job and the Connect job.

## Notes
