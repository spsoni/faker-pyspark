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

- [x] **Identical data:** executor output equals driver output for every supported type,
  with nulls, in 1, 3 and 7 partitions
  (`test_distributed_matches_driver_for_every_type`). Also run with `TZ` set to
  Australia/Melbourne and America/New_York.
- [x] **Everything reaches the executors:** overrides (method, choices, function), custom
  providers, locale, field metadata, nullability (`tests/test_distributed.py`).
- [x] **Automatic switch** above the threshold; clear error when Arrow is missing.
- [x] **Spark Connect:** `SPARK_REMOTE="local[2]" uv run pytest` on PySpark 4.3: 88 passed,
  100% coverage, the same as on a classic session.
- [x] **Size:** 8-column schema on `local[4]`: 1,000,000 rows on the executors in 48 s
  (20,600 rows/s) with the driver's Python process flat at 119 MB; 100,000 rows take 5 s
  on the executors and 26 s on the driver (3,800 rows/s, 232 MB).
- [x] **README examples executed:** the parquet example with 2,000,000 rows rather than
  the 50,000,000 written (46 s, read back complete); the Connect example as written
  (1,000,000 rows counted in 30 s); both forced modes.
- [ ] `pytest`, `prek`, strict `mypy` clean locally. Still open: CI green including the
  PySpark 3.4 minimum job and the Connect job (no PR yet), and the independent review.

## Notes

- **Bug found by testing off UTC:** an override that returns a time without a zone
  (Faker's `date_time`, for one) for a `timestamp` column gave different instants on the
  driver, which read it in the machine's zone, and on executors, which read it as UTC.
  Such values are now taken as UTC, as the generated ones already were. This touched
  `_generate.py`, which was added to the scope above.
- **Connect on PySpark 3.5 is unverified.** Arrow in Spark 3.5 does not run on Java 21,
  the only JDK on the development machine. The README says Connect is tested with
  PySpark 4.
- **Benchmark caution:** on a machine with under 7 GB of memory, a driver-side run of
  200,000 rows pushed the JVM into swap and stalled for minutes. That is the case this
  story exists for, but it also means the timings above were kept small on purpose.
- **Tests avoid `df.rdd`**, which Connect does not have; partitions are counted with
  `spark_partition_id()`.
