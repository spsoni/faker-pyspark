# Story 002 — reproducible-core

**Status:** done
**Depends on:** 001

## Goal

A seeded Faker gives the same schema and the same data every time, and random column
selection is fair. Interpretation: fix the three defects found in review of 0.9.0 inside
the existing API, without redesigning it (that is Story 003):

1. Randomness came from the global `random` module, so `Faker.seed()` had no effect.
2. Column names were drawn with replacement from a list where 21 list/dict-returning
   methods held 125 of 330 slots.
3. `max_columns` was never reached, because the count was `int(uniform(min, max))`.

## Scope — touch ONLY these

- `faker_pyspark/pyspark.py` — use the Faker instance's own RNG; de-duplicate and sort the
  candidate list; sample without replacement; inclusive bounds with validation; cache the
  method probe without disturbing the RNG.
- `tests/test_reproducible.py` — new; seeding and selection properties.
- `tests/test_pyspark.py` — one bound that moved because `max_columns` is now reachable.
- `pyproject.toml`, `uv.lock` — `hypothesis` in the dev group.
- `docs/compass/STATUS.md`, this file.

## Acceptance check (how we verify)

- [x] **Seeding works:** `test_same_seed_gives_same_schema_and_data` (three seeds) and
  `test_class_level_seed_is_honoured` pass; `test_different_seeds_give_different_column_names`
  shows the seed matters.
- [x] **Fair selection:** `fake_fields_list` returns sorted, unique names; a hypothesis
  property over random seeds and bounds shows names are distinct and the count is within
  the inclusive bounds; `test_column_count_reaches_both_bounds` sees 2, 3 and 4 for
  `min=2, max=4`.
- [x] **Bad bounds rejected** with `ValueError`.
- [x] **Whole suite:** `uv run pytest` — 26 passed, 99% coverage, six consecutive runs,
  about 11 s each.
- [x] `uv run prek run --all-files` and `uv run mypy faker_pyspark tests` clean; CI green.

## Notes

- **Known limit, carried to Story 003:** Faker anchors most date and time methods to the
  current clock, so those column *values* still differ between two runs with one seed. The
  seeded-data test compares every non-temporal value. Story 003 generates temporal values
  from the RNG over a fixed range, which removes the limit.
- **Why the probe is cached:** deciding which Faker methods are usable means calling all
  ~330 once. Doing that on every call took the suite from 10 s to 112 s. The probe now runs
  once per provider set and restores the RNG state afterwards, so it is invisible to seeding.
- `pytest-randomly` was considered and left out: hypothesis already varies the seed, and
  random test order adds nothing with one shared Spark session.
- Behaviour change for users: `pyspark_schema()` can now return `max_columns` columns plus
  the uid (previously one fewer), and column names no longer repeat.
