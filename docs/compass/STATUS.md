# Status board — faker-pyspark

> ≤20 lines, always. In-flight + next + blocked. NOT a history: story detail lives
> in the story files, the roadmap in `PLAN.md`.

## Done
001: tooling trial (see `stories/001-tooling-trial.md` for the scorecard).

## Now
Nothing in flight.

## Next
1. **002** — reproducible core: `Faker.seed()` works, column-selection bugs fixed, property-based tests.
2. **003** — new API + bring-your-own-schema.

## Blocked / decisions needed
- **PyPI publishing** — owner locked out of PyPI (recovery request raised 2026-11-19).
  `v0.9.0` is tagged on GitHub but not on PyPI. Once access is back: add a trusted
  publisher (owner `spsoni`, repo `faker-pyspark`, workflow `release.yml`, environment `pypi`).
