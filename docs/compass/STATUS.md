# Status board — faker-pyspark

> ≤20 lines, always. In-flight + next + blocked. NOT a history: story detail lives
> in the story files, the roadmap in `PLAN.md`.

## Done
001 tooling trial, 002 reproducible core, 003 new schema-driven API (see `stories/`).

## Now
**004** — scale: executor-side generation, Spark Connect.

## Next
1. **005** — test ergonomics: keys, related tables, pytest fixtures.
2. **006** — docs site and 1.0.

## Blocked / decisions needed
- **PyPI publishing** — owner locked out of PyPI (recovery request raised 2026-11-19).
  `v0.9.0` is tagged on GitHub but not on PyPI. Once access is back: add a trusted
  publisher (owner `spsoni`, repo `faker-pyspark`, workflow `release.yml`, environment `pypi`).
  Until then the release-please PR is left open, not merged.
