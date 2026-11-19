# faker-pyspark — plan

## What we are building

The best PySpark plugin for [Faker](https://faker.readthedocs.io/): give it a schema, get
realistic, reproducible test data as a Spark DataFrame, at any size.

Today the library only produces random-schema DataFrames, builds every row on the driver,
and cannot be seeded. The roadmap closes those gaps. The API is free to break on the way
to 1.0; a migration guide ships with it.

## Roadmap

| # | Story | What it makes true |
|---|---|---|
| 001 | Tooling trial | Newer dev tools adopted or rejected on evidence, with a scorecard to reuse elsewhere |
| 002 | Reproducible core | `Faker.seed()` gives identical output; skewed column choice and the unreachable `max_columns` fixed; property-based tests |
| 003 | New API + bring-your-own-schema | Clean redesigned API (breaking). Pass a `StructType` or DDL string and get realistic data for every Spark type, with column-name matching, per-column overrides, null ratios |
| 004 | Scale | Rows generated on executors, not the driver; works on Spark Connect / serverless |
| 005 | Test ergonomics | Unique/primary keys, related tables with foreign keys, locales, optional pytest fixtures |
| 006 | Docs site and 1.0 | Documentation site, migration guide from 0.x, 1.0 release |

Order: 002 is small and everything depends on deterministic generation; 003 fixes the API
shape before 004 and 005 build on it.

Stories live in `docs/compass/stories/`; what is in flight is on `docs/compass/STATUS.md`.
