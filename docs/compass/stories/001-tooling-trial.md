# Story 001 — tooling-trial

**Status:** verifying
**Depends on:** none

## Goal

Replace or add dev tools where a newer one is clearly better, decided by evidence gathered
in this repo, and leave a scorecard that says what to roll out to the owner's other Python
projects. No library behaviour changes: `faker_pyspark/` is not touched, and no release is
cut (all commits are `build:`/`ci:`/`chore:`/`docs:`, which release-please ignores).

Reading of "best tool alternatives": tools that are faster or safer **and** stable enough
to depend on, not simply the newest.

## Scope — touch ONLY these

- `PLAN.md`, `CLAUDE.md`, `docs/compass/STATUS.md`, this file — new.
- `pyproject.toml`, `uv.lock` — build backend, dev group, cooldown.
- `.pre-commit-config.yaml` — `typos`, `zizmor`, `actionlint` hooks.
- `.github/workflows/python-package.yml`, `codeql.yml`, `release.yml` — SHA pins, lint job.
- `.github/dependabot.yml` — cooldown, grouped action updates.
- `CONTRIBUTING.md` — commands.

Outside the repo: `uv self update` on dev01; close PRs #2 and #4–#7.

## Outcome — scorecard (2026-11-19)

| Area | Verdict | Evidence |
|---|---|---|
| Pre-commit runner | **Adopted `prek` 0.5.10** | Read the existing config unchanged. All nine hooks ran in 6.7 s cold, including installing three new hook environments. |
| Build backend | **Adopted `uv_build`** | Wheel has the same files as the hatchling wheel and a byte-identical `pyspark.py`. See the notes on metadata and sdist below. |
| Actions hygiene | **Adopted `zizmor` 1.32 + `actionlint` 1.7.12** | zizmor found 21 unpinned actions and 6 checkouts that left credentials on disk. All fixed; now zero findings. actionlint found nothing. |
| Supply-chain cooldown | **Adopted** `exclude-newer = "7 days"` | `uv.lock` records `exclude-newer-span = "P7D"`. It immediately held ruff at 0.17.4 instead of the two-day-old 0.17.5. |
| Spell check | **Adopted `typos` 1.52** | Clean on first run; no configuration needed. |
| Type checker | **Kept `mypy` 2.4.1 (strict)** | See the comparison below. Neither candidate met the rule. |
| Vulnerability audit | **`uv audit` works**; kept non-blocking | "Found no known vulnerabilities and no adverse project statuses in 20 packages". It is still a preview command, so it reports in CI but cannot fail the job. |

### Type-checker comparison

Rule set before the trial: a candidate replaces mypy only if it catches every seeded
mistake mypy catches and raises no false positives on the repo.

Fifteen deliberate mistakes in one file (missing annotations, wrong return type, `None`
misuse, bad PySpark argument, untyped py4j access, bare generics, missing attribute,
`Any` returned from a typed function, unused `type: ignore`, implicit `Optional`, bad
assignment, unreachable code, call to an untyped function, too many arguments, redundant
cast), each checker on its strictest setting:

| | mypy 2.4.1 `--strict` | pyrefly 1.4.1 `--preset strict` | ty 0.0.101 `all = "error"` |
|---|---|---|---|
| Seeded mistakes flagged as errors | **15 / 15** | 10 / 15 | 12 / 15 |
| Missed | none | `Any` return, unused ignore, unreachable code, untyped call; redundant cast only as a warning | missing annotations, unused ignore, untyped call |
| False positives on seeded file | 0 | 0 | 1 (`SparkSession.sparkContext` "may be missing") |
| Findings on this repo | 0 | 0 | 4: two wrong (`SparkSession.builder` resolves to `Unknown`), two style opinions on `if uid_column_name:` |
| Wall time on this repo | 3.4 s | 0.24 s | 0.10 s |

Both candidates are more than ten times faster, which does not matter at this size. pyrefly
is quiet and correct but misses a third of what strict mypy enforces. ty cannot require
annotations and mis-resolves a PySpark class property. **Decision: mypy stays; both
candidates were removed from the dev dependencies.** Worth re-running when ty reaches 1.0.

### Notes on `uv_build`

- `METADATA` differs from hatchling's only in an added `Author` field, keyword order, and
  `Metadata-Version` 2.4 instead of 2.5. Dependencies, extras, classifiers and URLs match.
- The sdist carries a normalized `pyproject.toml` plus the original as
  `pyproject.toml.orig`. That is how `uv_build` works, not a stray file.
- The sdist no longer includes `.gitignore`. Tests and docs are included through
  `source-include`.
- `requires` is capped at `<0.14`, so it needs a bump with each uv minor release.
  Dependabot handles that.

### What to roll out to other projects

Adopt as-is: `prek`, `zizmor` + `actionlint` with SHA-pinned actions, the 7-day cooldown in
both uv and Dependabot, `typos`. Adopt for pure-Python packages only: `uv_build`. Do not
switch type checker yet.

## Acceptance check (how we verify)

- [x] **Build parity:** wheel file lists identical, `pyspark.py` sha256 identical
  (`d81b31eb…`), metadata differences as listed above. Installed the new wheel with the
  `pyspark` extra in a clean Python 3.12 venv; `python -m faker pyspark_column_names -i
  faker_pyspark` printed `['building_number', 'free_email', 'job_female', 'uuid1']`.
- [x] **Hooks:** `uv run prek run --all-files` — all nine hooks Passed.
- [x] **Actions:** zizmor hook Passed with zero findings; every `uses:` is a SHA with a
  version comment. `actions/setup-java` is pinned to v6.2.0, not v6.3.0, because v6.3.0 was
  five days old.
- [x] **Cooldown:** `uv lock` succeeded; lock records `exclude-newer-span = "P7D"`.
- [x] **Type-checker scorecard:** above.
- [x] **`uv audit`:** works; output above.
- [ ] **No behaviour change:** `git diff main -- faker_pyspark/` empty; `uv run pytest` passes.
- [ ] **CI green** on the PR.
- [ ] **No release triggered** after merge.

## Notes

- PyPI publishing is still blocked on the owner's account recovery; see `STATUS.md`.
- `hypothesis` and `pytest-randomly` are deferred to Story 002, Zensical docs to Story 006.
  `deptry` (alpha, no release in eight months) and `pytest-xdist` (one Spark JVM per
  worker) were skipped.
