# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A **Snakemake 9 logger plugin** that forwards workflow events to a running [panoptes](https://github.com/panoptes-organization/panoptes) server. It is the producer side of the panoptes monitoring integration and replaces the `--wms-monitor` flag that Snakemake 9 removed. Published as `snakemake-logger-plugin-panoptes` on PyPI/bioconda.

## Commands

```bash
pip install -e '.[dev]'          # dev install (hatchling build backend)
pytest -v                        # run the unit tests
pytest tests/test_plugin.py::test_close_reports_workflow_success   # a single test
```

Tests **stub the HTTP layer** (`_FakeSession` in `tests/test_plugin.py`) and fabricate `LogRecord`s, so the whole suite runs with **no Snakemake and no live panoptes server**. Prefer adding cases there over end-to-end runs.

Discoverability / wiring check (needs `pip install 'snakemake>=9'`):
```bash
snakemake --help | grep logger-panoptes      # flags should appear once installed
python -c "from snakemake_logger_plugin_panoptes import LogHandler, LogHandlerSettings"
```

Real end-to-end validation is done from the panoptes side (its example workflow / e2e tests), not here.

## Architecture

Snakemake discovers this plugin by package-name convention (`snakemake_logger_plugin_<name>` → `--logger panoptes`). Two source files:

- **`src/snakemake_logger_plugin_panoptes/__init__.py`** — the Snakemake-facing surface:
  - `LogHandlerSettings` — the `--logger-panoptes-*` CLI flags (`address` (required), `timeout`, `workflow_id`), each also bindable via `SNAKEMAKE_LOGGER_PANOPTES_*` env vars.
  - `LogHandler` — the entry-point class (subclasses `LogHandlerBase` + `PanoptesLogHandler`). The Snakemake runtime populates `self.settings` and `self.common_settings` **before** `__post_init__`, which then constructs the underlying handler. The several `writes_to_*` / `has_*` / `needs_rulegraph` properties are the capability flags `LogHandlerBase` requires.

- **`src/snakemake_logger_plugin_panoptes/log_handler.py`** — `PanoptesLogHandler`, a `logging.Handler`, where all real logic lives:
  - `_ensure_workflow()` registers the run via `GET /create_workflow` (optionally `?workflow_id=`), caching the numeric id. **Returns early for dry runs** (`common_settings.dryrun`), so a dry run creates nothing on the server.
  - `emit()` dispatches on `record.event` (a Snakemake `LogEvent`) through `_DISPATCH` to `_on_<event>` translator methods. Each returns a JSON-able dict that is `POST`ed to `/update_workflow_status` (or `None` to skip).
  - `close()` sends a `workflow_success` event at end-of-run — but only for a real run that registered a workflow and saw no error, and at most once. This is what lets panoptes mark `--until` runs `Done` (Snakemake reports the full-DAG total, so `done` never reaches `total`).

**The output is a contract with the panoptes server.** Every dict `emit()`/`close()` produces must match a `level` that panoptes' `maintain_jobs` understands (`job_info`, `job_started`, `job_finished`, `job_error`, `progress`, `error`, `workflow_success`, `shellcmd`, `info`). Changing or adding an event here usually needs a coordinated change in the panoptes repo, released together.

## Conventions / gotchas

- **`emit()` must never raise into Snakemake.** It wraps translation in `try/except` and routes failures to `handleError`; network calls log warnings rather than raise. A monitoring plugin must never crash a workflow.
- **Version lives in `pyproject.toml` `[project].version`.** Bump per change; use a minor bump when adding/altering emitted events (a new event is a feature).
- Translator methods build payloads defensively via `getattr(record, ...)` and `_to_jsonable` / `_resources_to_dict`, because Snakemake's `LogRecord` attributes and types (Namedlist/IOFile/Wildcards) vary by event.
- `_errored` (set on `JOB_ERROR`/`ERROR`) and the dry-run check together gate whether `close()` reports success.

## Sibling repo

The consumer of these events is **`panoptes`** (`../panoptes`, org `panoptes-organization`, default branch `master`) — its `panoptes/server_utilities/db_queries.py::maintain_jobs` is the matching event handler. There is also `../snakemake_example_workflow`, a reference pipeline used to exercise both together.
