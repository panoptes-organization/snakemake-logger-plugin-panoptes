# snakemake-logger-plugin-panoptes

A [Snakemake 9](https://snakemake.readthedocs.io/) logger plugin that forwards
workflow events to a running [panoptes](https://github.com/panoptes-organization/panoptes)
server, replacing the legacy `--wms-monitor` integration that was removed in
Snakemake 9.

## Install

```bash
pip install snakemake-logger-plugin-panoptes
```

## Use

Start a panoptes server (see the panoptes README), then run Snakemake with the
plugin enabled:

```bash
snakemake \
    --cores 1 \
    --logger panoptes \
    --logger-panoptes-address http://127.0.0.1:5000
```

You can also point at the address via an environment variable:

```bash
export SNAKEMAKE_LOGGER_PANOPTES_ADDRESS=http://127.0.0.1:5000
snakemake --cores 1 --logger panoptes
```

### Settings

| Flag | Env var | Default | Description |
| --- | --- | --- | --- |
| `--logger-panoptes-address` | `SNAKEMAKE_LOGGER_PANOPTES_ADDRESS` | _(required)_ | Base URL of the panoptes server. |
| `--logger-panoptes-timeout` | — | `10.0` | Per-request HTTP timeout in seconds. |

## How it works

On workflow start the plugin calls `GET /create_workflow` to register a new
workflow with the panoptes server and remembers the returned workflow id. It
then translates Snakemake [`LogEvent`](https://github.com/snakemake/snakemake-interface-logger-plugins)
records (`JOB_INFO`, `JOB_STARTED`, `JOB_FINISHED`, `JOB_ERROR`, `SHELLCMD`,
`PROGRESS`, `ERROR`, `RUN_INFO`) into the JSON message format that panoptes'
`/update_workflow_status` endpoint already understands.

Network errors are logged but never crash the workflow.

## Development

```bash
pip install -e .[dev]   # editable install with test deps
pytest
```

## License

MIT
