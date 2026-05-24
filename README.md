# snakemake-logger-plugin-panoptes

[![PyPI](https://img.shields.io/pypi/v/snakemake-logger-plugin-panoptes.svg)](https://pypi.org/project/snakemake-logger-plugin-panoptes/)
[![Conda](https://img.shields.io/conda/vn/bioconda/snakemake-logger-plugin-panoptes.svg)](https://anaconda.org/bioconda/snakemake-logger-plugin-panoptes)

A [Snakemake 9](https://snakemake.readthedocs.io/) logger plugin that forwards
workflow events to a running [panoptes](https://github.com/panoptes-organization/panoptes)
server, replacing the legacy `--wms-monitor` integration that was removed in
Snakemake 9.

## Requirements

- Python 3.11 or newer
- [Snakemake](https://snakemake.readthedocs.io/) 9 or newer
- A running [panoptes](https://github.com/panoptes-organization/panoptes) server

## Install

```bash
# via PyPI
pip install snakemake-logger-plugin-panoptes

# or via bioconda
conda install -c conda-forge -c bioconda snakemake-logger-plugin-panoptes
```

After installing, Snakemake should advertise the plugin in `--help`:

```bash
snakemake --help | grep logger-panoptes
#   --logger-panoptes-address VALUE
#   --logger-panoptes-timeout VALUE
```

## Use

Start a [panoptes server](https://github.com/panoptes-organization/panoptes#run-the-server),
then run Snakemake with the plugin enabled:

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

For a working example workflow that uses this plugin, see
[snakemake_example_workflow](https://github.com/panoptes-organization/snakemake_example_workflow).

## Development

```bash
pip install -e '.[dev]'   # editable install with test deps (quote .[dev] for zsh)
pytest
```

## License

MIT
