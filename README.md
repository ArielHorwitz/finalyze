# Finalyze
A bespoke personal financial analysis tool.

Originally a small script meant to parse files exported from the bank website and produce simple graphs for personal financial planning. It has grown in scope to be quite elaborate, but still intended for my own personal financial planning.

## Usage
Install with [uv][uv] (recommended):
```
uv tool install git+https://github.com/ArielHorwitz/finalyze
```
Or via pip (in which case it is recommended to use a [virtual environment][venv]):
```
pip install git+https://github.com/ArielHorwitz/finalyze
```
Then run the entire pipeline:
```
finalyze
```

At the very least, the `ingestion.directories` table in the config must be populated with the path where the raw source files are located.

A config file with default values will be produced automatically if missing. Options are customizable only via the config file. For all possible options and some explanation, see [config.py](finalyze/config.py).

See also: `finalyze --help`

## Pipeline
The pipeline includes the following three subcommands:
- **Ingest**: Import data from exported files. Supports excel (.xls) file exports from Leumi bank.
- **Tag**: Categorize transactions for later grouping in the analysis.
- **Analyze**: Produce tables to an html file.

Run `finalyze <SUBCOMMAND>` to run only a specific subcommand of the pipeline.

## Development
Dependency management via [uv][uv]:
```
uv run finalyze
```

Linting and formatting using [bacon][bacon] (press 'f' to format):
```
bacon
```

Or manually:
```
./scripts/lint.sh
./scripts/format.sh --check
./scripts/format.sh
```

[venv]: https://docs.python.org/3/library/venv.html
[uv]: https://github.com/astral-sh/uv
[bacon]: https://github.com/Canop/bacon/
