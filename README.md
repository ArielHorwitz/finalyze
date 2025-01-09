# Finalyze
A bespoke personal financial analysis tool.

Originally a small script meant to parse files exported from the bank website and produce simple graphs for personal financial planning. It has grown in scope to be quite elaborate, but still intended for my own personal financial planning.

## Usage
Install (it is recommended to use a [virtual environment][1]):
```
pip install git+https://github.com/ArielHorwitz/finalyze
```
Then run the entire pipeline:
```
finalyze
```

At the very least, the `ingestion.directories` table in the config must be populated with the path where the raw source files are located.

A minimal config file will be produced automatically if missing. Options are customizable only via the config file. For all possible options, see [config.py](finalyze/config.py).

See also: `finalyze --help`

## Pipeline
The pipeline includes the following three subcommands:
- **Ingest**: Import data from exported files. Supports excel (.xls) file exports from Leumi bank.
- **Tag**: Categorize transactions for later grouping in the analysis.
- **Analyze**: Produce tables to an html file.

Run `finalyze <SUBCOMMAND>` to run only a specific subcommand of the pipeline.

[1]: https://docs.python.org/3/library/venv.html
