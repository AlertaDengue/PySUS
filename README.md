# PySUS

[![DOI](https://zenodo.org/badge/63720586.svg)](https://zenodo.org/badge/latestdoi/63720586)
[![release](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml/badge.svg)](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml)
[![Documentation Status](https://readthedocs.org/projects/pysus/badge/?version=latest)](https://pysus.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pysus.svg)](https://pypi.org/project/PySUS/)

PySUS is a Python library for accessing Brazil's public health data (DATASUS).
It downloads, converts, and analyses datasets from four independent sources —
**FTP DataSUS**, **dados.gov.br** (Open Data), **dadosabertos.saude.gov.br**
(OpenDataSUS), and **DuckLake** (S3 mirror) — and exposes them through a
single, DataFrame-first API.

## Key features

- **One-line downloads** — `sinan("DENG", 2024, as_dataframe=True)` returns a
  `pandas.DataFrame` in a single call.
- **Four data sources** — FTP, DadosGov, Saude (OpenDataSUS), DuckLake; the
  orchestrator picks the best route automatically.
- **Data quality** — `missing_values()`, `validate_data()`, `quality_score()`,
  and `profile_report()` give instant insight into completeness and schema
  integrity.
- **Transformation pipeline** — `link_datasets()`, `aggregate_by_state()`,
  `detect_units()`, `rename_columns()`, `optimize_memory()`.
- **Export** — `to_csv()`, `to_excel()`, `to_geojson()`, `to_sql()`, or the
  generic `export()` that auto-detects format.
- **Streaming & DuckDB** — `query_parquet()` runs SQL directly on Parquet via
  DuckDB; `to_arrow()` / `to_df()` convert on the fly.
- **Parallel downloads** — `download_many()` fetches files concurrently with a
  configurable semaphore.
- **Column metadata** — `search_columns()` finds variables by name or
  description across all DATASUS databases.
- **Diff & comparison** — `diff_dfs()`, `diff_summary()`, `diff_rows()` compare
  two DataFrames and surface schema or value differences.
- **Progress bars** — `enable_progress_bars()` / `disable_progress_bars()`.
- **Cache management** — `cache_status()`, `clear_cache()`, `set_cache()`.
- **Friendly errors** — every exception carries a `hint` and optional
  `docs_url`; warnings via `PySUSWarning`.
- **Retry & resume** — `@retry` decorator with exponential back-off;
  `PartialDownload` for HTTP Range resume.
- **Input validation** — `validate_choice()`, `validate_dataset()`,
  `validate_origin()` with fuzzy suggestions on typos.
- **CLI** — Typer-based commands for every operation.
- **Configuration** — TOML file (`pysus.toml`) + environment variables with
  3-tier precedence.
- **Streamlit web UI** — `pysus web` launches a local browser interface.

## Installation

```bash
pip install pysus
```

For the Streamlit web interface:

```bash
pip install pysus[web]
```

### Docker

```bash
docker pull alertadengue/pysus
docker run -p 8888:8888 alertadengue/pysus
```

Or build locally:

```bash
docker compose up --build
# Open http://127.0.0.1:8888/lab
docker compose down
```

## Quick start

### Download a dataset (one-liner)

```python
from pysus import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

# Returns a list of local Parquet paths
parquet_files = sinan(disease="deng", year=2024)

# Get a DataFrame directly
df = sinan(disease="deng", year=2024, as_dataframe=True)

# Multiple years, filtered by state
df = sinasc(state="SP", year=[2020, 2021, 2022, 2023], as_dataframe=True)
```

### Browse available datasets

```python
from pysus import info, search, list_files

info()                          # table of all datasets across all origins
search("sinan")                 # fuzzy search across FTP, Saude, DadosGov
list_files("SINAN")             # list files within a dataset
```

### The PySUS client (full control)

```python
import pysus

async def main():
    async with pysus.PySUS() as client:
        files = await client.query(
            dataset="sinan",
            group="DENG",
            state="SP",
            year=2024,
        )
        for f in files:
            local = await client.download(f)
            print(local.path)

        df = client.read_parquet(
            [str(f.path) for f in files],
            mode="union",
        )
```

Works identically in synchronous code:

```python
from pysus import PySUS

with PySUS() as client:
    files = client.query(dataset="sinan", group="DENG", state="SP", year=2024)
```

### Parallel downloads

```python
from pysus import download_many

paths = await download_many(files, max_concurrent=5)
```

### Streaming / DuckDB

```python
from pysus import query_parquet

# Run SQL directly on Parquet files (no full load into memory)
df = query_parquet("path/to/file.parquet", sql="SELECT * WHERE NU_IDADE > 30")
```

### Data quality

```python
from pysus import missing_values, validate_data, quality_score, profile_report

report = profile_report(df)          # HTML summary
missing = missing_values(df)         # per-column missing counts
score   = quality_score(df)          # 0-100 completeness score
issues  = validate_data(df, rules)   # custom rule validation
```

### Transformation

```python
from pysus import (
    link_datasets,
    aggregate_by_state,
    detect_units,
    optimize_memory,
    rename_columns,
    set_precision,
)

df = optimize_memory(df)              # downcast dtypes, save memory
df = rename_columns(df, mapping)      # rename columns via dict
df = link_datasets(df_a, df_b, keys)  # join by linking keys
```

### Export

```python
from pysus import export, to_csv, to_excel, to_geojson, to_sql

to_csv(df, "output.csv")
to_excel(df, "output.xlsx")
to_geojson(df, "output.geojson", lat="LAT", lon="LON")
to_sql(df, "sqlite:///health.db", table_name="notifications")
export(df, "output.parquet")         # auto-detects format from extension
```

### Diff

```python
from pysus import diff_summary, diff_dfs

diff_summary(df_old, df_new)   # printed summary of schema & value changes
result = diff_dfs(df_old, df_new)  # structured ComparisonResult
```

### Column search

```python
from pysus import search_columns, load_column_metadata

cols = search_columns("dengue")         # search all databases
meta = load_column_metadata("SINAN")    # load schema for one database
```

### Progress bars

```python
from pysus import disable_progress_bars, enable_progress_bars

disable_progress_bars()  # silence tqdm during batch jobs
# ...
enable_progress_bars()
```

### Cache management

```python
from pysus import set_cache, cache_status, clear_cache

set_cache("/data/pysus")          # change cache directory
cache_status()                    # show disk usage and file counts
clear_cache()                     # delete all cached files
```

## CLI commands

```bash
pysus info                  # table of all datasets
pysus search sinan          # search datasets by name
pysus ftp list-datasets     # FTP DataSUS catalog
pysus ftp download SINAN    # download from FTP
pysus dadosgov list         # dados.gov.br catalog (needs DADOSGOV_TOKEN)
pysus saude list-datasets   # OpenDataSUS (dadosabertos.saude.gov.br)
pysus saude show sinan      # show dataset metadata
pysus saude download sinan  # download dataset resources
pysus configure             # interactive setup
pysus cache status          # show cache usage
pysus cache clear           # delete cached files
pysus web                   # launch Streamlit UI
```

## Configuration

### TOML file

Create `pysus.toml` in your project root (or `~/.pysus.toml`):

```toml
[cache]
path = "/data/pysus"

[download]
timeout = 300
max_retries = 3
backoff_base = 1.0

[dadosgov]
token = "your-api-token-here"
```

### Environment variables

| Variable | Purpose | Required |
|----------|---------|----------|
| `PYSUS_CACHEPATH` | Override the default cache directory (`~/pysus`) | No |
| `DADOSGOV_TOKEN` | API token for dados.gov.br downloads | Yes (DadosGov only) |
| `ACCESS_KEY` / `SECRET_KEY` | S3 credentials for catalog sync | No (maintainers) |

Precedence: explicit argument > environment variable > TOML file > default.

## Data sources

| Dataset | Description | FTP | DadosGov | Saude | DuckLake |
|---------|-------------|:---:|:--------:|:-----:|:--------:|
| SINAN | Disease notifications | x | x | x | x |
| SIM | Mortality | x | x | x | x |
| SINASC | Births | x | x | x | x |
| SIH | Hospitalisations | x | | | x |
| SIA | Ambulatory procedures | x | | | x |
| CIHA | Hospital admissions | x | | | x |
| CNES | Health facilities | x | x | x | x |
| PNI | Immunisations | x | x | x | x |
| IBGE | Geographic data | x | | | x |
| COVID19 | COVID-19 confirmed cases | x | x | x | x |
| Arboviroses | Arboviral diseases | | | x | |
| AssistenciaSaude | Health assistance | | | x | |
| AtencaoPrimaria | Primary care | | | x | |
| Vacinacao | Vaccination | | | x | |
| SisAgua | Water surveillance | | | x | |
| Sisvan | Nutritional surveillance | | | x | |

## Architecture

```
pysus
├── api/
│   ├── client.py           PySUS orchestrator (sync + async)
│   ├── errors.py           Error hierarchy with hints
│   ├── retry.py            @retry decorator
│   ├── partial.py          PartialDownload (HTTP Range resume)
│   ├── validate.py         Input validation with suggestions
│   ├── progress.py         tqdm progress bar controls
│   ├── concurrent.py       download_many()
│   ├── cache_utils.py      cache_status / clear_cache
│   ├── streaming.py        query_parquet / to_arrow / to_df
│   ├── flatten.py          JSON column flattening
│   ├── mappings.py         Portuguese → English column names
│   ├── columns.py          Column search
│   ├── export/             CSV / Excel / GeoJSON / SQL exporters
│   ├── diff/               DataFrame comparison
│   ├── quality/            Missing values, validation, profiling, scoring
│   ├── transform/          Linking, aggregation, units, memory, precision
│   ├── metadata/           Column metadata, local cache, schema versioning
│   ├── ftp/                FTP DataSUS client
│   ├── dadosgov/           dados.gov.br API client
│   ├── saude/              OpenDataSUS (dadosabertos.saude.gov.br) client
│   ├── ducklake/           S3/Parquet catalog client
│   └── _impl/              Public re-exports (the pysus.* namespace)
├── cli/                    Typer CLI sub-commands
├── config.py               TOML + env-var configuration
└── web/                    Streamlit web interface
```

## Development

### Setup

```bash
# Conda
conda env create -f conda/dev.yaml
conda activate pysus

# Poetry
poetry install
```

### Tests

```bash
# Unit tests (host)
pytest pysus/tests/

# Unit tests (Docker — recommended for full coverage)
docker compose exec -T -w /usr/src jupyter python3 -m pytest pysus/tests/
```

### Linting

```bash
pre-commit run --all-files
```

Enforced via pre-commit: **black** (80-col), **flake8** (80-col), **isort**
(profile=black, line-length=80), **mypy**, **pyupgrade**.

## License

GPL
