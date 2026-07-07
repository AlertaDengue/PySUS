# PySUS 2.0 is now available!

[![DOI](https://zenodo.org/badge/63720586.svg)](https://zenodo.org/badge/latestdoi/63720586)
[![release](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml/badge.svg)](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml)
[![Documentation Status](https://readthedocs.org/projects/pysus/badge/?version=latest)](https://pysus.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pysus.svg)](https://pypi.org/project/PySUS/)
[![EpidBot Ready](https://img.shields.io/badge/EpidBot-Ready-brightgreen?style=flat-square&logo=robot&logoColor=white)](https://epidbot.kwar-ai.com.br)

PySUS is a Python package for accessing and analyzing Brazil's public health data (DATASUS). It provides tools to download, process, and work with health datasets including SINAN (disease notifications), SIM (mortality), SINASC (births), SIH (hospitalizations), SIA (ambulatory), CIHA, CNES, PNI, and more.

## What's New in PySUS 2.0

- **Simplified API**: New high-level functions for direct DataFrame access
- **Streamlit Web UI**: Launch a local web interface for browsing and downloading datasets
- **Flexible Schema Modes**: Read multiple parquet files with union, intersection, or strict modes
- **SQL Query**: Filter catalog queries by dataset, group, state, year, and month

## Installation

```bash
pip install pysus
```

For the local Streamlit web interface:
```bash
pip install pysus[web]
```

### Docker

A pre-built JupyterLab image is available on Docker Hub:

```bash
docker pull alertadengue/pysus
docker run -p 8888:8888 alertadengue/pysus
```

Or build locally and start the container:

```bash
docker compose up --build
```

Then open [http://127.0.0.1:8888/lab](http://127.0.0.1:8888/lab) in your browser.

Stop the container:

```bash
docker compose down
```

## Quick Start

### Simplified Database Functions (New in 2.0)

By default, the high-level convenience functions query and download data locally, returning a list of paths to the downloaded Parquet files. This allows you to inspect the file structure or load them with your preferred tool (e.g., pandas, Polars, DuckDB).

```python
from pysus import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

# Download SINAN Dengue data for 2000 and return a list of Parquet paths
parquet_files = sinan(disease="deng", year=2000)

# Multiple years
parquet_files = sinan(disease="deng", year=[2023, 2024])

# SINASC births for São Paulo, 2020-2023
parquet_files = sinasc(state="SP", year=[2020, 2021, 2022, 2023])

# SIM mortality data
parquet_files = sim(state="SP", year=2024)

# SIH hospitalizations with month
parquet_files = sih(state="SP", year=2024, month=[1, 2, 3])

# CNES health facilities
parquet_files = cnes(state="SP", year=2024, month=1)
```

### Loading as a DataFrame Directly
If you prefer to load and combine the data automatically into a single pandas DataFrame, pass the as_dataframe=True parameter to any of the functions:

```python
import pandas as pd
from pysus import sinan

# Download and return a concatenated pandas DataFrame
df = sinan(disease="deng", year=2024, as_dataframe=True)
```

### Listing the files

You can also list the files within the dataset to check which files are available to download

```python
from pysus import list_files

list_files("SINAN")
```

### Using the PySUS Client

```python
from pysus import PySUS

async def main():
    async with PySUS() as pysus:
        # Query DuckLake catalog
        files = await pysus.query(
            dataset="sinan",
            group="DENG",
            state="SP",
            year=2024,
        )

        # Download files
        for f in files:
            local = await pysus.download(f)
            print(local.path)

        # Read multiple parquet files
        import glob
        paths = glob.glob("/cache/sinan/**/*.parquet")
        df = pysus.read_parquet(paths, mode="union").df()
```

### Using the Streamlit Web UI (experimental feature)

Launch the local web interface:

```bash
pysus web
```

Or with a custom port:

```bash
pysus web -p 8080
```

Or run directly with Streamlit:

```bash
streamlit run pysus/web/app.py
```

The web interface provides three data sources:

- **Default (DuckLake)**: Queries the PySUS S3 catalog — the primary data source. Select a dataset and filter by group, state, year, and month.
- **FTP DataSUS**: Browses legacy DATASUS FTP directories. Auto-connects on tab selection.
- **API DataSUS (DadosGov)**: Queries the dados.gov.br open data API. Requires an API token.

Use the interactive filters to find files, add them to the download queue, and download with a single click. After a query, an expandable Python snippet shows the equivalent code to reproduce the same operation in a script or notebook.

## Features

- **Automatic Downloads**: Fetch data from FTP, DuckLake (S3), and dados.gov.br API
- **Parquet Output**: All downloaded data is converted to Apache Parquet format
- **DuckLake Integration**: S3-compatible cloud storage for parquet catalogs
- **Local Catalog**: SQLite-based tracking of download history to avoid re-downloads
- **Type Inference**: Automatic data type conversion from legacy formats (DBF, DBC)
- **CLI with Streamlit UI**: Command-line interface with local web-based UI

## Architecture

PySUS 2.0 has a modular architecture:

```
PySUS
├── FTP Client         # Traditional FTP-based datasets
├── DadosGov Client   # dados.gov.br API access
├── DuckLake Client   # S3 object storage for Parquet catalogs
└── Database Functions # High-level functions (sinan, sinasc, sim, etc.)
```

### Database Functions

New in PySUS 2.0, these functions provide a simplified interface:

| Function | Dataset | Parameters |
|----------|---------|------------|
| `sinan(disease, year)` | Disease Notifications | disease (e.g., "DENG", "ZIKA"), year |
| `sinasc(state, year, group)` | Births | state, year, group (optional) |
| `sim(state, year, group)` | Mortality | state, year, group (optional) |
| `sih(state, year, month, group)` | Hospitalizations | state, year, month, group (optional) |
| `sia(state, year, month, group)` | Ambulatory | state, year, month, group (optional) |
| `pni(state, year, group)` | Immunizations | state, year, group (optional) |
| `ibge(year, group)` | IBGE | year, group (optional) |
| `cnes(state, year, month, group)` | Health Facilities | state, year, month, group (optional) |
| `ciha(state, year, month)` | Hospital Admissions | state, year, month |

### DuckLake Query

```python
async with PySUS() as pysus:
    # Filter by any combination of parameters
    files = await pysus.query(
        dataset="sinan",      # dataset name
        group="DENG",         # disease group
        state="SP",           # state code
        year=2024,            # year
        month=1,              # month (optional)
    )
```

### read_parquet Modes

```python
# Union mode (default) - includes all columns from any file
df = pysus.read_parquet(paths, mode="union").df()

# Intersection mode - only common columns across all files
df = pysus.read_parquet(paths, mode="intersection").df()

# Strict mode - raises error if schemas don't match
df = pysus.read_parquet(paths, mode="strict").df()

# With custom SQL
df = pysus.read_parquet(paths, sql="SELECT * WHERE column > 100").df()
```

## Configuration

### Cache Directory

```python
from pysus import CACHEPATH
import os

os.environ['PYSUS_CACHEPATH'] = '/my/custom/path'
# or
pysus = PySUS(db_path='/my/config.db')
```

### Environment Variables

- `PYSUS_CACHEPATH`: Directory for cached files

## Data Sources

| Dataset | Description | Source |
|---------|-------------|--------|
| SINAN | Disease Notifications | FTP / DuckLake |
| SIM | Mortality | FTP / DuckLake |
| SINASC | Births | FTP / DuckLake |
| SIH | Hospitalizations | FTP / DuckLake |
| SIA | Ambulatory | FTP / DuckLake |
| CIHA | Hospital Admissions | FTP / DuckLake |
| CNES | Health Facilities | FTP / DuckLake |
| PNI | Immunizations | FTP / DuckLake |
| IBGE | Geographic Data | FTP / DuckLake |


## Development

### Installation

#### Using Conda
```bash
conda env create -f conda/dev.yaml
conda activate pysus
```

#### Using Poetry
```bash
poetry install
```

### Conda-Forge Recipe

The conda recipe is **auto-generated from `pyproject.toml`**. After releasing a new version,
checkout to the main branch and run:

```bash
python conda/generate_recipe.py
```

This reads `pyproject.toml` and writes `conda/recipe/meta.yaml` with the correct conda-forge package names, version constraints, and the current SHA256 (fetched from PyPI). Never edit `meta.yaml` by hand.

To submit or update the recipe, copy it into the [pysus-feedstock](https://github.com/conda-forge/pysus-feedstock) repo.

### Running Tests

Run code linters:
```bash
pre-commit run --all-files
```

Run tests:
```bash
pytest tests/
```

Run tests inside the Docker container:

```bash
docker compose exec -T -w /usr/src jupyter python3 -m pytest pysus/tests/
```

## License

GPL
