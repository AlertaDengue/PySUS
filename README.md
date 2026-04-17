# PySUS 2.0 is now available!

[![DOI](https://zenodo.org/badge/63720586.svg)](https://zenodo.org/badge/latestdoi/63720586)
[![release](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml/badge.svg)](https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml)
[![Documentation Status](https://readthedocs.org/projects/pysus/badge/?version=latest)](https://pysus.readthedocs.io/en/latest/?badge=latest)
[![PyPI version](https://badge.fury.io/py/pysus.svg)](https://pypi.org/project/PySUS/)

PySUS is a Python package for accessing and analyzing Brazil's public health data (DATASUS). It provides tools to download, process, and work with health datasets including SINAN (disease notifications), SIM (mortality), SINASC (births), SIH (hospitalizations), SIA (ambulatory), CIHA, CNES, PNI, and more.

## What's New in PySUS 2.0

- **Simplified API**: New high-level functions for direct DataFrame access
- **CLI & TUI**: Launch the text-based user interface from command line
- **Flexible Schema Modes**: Read multiple parquet files with union, intersection, or strict modes
- **SQL Query**: Filter catalog queries by dataset, group, state, year, and month

## Installation

```bash
pip install pysus
```

For DBC file support (requires libffi):
```bash
# Ubuntu/Debian
sudo apt install libffi-dev
pip install pysus[dbc]
```

## Quick Start

### Simplified Database Functions (New in 2.0)

The easiest way to get data as a pandas DataFrame:

```python
from pysus import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

# Download SINAN Dengue data for 2024
df = sinan(disease="deng", year=2024)

# Multiple years
df = sinan(disease="deng", year=[2023, 2024])

# SINASC births for São Paulo, 2020-2023
df = sinasc(state="SP", year=[2020, 2021, 2022, 2023])

# SIM mortality data
df = sim(state="SP", year=2024)

# SIH hospitalizations with month
df = sih(state="SP", year=2024, month=[1, 2, 3])

# CNES health facilities
df = cnes(state="SP", year=2024, month=1)
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

### Using the TUI

Launch the interactive text-based interface:

```bash
pysus tui -l pt
```

Or from Python:

```python
from pysus.tui.app import PySUS
app = PySUS(lang="pt")
app.run()
```

## Features

- **Automatic Downloads**: Fetch data from FTP, DuckLake (S3), and dados.gov.br API
- **Parquet Output**: All downloaded data is converted to Apache Parquet format
- **DuckLake Integration**: S3-compatible cloud storage for parquet catalogs
- **Local Catalog**: SQLite-based tracking of download history to avoid re-downloads
- **Type Inference**: Automatic data type conversion from legacy formats (DBF, DBC)
- **CLI with TUI**: Command-line interface with interactive text-based UI

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

### Running Tests

Run code linters:
```bash
pre-commit run --all-files
```

Run tests:
```bash
pytest tests/
```

## License

GPL
