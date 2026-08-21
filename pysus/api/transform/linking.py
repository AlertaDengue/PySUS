"""Cross-dataset linking helpers for DATASUS databases.

Provides common linking keys and merge utilities for joining
datasets from different databases.

Usage::

    from pysus.api.transform.linking import get_linking_keys, link_datasets

    keys = get_linking_keys("sinan", "sih")
    merged = link_datasets(sinan_df, sih_df, on="MUNIC_RES")
"""

from __future__ import annotations

from typing import Literal

import pandas as pd

# Common linking keys across DATASUS databases
LINKING_KEYS: dict[str, dict[str, object]] = {
    "CNES": {
        "description": "Código Nacional de Estabelecimentos de Saúde",
        "databases": ["sih", "sia", "cnes"],
    },
    "MUNIC_RES": {
        "description": "Município de residência (IBGE 7)",
        "databases": ["sinan", "sih", "sia", "sim", "sinasc"],
    },
    "MUNIC_MOV": {
        "description": "Município de atendimento",
        "databases": ["sih", "sia"],
    },
    "UF_RES": {
        "description": "Unidade federativa de residência",
        "databases": ["sinan", "sih", "sia"],
    },
    "N_AIH": {
        "description": "Número da AIH",
        "databases": ["sih"],
    },
    "CPF": {
        "description": "CPF do paciente",
        "databases": ["sih", "sia"],
    },
}


def get_linking_keys(
    database1: str,
    database2: str,
) -> list[str]:
    """Get common linking keys between two databases.

    Parameters
    ----------
    database1 : str
        First database name.
    database2 : str
        Second database name.

    Returns
    -------
    list[str]
        Column names that can link the two datasets.
    """
    common: list[str] = []
    for key, info in LINKING_KEYS.items():
        dbs = info["databases"]
        if isinstance(dbs, list) and database1 in dbs and database2 in dbs:
            common.append(key)
    return common


def link_datasets(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    on: str | list[str],
    how: Literal["inner", "left", "right", "outer"] = "inner",
) -> pd.DataFrame:
    """Link two DATASUS datasets on common keys.

    Handles column name conflicts by adding suffixes.

    Parameters
    ----------
    df1 : pd.DataFrame
        First DataFrame.
    df2 : pd.DataFrame
        Second DataFrame.
    on : str or list
        Column name(s) to join on.
    how : str
        Join type.

    Returns
    -------
    pd.DataFrame
        Merged DataFrame.
    """
    on_list = [on] if isinstance(on, str) else on
    overlapping = set(df1.columns) & set(df2.columns) - set(on_list)

    if overlapping:
        df1 = df1.rename(columns={c: f"{c}_1" for c in overlapping})
        df2 = df2.rename(columns={c: f"{c}_2" for c in overlapping})

    return pd.merge(df1, df2, on=on_list, how=how)
