"""Column rename/alias system for historical DATASUS columns.

Maps old column names to current names for backward compatibility.

Usage::

    from pysus.api.transform.aliases import get_aliases, rename_columns

    old_names = get_aliases("sinan", "DT_NOTIFIC")
    df = rename_columns(df, database="sinan")
"""

from __future__ import annotations

import pandas as pd

# Historical column name changes across DATASUS versions
ALIAS_HISTORY: dict[str, dict[str, list[str]]] = {
    "sinan": {
        "DT_NOTIFIC": ["DT_NOT"],
        "CS_SEXO": ["SEXO"],
        "NU_IDADE_N": ["IDADE_N"],
    },
    "sih": {
        "N_AIH": ["NUM_AIH"],
        "DIAG_PRINC": ["DIAGNOSTICO"],
    },
}


def get_aliases(
    database: str,
    column: str,
) -> list[str]:
    """Get historical aliases for a column name.

    Parameters
    ----------
    database : str
        Database name (``"sinan"``, ``"sih"``, etc.).
    column : str
        Current column name.

    Returns
    -------
    list[str]
        Previous names for this column.
    """
    return ALIAS_HISTORY.get(database, {}).get(column, [])


def rename_columns(
    df: pd.DataFrame,
    mapping: dict[str, str] | None = None,
    database: str | None = None,
) -> pd.DataFrame:
    """Rename columns using mapping or historical aliases.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    mapping : dict, optional
        Custom rename mapping ``{old_name: new_name}``.
    database : str, optional
        Database name to use historical aliases.

    Returns
    -------
    pd.DataFrame
        DataFrame with renamed columns.
    """
    df = df.copy()

    if mapping:
        df = df.rename(columns=mapping)

    if database:
        aliases = ALIAS_HISTORY.get(database, {})
        for new_name, old_names in aliases.items():
            for old_name in old_names:
                if old_name in df.columns and new_name not in df.columns:
                    df = df.rename(columns={old_name: new_name})

    return df
