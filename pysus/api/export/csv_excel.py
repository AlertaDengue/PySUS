"""CSV and Excel export with optional metadata.

Usage::

    from pysus.api.export.csv_excel import to_csv, to_excel

    to_csv(df, "output.csv", metadata={"source": "DATASUS"})
    to_excel(df, "output.xlsx", metadata={"source": "DATASUS"})
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def to_csv(
    df: pd.DataFrame,
    path: str | Path,
    metadata: dict | None = None,
    encoding: str = "utf-8",
) -> Path:
    """Export DataFrame to CSV with optional metadata sidecar.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    path : str or Path
        Output file path.
    metadata : dict, optional
        Metadata to save in ``.metadata.json`` sidecar file.
    encoding : str
        File encoding.

    Returns
    -------
    Path
        Path to created file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(path, index=False, encoding=encoding)

    if metadata:
        metadata_path = path.with_suffix(".metadata.json")
        metadata_path.write_text(
            json.dumps(metadata, indent=2, default=str, ensure_ascii=False),
            encoding="utf-8",
        )

    return path


def to_excel(
    df: pd.DataFrame,
    path: str | Path,
    metadata: dict | None = None,
    sheet_name: str = "Data",
) -> Path:
    """Export DataFrame to Excel with metadata sheet.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    path : str or Path
        Output file path.
    metadata : dict, optional
        Metadata to include as a separate sheet.
    sheet_name : str
        Name of the data sheet.

    Returns
    -------
    Path
        Path to created file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)

        if metadata:
            metadata_df = pd.DataFrame(
                [{"key": k, "value": str(v)} for k, v in metadata.items()]
            )
            metadata_df.to_excel(writer, sheet_name="Metadata", index=False)

    return path
