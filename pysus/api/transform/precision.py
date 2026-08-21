"""Configurable numeric precision for DataFrames.

Controls float16/32/64 precision and memory optimization.

Usage::

    from pysus.api.transform.precision import set_precision, optimize_memory

    df32 = set_precision(df, precision="float32")
    optimized = optimize_memory(df)
"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd


def set_precision(
    df: pd.DataFrame,
    precision: Literal["float16", "float32", "float64"] = "float32",
    columns: list[str] | None = None,
) -> pd.DataFrame:
    """Set numeric precision for DataFrame columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    precision : str
        Target precision.
    columns : list, optional
        Specific columns (all numeric if None).

    Returns
    -------
    pd.DataFrame
        DataFrame with adjusted precision.
    """
    dtype_map = {
        "float16": np.float16,
        "float32": np.float32,
        "float64": np.float64,
    }

    target_dtype = dtype_map[precision]

    if columns is None:
        columns = df.select_dtypes(
            include=["float64", "float32", "float16"]
        ).columns.tolist()

    df = df.copy()
    for col in columns:
        if col in df.columns and pd.api.types.is_float_dtype(df[col]):
            df[col] = df[col].astype(target_dtype)

    return df


def optimize_memory(
    df: pd.DataFrame,
    downcast_int: bool = True,
    downcast_float: bool = True,
) -> pd.DataFrame:
    """Optimize memory usage by downcasting numeric types.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    downcast_int : bool
        Downcast integer columns.
    downcast_float : bool
        Downcast float columns.

    Returns
    -------
    pd.DataFrame
        Memory-optimized DataFrame.
    """
    df = df.copy()

    for col in df.select_dtypes(include=["int"]).columns:
        if downcast_int:
            df[col] = pd.to_numeric(df[col], downcast="integer")

    for col in df.select_dtypes(include=["float"]).columns:
        if downcast_float:
            df[col] = pd.to_numeric(df[col], downcast="float")

    return df
