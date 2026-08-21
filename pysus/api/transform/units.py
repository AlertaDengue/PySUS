"""Unit-of-measure detection for numeric columns.

Uses column names and value ranges to infer measurement units.

Usage::

    from pysus.api.transform.units import detect_units

    units = detect_units(df)
    for u in units:
        print(f"{u.column}: {u.unit} ({u.confidence:.0%})")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd


@dataclass
class DetectedUnit:
    """Detected unit for a column.

    Attributes
    ----------
    column : str
        Column name.
    unit : str
        Detected unit (e.g. ``"kg"``, ``"mg/dL"``).
    confidence : float
        Detection confidence 0.0-1.0.
    detected_from : str
        Detection method used.
    """

    column: str
    unit: str
    confidence: float
    detected_from: Literal["column_name", "value_range", "metadata"]


# Column name patterns → unit mapping
_NAME_PATTERNS: dict[str, tuple[str, float]] = {
    "PESO": ("kg", 0.9),
    "KG": ("kg", 0.95),
    "ALTURA": ("cm", 0.85),
    "CM": ("cm", 0.9),
    "GLIC": ("mg/dL", 0.8),
    "GLICOSE": ("mg/dL", 0.85),
    "CREAT": ("mg/dL", 0.75),
    "HEMOGLOBINA": ("g/dL", 0.8),
    "HEMATOC": ("%", 0.8),
    "TEMPERAT": ("°C", 0.85),
    "PA_": ("mmHg", 0.8),
    "FREQ": ("bpm", 0.75),
    "PRESSAO": ("mmHg", 0.8),
}


def detect_units(
    df: pd.DataFrame,
    metadata: dict[str, dict[str, Any]] | None = None,
) -> list[DetectedUnit]:
    """Detect units of measurement for numeric columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    metadata : dict, optional
        Column metadata with ``"unit"`` keys.

    Returns
    -------
    list[DetectedUnit]
        Detected units per column.
    """
    units: list[DetectedUnit] = []
    seen: set[str] = set()

    # Check metadata first (highest confidence)
    if metadata:
        for col, meta in metadata.items():
            if "unit" in meta and col in df.columns:
                units.append(
                    DetectedUnit(
                        column=col,
                        unit=meta["unit"],
                        confidence=1.0,
                        detected_from="metadata",
                    )
                )
                seen.add(col)

    # Detect from column names
    for col in df.select_dtypes(include="number").columns:
        if col in seen:
            continue
        col_upper = col.upper()
        for pattern, (unit, conf) in _NAME_PATTERNS.items():
            if pattern in col_upper:
                units.append(
                    DetectedUnit(
                        column=col,
                        unit=unit,
                        confidence=conf,
                        detected_from="column_name",
                    )
                )
                seen.add(col)
                break

    # Detect from value ranges
    for col in df.select_dtypes(include="number").columns:
        if col in seen:
            continue
        detected = _detect_from_values(df[col])
        if detected is not None:
            units.append(detected)

    return units


def _detect_from_values(series: pd.Series) -> DetectedUnit | None:
    """Detect unit from value range heuristics."""
    non_null = series.dropna()
    if len(non_null) == 0:
        return None

    median = float(non_null.median())
    col_name = str(series.name)

    # Weight-like values (3-300)
    if 3 <= median <= 300:
        return DetectedUnit(
            column=col_name,
            unit="kg",
            confidence=0.5,
            detected_from="value_range",
        )

    # Temperature-like (35-42)
    if 35 <= median <= 42:
        return DetectedUnit(
            column=col_name,
            unit="°C",
            confidence=0.6,
            detected_from="value_range",
        )

    # Blood pressure systolic (60-250)
    if 60 <= median <= 250:
        return DetectedUnit(
            column=col_name,
            unit="mmHg",
            confidence=0.4,
            detected_from="value_range",
        )

    return None
