"""Quality scoring for DataFrames.

Computes a 0-100 quality score based on completeness, validity,
and consistency metrics.

Usage::

    from pysus.api.quality.score import quality_score

    score = quality_score(df)
    print(f"Overall: {score.overall:.1f}/100")
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class QualityScore:
    """Quality score result.

    Attributes
    ----------
    overall : float
        Overall score 0-100.
    completeness : float
        Completeness score 0-100 (non-null percentage).
    validity : float
        Validity score 0-100 (values passing basic checks).
    consistency : float
        Consistency score 0-100 (pattern matching).
    details : dict
        Detailed breakdown by column.
    """

    overall: float
    completeness: float
    validity: float
    consistency: float
    details: dict[str, Any]


def quality_score(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]] | None = None,
) -> QualityScore:
    """Calculate overall quality score for a DataFrame.

    Scoring weights:
    - Completeness: 40% (average non-null percentage)
    - Validity: 40% (values passing basic validation)
    - Consistency: 20% (values matching expected patterns)

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    schema : dict, optional
        Column metadata for validation (from
        ``load_column_metadata``).

    Returns
    -------
    QualityScore
        Quality score with overall and component scores.
    """
    # Completeness: average non-null percentage across columns
    if len(df) == 0:
        completeness = 100.0
    else:
        completeness = float(df.notna().mean().mean() * 100)

    # Validity: percentage of values passing basic checks
    validity = _calculate_validity(df, schema)

    # Consistency: percentage of values matching patterns
    consistency = _calculate_consistency(df)

    # Overall: weighted average
    overall = completeness * 0.4 + validity * 0.4 + consistency * 0.2

    return QualityScore(
        overall=round(overall, 2),
        completeness=round(completeness, 2),
        validity=round(validity, 2),
        consistency=round(consistency, 2),
        details={
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "completeness_by_column": {
                col: round(pct, 4) for col, pct in df.notna().mean().items()
            },
            "validity_by_column": _validity_by_column(df, schema),
        },
    )


def _calculate_validity(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]] | None,
) -> float:
    """Calculate validity score (percentage of non-null, valid values)."""
    if len(df) == 0:
        return 100.0

    valid_counts = []
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        if len(non_null) == 0:
            valid_counts.append(1.0)
            continue

        # Basic validity: non-null values that aren't just whitespace
        if series.dtype == "object":
            valid = non_null.str.strip().str.len() > 0
            valid_counts.append(valid.mean())
        else:
            valid_counts.append(1.0)

    return (
        float(sum(valid_counts) / len(valid_counts) * 100)
        if valid_counts
        else 100.0
    )


def _calculate_consistency(df: pd.DataFrame) -> float:
    """Calculate consistency score (pattern matching)."""
    if len(df) == 0:
        return 100.0

    scores = []
    for col in df.columns:
        series = df[col].dropna()
        if len(series) == 0:
            scores.append(1.0)
            continue

        # Check for date-like patterns
        if col.startswith("DT_"):
            import re

            pattern = re.compile(r"^\d{8}$")
            matches = series.astype(str).str.match(pattern).mean()
            scores.append(float(matches))
        else:
            scores.append(1.0)

    return float(sum(scores) / len(scores) * 100) if scores else 100.0


def _validity_by_column(
    df: pd.DataFrame,
    schema: dict[str, dict[str, Any]] | None,
) -> dict[str, float]:
    """Calculate validity percentage per column."""
    result: dict[str, float] = {}
    for col in df.columns:
        series = df[col]
        non_null = series.dropna()
        if len(non_null) == 0:
            result[col] = 1.0
        elif series.dtype == "object":
            valid = non_null.str.strip().str.len() > 0
            result[col] = round(float(valid.mean()), 4)
        else:
            result[col] = 1.0
    return result
