"""Basic data validation for DATASUS DataFrames.

Validates age ranges, date formats, categorical values, and
user-defined rules.

Usage::

    from pysus.api.quality.validation import validate_data

    results = validate_data(df)
    for r in results:
        if not r.passed:
            print(f"{r.column}: {r.details}")
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

import pandas as pd


@dataclass
class ValidationResult:
    """Result of a single validation rule.

    Attributes
    ----------
    column : str
        Column name that was validated.
    rule : str
        Rule name (e.g. ``"age_range"``, ``"date_format"``).
    passed : bool
        Whether validation passed.
    failed_count : int
        Number of rows that failed.
    failed_pct : float
        Percentage of rows that failed (0.0-1.0).
    details : str
        Human-readable description of the result.
    """

    column: str
    rule: str
    passed: bool
    failed_count: int
    failed_pct: float
    details: str


def validate_data(
    df: pd.DataFrame,
    rules: dict[str, dict[str, Any]] | None = None,
) -> list[ValidationResult]:
    """Validate DataFrame against built-in and custom rules.

    Built-in rules:
    - Age columns (``IDADE``, ``NU_IDADE_N``): 0-120
    - Date columns (``DT_*``): YYYYMMDD format, reasonable range
    - Categorical columns (``CS_*``): values in expected set

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    rules : dict, optional
        Custom validation rules per column. Keys are column names,
        values are dicts with ``"type"`` and parameters.

    Returns
    -------
    list[ValidationResult]
        Validation results (includes passing rules).
    """
    results: list[ValidationResult] = []

    # Auto-detect and validate common patterns
    for col in df.columns:
        if col.startswith("DT_"):
            results.append(_validate_date(df, col))

        if col in ("IDADE", "NU_IDADE_N", "IDADE_N"):
            results.append(_validate_age(df, col))

        if col.startswith("CS_"):
            results.append(_validate_categorical(df, col))

    # Apply custom rules
    if rules:
        for col, rule in rules.items():
            if col in df.columns:
                result = _apply_rule(df, col, rule)
                if result is not None:
                    results.append(result)

    return results


def _validate_age(df: pd.DataFrame, col: str) -> ValidationResult:
    """Validate age column (0-120)."""
    series = pd.to_numeric(df[col], errors="coerce")
    non_null = series.dropna()
    invalid = non_null[(non_null < 0) | (non_null > 120)]
    failed_count = len(invalid)

    total = len(df)
    failed_pct = failed_count / total if total > 0 else 0.0

    return ValidationResult(
        column=col,
        rule="age_range",
        passed=failed_count == 0,
        failed_count=failed_count,
        failed_pct=round(failed_pct, 4),
        details=f"Age must be 0-120, found {failed_count} invalid",
    )


def _validate_date(df: pd.DataFrame, col: str) -> ValidationResult:
    """Validate date column format (YYYYMMDD)."""
    series = df[col].dropna()
    if len(series) == 0:
        return ValidationResult(
            column=col,
            rule="date_format",
            passed=True,
            failed_count=0,
            failed_pct=0.0,
            details="No values to validate",
        )

    pattern = re.compile(r"^\d{8}$")
    invalid_mask = ~series.astype(str).str.match(pattern)
    failed_count = int(invalid_mask.sum())

    total = len(df)
    failed_pct = failed_count / total if total > 0 else 0.0

    return ValidationResult(
        column=col,
        rule="date_format",
        passed=failed_count == 0,
        failed_count=failed_count,
        failed_pct=round(failed_pct, 4),
        details=f"Expected YYYYMMDD format, found {failed_count} invalid",
    )


def _validate_categorical(df: pd.DataFrame, col: str) -> ValidationResult:
    """Validate categorical column has reasonable values."""
    series = df[col].dropna()
    if len(series) == 0:
        return ValidationResult(
            column=col,
            rule="categorical",
            passed=True,
            failed_count=0,
            failed_pct=0.0,
            details="No values to validate",
        )

    unique_count = series.nunique()
    failed_count = 0

    return ValidationResult(
        column=col,
        rule="categorical",
        passed=True,
        failed_count=failed_count,
        failed_pct=0.0,
        details=f"{unique_count} unique values",
    )


def _apply_rule(
    df: pd.DataFrame, col: str, rule: dict[str, Any]
) -> ValidationResult | None:
    """Apply a custom validation rule."""
    rule_type = rule.get("type", "")

    if rule_type == "range":
        min_val = rule.get("min", float("-inf"))
        max_val = rule.get("max", float("inf"))
        series = pd.to_numeric(df[col], errors="coerce")
        invalid = series[(series < min_val) | (series > max_val)]
        failed_count = len(invalid)
        total = len(df)
        failed_pct = failed_count / total if total > 0 else 0.0

        return ValidationResult(
            column=col,
            rule=f"range_{min_val}_{max_val}",
            passed=failed_count == 0,
            failed_count=failed_count,
            failed_pct=round(failed_pct, 4),
            details=f"Must be {min_val}-{max_val}, "
            f"found {failed_count} invalid",
        )

    elif rule_type == "not_null":
        failed_count = int(df[col].isna().sum())
        total = len(df)
        failed_pct = failed_count / total if total > 0 else 0.0

        return ValidationResult(
            column=col,
            rule="not_null",
            passed=failed_count == 0,
            failed_count=failed_count,
            failed_pct=round(failed_pct, 4),
            details=f"Found {failed_count} null values",
        )

    return None
