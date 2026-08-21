"""Data quality analysis module for PySUS.

Provides missing value analysis, data validation, column statistics,
quality scoring, and automatic profiling reports.

Usage::

    from pysus.api.quality import missing_values, validate_data
    from pysus.api.quality import column_stats, quality_score, profile_report

    missing = missing_values(df)
    report = validate_data(df)
    stats = column_stats(df)
    score = quality_score(df)
    report = profile_report(df)
"""

from pysus.api.quality.missing import missing_values
from pysus.api.quality.profiling import profile_report
from pysus.api.quality.score import quality_score
from pysus.api.quality.statistics import column_stats
from pysus.api.quality.validation import validate_data

__all__ = [
    "column_stats",
    "missing_values",
    "profile_report",
    "quality_score",
    "validate_data",
]
