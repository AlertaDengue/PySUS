"""Automatic data profiling report for DataFrames.

Generates comprehensive profiling reports including statistics,
missing values, quality scores, and distributions.

Usage::

    from pysus.api.quality.profiling import profile_report

    # Text report
    print(profile_report(df))

    # JSON report
    report = profile_report(df, format="json")

    # Save to file
    profile_report(df, output="report.txt")
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pysus.api.quality.missing import missing_values
from pysus.api.quality.score import quality_score
from pysus.api.quality.statistics import column_stats


def profile_report(
    df: pd.DataFrame,
    output: str | Path | None = None,
    format: Literal["html", "json", "text"] = "text",
) -> str | dict[str, Any]:
    """Generate comprehensive profiling report.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    output : str or Path, optional
        File path to save report.
    format : str
        Output format: ``"html"``, ``"json"``, or ``"text"``.

    Returns
    -------
    str or dict
        Report as string (text/html) or dict (json).
    """
    stats = column_stats(df)
    missing = missing_values(df)
    score = quality_score(df)

    report_data: dict[str, Any] = {
        "overview": {
            "rows": len(df),
            "columns": len(df.columns),
            "memory_mb": round(
                df.memory_usage(deep=True).sum() / (1024 * 1024), 2
            ),
            "missing_total": int(df.isna().sum().sum()),
            "missing_pct": round(float(df.isna().mean().mean()), 4),
        },
        "columns": stats.to_dict("records"),
        "missing": (
            missing.to_dict("records")
            if isinstance(missing, pd.DataFrame)
            else []
        ),
        "quality_score": {
            "overall": score.overall,
            "completeness": score.completeness,
            "validity": score.validity,
            "consistency": score.consistency,
        },
    }

    output_str: str | dict[str, Any]
    if format == "json":
        output_str = report_data
    elif format == "html":
        output_str = _generate_html_report(report_data)
    else:
        output_str = _generate_text_report(report_data)

    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        if format == "json":
            save_content = json.dumps(
                output_str,
                indent=2,
                default=str,
                ensure_ascii=False,
            )
        else:
            save_content = str(output_str)
        Path(output).write_text(save_content, encoding="utf-8")

    return output_str


def _generate_text_report(data: dict[str, Any]) -> str:
    """Generate plain text report."""
    lines = []
    lines.append("=" * 60)
    lines.append("DATASUS Data Profile Report")
    lines.append("=" * 60)

    overview = data["overview"]
    lines.append("")
    lines.append("OVERVIEW")
    lines.append("-" * 40)
    lines.append(f"  Rows: {overview['rows']:,}")
    lines.append(f"  Columns: {overview['columns']}")
    lines.append(f"  Memory: {overview['memory_mb']:.2f} MB")
    lines.append(
        f"  Missing values: {overview['missing_total']:,}"
        f" ({overview['missing_pct']:.1%})"
    )

    score = data["quality_score"]
    lines.append("")
    lines.append("QUALITY SCORE")
    lines.append("-" * 40)
    lines.append(f"  Overall: {score['overall']:.1f}/100")
    lines.append(f"  Completeness: {score['completeness']:.1f}/100")
    lines.append(f"  Validity: {score['validity']:.1f}/100")
    lines.append(f"  Consistency: {score['consistency']:.1f}/100")

    lines.append("")
    lines.append("TOP COLUMNS BY MEMORY")
    lines.append("-" * 40)
    for col_info in data["columns"][:10]:
        lines.append(
            f"  {col_info['column']:30s}"
            f" {col_info['dtype']:12s}"
            f" {col_info['memory_mb']:8.2f} MB"
        )

    if data["missing"]:
        lines.append("")
        lines.append("TOP MISSING VALUES")
        lines.append("-" * 40)
        for m in data["missing"][:10]:
            if m["missing_pct"] > 0:
                lines.append(
                    f"  {m['column']:30s}"
                    f" {m['missing_pct']:.1%}"
                    f" ({m['missing_count']:,} missing)"
                )

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def _generate_html_report(data: dict[str, Any]) -> str:
    """Generate HTML report."""
    overview = data["overview"]
    score = data["quality_score"]

    parts = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html>")
    parts.append("<head>")
    parts.append("  <title>DATASUS Data Profile</title>")
    parts.append("  <style>")
    parts.append("    body { font-family: sans-serif; margin: 20px; }")
    parts.append("    h1 { color: #333; }")
    parts.append(
        "    table { border-collapse: collapse;"
        " width: 100%; margin: 10px 0; }"
    )
    parts.append(
        "    th, td { border: 1px solid #ddd;"
        " padding: 8px; text-align: left; }"
    )
    parts.append("    th { background-color: #f5f5f5; }")
    parts.append("    .metric { display: inline-block; margin: 10px 20px; }")
    parts.append("    .metric-value { font-size: 24px; font-weight: bold; }")
    parts.append("    .metric-label { color: #666; }")
    parts.append("  </style>")
    parts.append("</head>")
    parts.append("<body>")
    parts.append("<h1>DATASUS Data Profile Report</h1>")

    rows = overview["rows"]
    cols = overview["columns"]
    mem = overview["memory_mb"]
    overall = score["overall"]
    completeness = score["completeness"]
    validity = score["validity"]

    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">{rows:,}</div>')
    parts.append('<div class="metric-label">Rows</div>')
    parts.append("</div>")
    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">{cols}</div>')
    parts.append('<div class="metric-label">Columns</div>')
    parts.append("</div>")
    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">{mem:.2f} MB</div>')
    parts.append('<div class="metric-label">Memory</div>')
    parts.append("</div>")

    parts.append("<h2>Quality Score</h2>")
    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">' f"{overall:.1f}/100</div>")
    parts.append('<div class="metric-label">Overall</div>')
    parts.append("</div>")
    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">' f"{completeness:.1f}/100</div>")
    parts.append('<div class="metric-label">Completeness</div>')
    parts.append("</div>")
    parts.append('<div class="metric">')
    parts.append(f'<div class="metric-value">' f"{validity:.1f}/100</div>")
    parts.append('<div class="metric-label">Validity</div>')
    parts.append("</div>")

    parts.append("<h2>Column Statistics</h2>")
    parts.append("<table>")
    parts.append("  <tr>")
    parts.append("    <th>Column</th>")
    parts.append("    <th>Type</th>")
    parts.append("    <th>Null %</th>")
    parts.append("    <th>Unique</th>")
    parts.append("    <th>Memory (MB)</th>")
    parts.append("  </tr>")

    for col_info in data["columns"]:
        col = col_info["column"]
        dtype = col_info["dtype"]
        null_pct = col_info["null_pct"]
        unique = col_info["unique_count"]
        mem_mb = col_info["memory_mb"]
        parts.append("<tr>")
        parts.append(f"<td>{col}</td>")
        parts.append(f"<td>{dtype}</td>")
        parts.append(f"<td>{null_pct:.1%}</td>")
        parts.append(f"<td>{unique:,}</td>")
        parts.append(f"<td>{mem_mb:.2f}</td>")
        parts.append("</tr>")

    parts.append("</table>")
    parts.append("</body>")
    parts.append("</html>")

    return "\n".join(parts)
