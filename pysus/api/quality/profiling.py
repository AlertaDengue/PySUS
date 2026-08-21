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
    # Generate components
    stats = column_stats(df)
    missing = missing_values(df)
    score = quality_score(df)

    # Build report data
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

    # Format output
    output_str: str | dict[str, Any]
    if format == "json":
        output_str = report_data
    elif format == "html":
        output_str = _generate_html_report(report_data)
    else:
        output_str = _generate_text_report(report_data)

    # Save if output path provided
    if output:
        Path(output).parent.mkdir(parents=True, exist_ok=True)
        if format == "json":
            save_content = json.dumps(
                output_str, indent=2, default=str, ensure_ascii=False
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

    # Overview
    overview = data["overview"]
    lines.append("")
    lines.append("OVERVIEW")
    lines.append("-" * 40)
    lines.append(f"  Rows:            {overview['rows']:,}")
    lines.append(f"  Columns:         {overview['columns']}")
    lines.append(f"  Memory:          {overview['memory_mb']:.2f} MB")
    lines.append(
        f"  Missing values:  {overview['missing_total']:,} "
        f"({overview['missing_pct']:.1%})"
    )

    # Quality score
    score = data["quality_score"]
    lines.append("")
    lines.append("QUALITY SCORE")
    lines.append("-" * 40)
    lines.append(f"  Overall:    {score['overall']:.1f}/100")
    lines.append(f"  Completeness: {score['completeness']:.1f}/100")
    lines.append(f"  Validity:     {score['validity']:.1f}/100")
    lines.append(f"  Consistency:  {score['consistency']:.1f}/100")

    # Top columns by memory
    lines.append("")
    lines.append("TOP COLUMNS BY MEMORY")
    lines.append("-" * 40)
    for col_info in data["columns"][:10]:
        lines.append(
            f"  {col_info['column']:30s} "
            f"{col_info['dtype']:12s} "
            f"{col_info['memory_mb']:8.2f} MB"
        )

    # Missing values (top 10)
    if data["missing"]:
        lines.append("")
        lines.append("TOP MISSING VALUES")
        lines.append("-" * 40)
        for m in data["missing"][:10]:
            if m["missing_pct"] > 0:
                lines.append(
                    f"  {m['column']:30s} "
                    f"{m['missing_pct']:.1%} "
                    f"({m['missing_count']:,} missing)"
                )

    lines.append("")
    lines.append("=" * 60)
    return "\n".join(lines)


def _generate_html_report(data: dict[str, Any]) -> str:
    """Generate HTML report."""
    overview = data["overview"]
    score = data["quality_score"]

    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>DATASUS Data Profile</title>
    <style>
        body {{ font-family: sans-serif; margin: 20px; }}
        h1 {{ color: #333; }}
        table {{ border-collapse: collapse; width: 100%; margin: 10px 0; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f5f5f5; }}
        .metric {{ display: inline-block; margin: 10px 20px; }}
        .metric-value {{ font-size: 24px; font-weight: bold; }}
        .metric-label {{ color: #666; }}
    </style>
</head>
<body>
    <h1>DATASUS Data Profile Report</h1>

    <div class="metric">
        <div class="metric-value">{overview['rows']:,}</div>
        <div class="metric-label">Rows</div>
    </div>
    <div class="metric">
        <div class="metric-value">{overview['columns']}</div>
        <div class="metric-label">Columns</div>
    </div>
    <div class="metric">
        <div class="metric-value">{overview['memory_mb']:.2f} MB</div>
        <div class="metric-label">Memory</div>
    </div>

    <h2>Quality Score</h2>
    <div class="metric">
        <div class="metric-value">{score['overall']:.1f}/100</div>
        <div class="metric-label">Overall</div>
    </div>
    <div class="metric">
        <div class="metric-value">{score['completeness']:.1f}/100</div>
        <div class="metric-label">Completeness</div>
    </div>
    <div class="metric">
        <div class="metric-value">{score['validity']:.1f}/100</div>
        <div class="metric-label">Validity</div>
    </div>

    <h2>Column Statistics</h2>
    <table>
        <tr>
            <th>Column</th>
            <th>Type</th>
            <th>Null %</th>
            <th>Unique</th>
            <th>Memory (MB)</th>
        </tr>
"""

    for col_info in data["columns"]:
        html += f"""        <tr>
            <td>{col_info['column']}</td>
            <td>{col_info['dtype']}</td>
            <td>{col_info['null_pct']:.1%}</td>
            <td>{col_info['unique_count']:,}</td>
            <td>{col_info['memory_mb']:.2f}</td>
        </tr>
"""

    html += """    </table>
</body>
</html>"""

    return html
