"""Compare datasets across the three clients (FTP, DadosGov, S3/DuckLake).

Usage:
    python -m pysus.management.scripts.compare_clients \
        [--datasets SINAN SIM]
    python -m pysus.management.scripts.compare_clients \
        --json --output /tmp/report.json

Requires ``.env`` (or environment) with ``ACCESS_KEY``, ``SECRET_KEY`` and
optionally ``DADOSGOV_TOKEN`` (DadosGov is skipped without the token).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

from pysus.management.report import ComparisonReporter
from pysus.management.sync import SyncEngine


def load_env(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


async def run(datasets: list[str] | None) -> dict:
    env = load_env()

    engine = SyncEngine(
        access_key=env.get("ACCESS_KEY"),
        secret_key=env.get("SECRET_KEY"),
        dadosgov_token=env.get("DADOSGOV_TOKEN"),
    )

    async with engine:
        records = {
            "ducklake": await engine.inventory.collect("ducklake", datasets),
            "ftp": await engine.inventory.collect("ftp", datasets),
        }
        records["dadosgov"] = []
        if env.get("DADOSGOV_TOKEN"):
            records["dadosgov"] = await engine.inventory.collect(
                "dadosgov", datasets, dadosgov_token=env["DADOSGOV_TOKEN"]
            )

    reporter = ComparisonReporter()
    reports = reporter.report(
        records["ducklake"] + records["ftp"] + records["dadosgov"]
    )

    return {
        "origin_counts": {
            origin: len(items) for origin, items in records.items()
        },
        "reports": [r.to_dict() for r in reports],
    }


def print_table(result: dict) -> None:
    header = (
        f"{'dataset':<10} {'total':>7} {'all3':>6} {'ftp+dg':>7} "
        f"{'ftp+s3':>7} {'dg+s3':>6} {'ftp':>6} {'dg':>6} {'s3':>6}"
    )
    print(header)
    print("-" * len(header))
    for report in result["reports"]:
        print(
            f"{report['dataset']:<10} {report['total']:>7} "
            f"{report['on_all_three']:>6} {report['on_ftp_dadosgov']:>7} "
            f"{report['on_ftp_s3']:>7} {report['on_dadosgov_s3']:>6} "
            f"{report['ftp_only']:>6} {report['dadosgov_only']:>6} "
            f"{report['s3_only']:>6}"
        )
    print()
    print(
        "origin record counts: "
        + ", ".join(f"{k}={v}" for k, v in result["origin_counts"].items())
    )

    for report in result["reports"]:
        examples = report.get("examples") or {}
        interesting = {
            k: v
            for k, v in examples.items()
            if k in ("ftp_only", "dadosgov_only", "s3_only", "all_three")
        }
        if interesting:
            print(f"\n[{report['dataset']}] examples:")
            for category, labels in interesting.items():
                for label in labels:
                    print(f"  {category:<15} {label}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Restrict to datasets (e.g. SINAN SIM)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full report as JSON instead of a table",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Also write the JSON report to this file",
    )
    args = parser.parse_args()

    datasets = [d.upper() for d in args.datasets] if args.datasets else None

    result = asyncio.run(run(datasets))

    if args.output:
        Path(args.output).write_text(json.dumps(result, indent=2))
        print(f"report written to {args.output}", file=sys.stderr)

    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print_table(result)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
