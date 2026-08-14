"""Sync S3 with every file available on FTP and DadosGov.

Downloads missing/outdated files, converts them to parquet, uploads to S3
and updates the DuckLake catalogs. Resumable: files already cataloged with
an equally recent origin are skipped, and the catalogs are checkpointed to
S3 every N uploads.

Usage:
    python -m pysus.management.scripts.sync_clients --datasets SINAN SIM
    python -m pysus.management.scripts.sync_clients --checkpoint-every 500
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

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


async def run(
    datasets, checkpoint_every, force, workers, ftp_connections
) -> dict:
    env = load_env()
    engine = SyncEngine(
        access_key=env.get("ACCESS_KEY"),
        secret_key=env.get("SECRET_KEY"),
        dadosgov_token=env.get("DADOSGOV_TOKEN"),
    )

    counts: dict[str, int] = {}

    def on_outcome(outcome) -> None:
        counts[outcome.status] = counts.get(outcome.status, 0) + 1
        if outcome.status in ("uploaded", "failed", "needs_token"):
            print(f"[{outcome.status}] {outcome.detail}", flush=True)
        total = sum(counts.values())
        if total % 500 == 0:
            print(f"progress: {counts}", flush=True)

    async with engine:
        report = await engine.run(
            datasets=datasets,
            force=force,
            checkpoint_every=checkpoint_every,
            on_outcome=on_outcome,
            workers=workers,
            ftp_connections=ftp_connections,
        )
    return report.summary()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--datasets",
        nargs="*",
        default=None,
        help="Restrict to datasets (e.g. SINAN SIM)",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=500,
        help="Upload catalogs to S3 every N successful uploads",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Reprocess files even when the catalog is current",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=16,
        help="Concurrent ingestion workers",
    )
    parser.add_argument(
        "--ftp-connections",
        type=int,
        default=6,
        help="FTP connection pool size",
    )
    args = parser.parse_args()

    datasets = [d.upper() for d in args.datasets] if args.datasets else None

    summary = asyncio.run(
        run(
            datasets,
            args.checkpoint_every,
            args.force,
            args.workers,
            args.ftp_connections,
        )
    )
    print(summary)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
