"""Backfill ``group_id`` in DuckLake catalogs where it is still NULL.

The catalog can retain rows written before the filename group parser ran.
Such rows are invisible to group-filtered queries (see issue #380).  This
script surveys every per-dataset catalog, derives the group from the
dataset formatter (falling back to the canonical S3 key segment), updates
the rows and re-uploads the modified catalogs.

Usage:
    python -m pysus.management.scripts.backfill_group_ids --dry-run
    python -m pysus.management.scripts.backfill_group_ids
"""

from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

import httpx
from pysus.management.normalize import BucketNormalizer

from .relayout_bucket import CATALOGS, load_env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the plan without touching the catalogs",
    )
    parser.add_argument(
        "--workdir",
        default=str(Path(tempfile.gettempdir()) / "pysus_backfill_group_ids"),
        help="Working directory for downloaded catalogs",
    )
    args = parser.parse_args()

    env = load_env()
    normalizer = BucketNormalizer(
        access_key=env["ACCESS_KEY"],
        secret_key=env["SECRET_KEY"],
    )

    workdir = Path(args.workdir)
    workdir.mkdir(parents=True, exist_ok=True)

    print("downloading catalogs...", flush=True)
    with httpx.Client(follow_redirects=True, timeout=600) as client:
        for name in CATALOGS:
            url = (
                f"https://nbg1.your-objectstorage.com/pysus/"
                f"public/{name}.duckdb"
            )
            response = client.get(url)
            if response.status_code == 404:
                print(f"  {name}: not found, skipping", flush=True)
                continue
            response.raise_for_status()
            (workdir / f"{name}.duckdb").write_bytes(response.content)
            print(f"  {name} ({len(response.content)} bytes)", flush=True)

    changed: list[str] = []
    total = 0
    for name in CATALOGS:
        catalog_dir = workdir / f"{name}.duckdb"
        if not catalog_dir.exists():
            continue
        print(f"surveying {name}...", flush=True)
        fixes = normalizer.survey_group_ids(catalog_dir)
        total += len(fixes)
        print(f"  {name}: {len(fixes)} group_id repairs", flush=True)
        if not fixes:
            continue
        changed.append(name)
        normalizer.apply_catalog(
            catalog_dir,
            [],
            [],
            dry_run=args.dry_run,
            group_fixes=fixes,
        )

    print(f"total group_id repairs: {total}", flush=True)

    if args.dry_run:
        print("DRY RUN — no changes applied", flush=True)
        return 0

    if not changed:
        print("nothing to upload", flush=True)
        return 0

    print("uploading catalogs...", flush=True)
    from pysus.api.ducklake.functional import upload_s3

    async def upload_all():
        for name in changed:
            await upload_s3(
                local_path=workdir / f"{name}.duckdb",
                remote_path=f"public/{name}.duckdb",
                access_key=env["ACCESS_KEY"],
                secret_key=env["SECRET_KEY"],
            )
            print(f"  uploaded {name}", flush=True)

    asyncio.run(upload_all())
    print("done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
