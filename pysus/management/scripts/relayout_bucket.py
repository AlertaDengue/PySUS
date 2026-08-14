"""Relayout the bucket: hierarchical directories compose file attributes.

Moves every cataloged parquet to::

    public/data/<origin>/<dataset>/<group>/<year>/<month>/<state>/<STEM>.parquet

Missing attributes use ``_`` (state falls back to ``BR`` for national
files). Catalog ``files.path`` rows are updated accordingly and the
catalogs are re-uploaded to S3. Uncataloged objects are relocated using
the per-dataset formatters when possible.

Usage:
    python -m pysus.management.scripts.relayout_bucket --dry-run
    python -m pysus.management.scripts.relayout_bucket
"""

from __future__ import annotations

import argparse
from pathlib import Path

import httpx
from pysus.management.normalize import BucketNormalizer

CATALOGS = (
    "catalog_ciha",
    "catalog_cnes",
    "catalog_covid19",
    "catalog_ibge",
    "catalog_pni",
    "catalog_sia",
    "catalog_sih",
    "catalog_sim",
    "catalog_sinan",
    "catalog_sinasc",
)

SCAN_PREFIXES = ("public/data/ftp/", "public/data/dadosgov/", "data/ftp/")


def load_env(path: str = ".env") -> dict[str, str]:
    env: dict[str, str] = {}
    for line in Path(path).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print the plan without touching S3",
    )
    parser.add_argument(
        "--workdir",
        default="/tmp/opencode/relayout",
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

    print("listing objects...", flush=True)
    object_keys: set[str] = set()
    object_sizes: dict[str, int] = {}
    for prefix in SCAN_PREFIXES:
        for key, size in normalizer._list_objects(prefix):
            object_keys.add(key)
            object_sizes[key] = size
    print(f"objects: {len(object_keys)}", flush=True)

    print("downloading catalogs...", flush=True)
    with httpx.Client(follow_redirects=True, timeout=600) as client:
        for name in CATALOGS:
            url = (
                f"https://nbg1.your-objectstorage.com/pysus/"
                f"public/{name}.duckdb"
            )
            response = client.get(url)
            response.raise_for_status()
            (workdir / f"{name}.duckdb").write_bytes(response.content)
            print(f"  {name} ({len(response.content)} bytes)", flush=True)

    all_old_paths: set[str] = set()
    all_new_paths: set[str] = set()
    plans = {}

    for name in CATALOGS:
        catalog_dir = workdir / f"{name}.duckdb"
        print(f"surveying {name}...", flush=True)
        plan = normalizer.survey_relayout(catalog_dir, object_keys)
        plans[name] = plan
        all_old_paths.update(fix.old_path for fix in plan.catalog_fixes)
        all_old_paths.update(delete.path for delete in plan.catalog_row_deletes)
        all_new_paths.update(fix.new_path for fix in plan.catalog_fixes)
        print(f"  {name}: {plan.summary()}", flush=True)

    print("relocating uncataloged objects...", flush=True)
    orphans = object_keys - all_old_paths
    orphan_plan = normalizer.relocate_uncataloged(orphans, set())
    plans["__orphans__"] = orphan_plan
    print(f"  orphans: {orphan_plan.summary()}", flush=True)

    if args.dry_run:
        print("DRY RUN — no changes applied", flush=True)
        return 0

    print("relocating objects (aliases kept at old keys)...", flush=True)
    aliases: dict[str, str] = {}
    for name, plan in plans.items():
        print(f"  applying {name}...", flush=True)
        aliases.update(
            normalizer.apply_renames_with_aliases(
                plan.object_renames,
                dry_run=False,
                object_sizes=object_sizes,
                workers=48,
            )
        )
        normalizer.apply_objects([], plan.object_deletes, dry_run=False)

    print("applying catalog updates...", flush=True)
    for name in CATALOGS:
        plan = plans[name]
        normalizer.apply_catalog(
            workdir / f"{name}.duckdb",
            plan.catalog_fixes,
            plan.catalog_row_deletes,
            dry_run=False,
        )

    print("uploading catalogs...", flush=True)
    import asyncio
    import json

    from pysus.api.ducklake.functional import upload_s3

    async def upload_all():
        for name in CATALOGS:
            await upload_s3(
                local_path=workdir / f"{name}.duckdb",
                remote_path=f"public/{name}.duckdb",
                access_key=env["ACCESS_KEY"],
                secret_key=env["SECRET_KEY"],
            )
            print(f"  uploaded {name}", flush=True)

    asyncio.run(upload_all())

    print("writing alias registry...", flush=True)
    registry_key = "public/data/.aliases.json"
    try:
        registry_obj = normalizer.client.get_object(
            Bucket="pysus", Key=registry_key
        )
        registry = json.loads(registry_obj["Body"].read())
    except Exception:  # noqa
        registry = {}
    registry.update(aliases)
    normalizer.client.put_object(
        Bucket="pysus",
        Key=registry_key,
        Body=json.dumps(registry, indent=2, sort_keys=True).encode(),
    )
    print(f"alias registry: {len(registry)} entries", flush=True)
    print("done", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
