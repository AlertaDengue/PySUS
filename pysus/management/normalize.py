"""Bucket normalization: canonicalize S3 parquet keys and fix catalog paths.

Ensures every parquet object is stored under the hierarchical key
convention (``public/data/<origin>/<dataset>/<group>/<year>/<month>/
<state>/<STEM>.parquet``) and that the DuckLake catalog ``files.path``
rows point to the objects that actually exist.

Renames are copy+delete (S3 has no move). Attribute gaps are enriched
with the per-dataset formatters already shipped with the clients, so each
dataset's specific filename conventions drive the migration.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import boto3
from botocore.config import Config

from .records import compose_s3_key

_BUCKET = "pysus"
_ENDPOINT = "nbg1.your-objectstorage.com"
_REGION = "nbg1"


@dataclass
class ObjectRename:
    old: str
    new: str
    size: int = 0


@dataclass
class CatalogPathFix:
    catalog: str
    old_path: str
    new_path: str


@dataclass
class CatalogRowDelete:
    catalog: str
    path: str
    reason: str = ""


@dataclass
class NormalizePlan:
    object_renames: list[ObjectRename] = field(default_factory=list)
    object_deletes: list[str] = field(default_factory=list)
    catalog_fixes: list[CatalogPathFix] = field(default_factory=list)
    catalog_row_deletes: list[CatalogRowDelete] = field(default_factory=list)
    broken_rows: list[tuple[str, str]] = field(default_factory=list)
    raw_objects: list[str] = field(default_factory=list)

    def summary(self) -> dict:
        return {
            "object_renames": len(self.object_renames),
            "object_deletes": len(self.object_deletes),
            "catalog_fixes": len(self.catalog_fixes),
            "catalog_row_deletes": len(self.catalog_row_deletes),
            "broken_rows": len(self.broken_rows),
            "raw_objects": len(self.raw_objects),
        }


_FORMATTER_CACHE: dict[tuple[str, str], Callable | None] = {}


def formatter_for(origin: str, dataset: str) -> Callable | None:
    """Return the filename formatter for *origin*/*dataset*, if any.

    Formatters are the per-dataset parsers shipped with each client; they
    encode each dataset's specific filename conventions (group codes,
    state/month/year positions), so the migration stays data-driven.
    """
    key = (origin.strip().lower(), dataset.strip().upper())
    if key in _FORMATTER_CACHE:
        return _FORMATTER_CACHE[key]

    formatter: Callable | None = None
    try:
        if key[0] == "ftp":
            from pysus.api.ftp.databases import (
                AVAILABLE_DATABASES as FTP_DATABASES,
            )

            for ftp_class in FTP_DATABASES:
                if ftp_class.__name__.upper() == key[1]:
                    formatter = ftp_class.model_construct().formatter
                    break
        elif key[0] == "dadosgov":
            from pysus.api.dadosgov.databases import (
                AVAILABLE_DATABASES as DADOSGOV_DATABASES,
            )

            for gov_class in DADOSGOV_DATABASES:
                if gov_class.__name__.upper() == key[1]:
                    formatter = gov_class.model_construct().formatter
                    break
    except Exception:  # noqa
        formatter = None

    _FORMATTER_CACHE[key] = formatter
    return formatter


class BucketNormalizer:
    """Survey and normalize parquet object keys and catalog paths on S3."""

    def __init__(self, access_key: str, secret_key: str):
        self.client = boto3.client(
            "s3",
            endpoint_url=f"https://{_ENDPOINT}",
            region_name=_REGION,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version="s3v4"),
        )
        self.raw_objects: list[str] = []
        self.broken_rows: list[tuple[str, str]] = []

    # ------------------------------------------------------------------
    # survey
    # ------------------------------------------------------------------
    def _list_objects(self, prefix: str) -> list[tuple[str, int]]:
        paginator = self.client.get_paginator("list_objects_v2")
        objects: list[tuple[str, int]] = []
        for page in paginator.paginate(Bucket=_BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append((obj["Key"], obj["Size"]))
        return objects

    # ------------------------------------------------------------------
    # apply
    # ------------------------------------------------------------------
    def _copy_source(self, key: str, size: int | None = None) -> str:
        """Return the actual content key for *key*, following alias markers.

        Objects larger than an alias marker (a few hundred bytes) are
        never markers, so the HEAD can be skipped when *size* is known.
        """
        if size is not None and size > 4096:
            return key
        for _ in range(5):
            try:
                head = self.client.head_object(Bucket=_BUCKET, Key=key)
            except Exception:  # noqa
                return key
            target = head.get("Metadata", {}).get("pysus-alias")
            if not target:
                return key
            key = str(target)
        raise RuntimeError(f"Too many alias hops resolving {key}")

    def _do_relocate(self, rename: ObjectRename, sizes: dict[str, int]) -> None:
        from pysus.api.ducklake.functional import alias_marker

        source = self._copy_source(rename.old, sizes.get(rename.old))
        if source == rename.new:
            # already relocated by a previous run (old key holds an alias
            # marker pointing at the very same target)
            return
        self.client.copy_object(
            Bucket=_BUCKET,
            CopySource={"Bucket": _BUCKET, "Key": source},
            Key=rename.new,
        )
        self.client.put_object(
            Bucket=_BUCKET,
            Key=rename.old,
            Body=alias_marker(rename.new).encode(),
            Metadata={"pysus-alias": rename.new},
        )

    def apply_objects(
        self,
        renames: list[ObjectRename],
        deletes: list[str],
        dry_run: bool = True,
    ) -> None:
        """Apply renames (with aliases) and deletions on the bucket."""
        if renames:
            self.apply_renames_with_aliases(renames, dry_run=dry_run)

        for key in deletes:
            print(
                f"{'DRY ' if dry_run else ''}"
                f"delete {key} (duplicate format)"
            )
            if dry_run:
                continue
            self.client.delete_object(Bucket=_BUCKET, Key=key)

    def apply_renames_with_aliases(
        self,
        renames: list[ObjectRename],
        dry_run: bool = True,
        object_sizes: dict[str, int] | None = None,
        workers: int = 16,
    ) -> dict[str, str]:
        """Copy objects to their new keys and leave alias markers behind.

        Instead of deleting the old key, a tiny pointer object
        (``{"pysus-alias": "<new>"}`` content + ``pysus-alias`` custom
        metadata) is written there, keeping old paths resolvable for
        backwards compatibility. Sources that are themselves aliases are
        copied from their target, so re-runs never propagate markers.
        Copy+marker writes run in parallel (``workers`` threads); S3
        objects are immutable inputs, so ordering does not matter.
        """
        from concurrent.futures import ThreadPoolExecutor

        sizes = object_sizes or {}
        aliases: dict[str, str] = {}
        failures: dict[str, str] = {}
        total = len(renames)
        done = 0

        def apply_one(rename: ObjectRename) -> tuple[ObjectRename, str | None]:
            if not dry_run:
                try:
                    self._do_relocate(rename, sizes)
                except Exception as exc:  # noqa
                    return rename, str(exc)
            return rename, None

        if dry_run:
            for rename in renames:
                print(
                    f"DRY relocate {rename.old} "
                    f"-> {rename.new} (alias kept)"
                )
            return {}

        with ThreadPoolExecutor(max_workers=workers) as pool:
            for rename, error in pool.map(apply_one, renames):
                done += 1
                if error:
                    failures[rename.old] = error
                else:
                    aliases[rename.old] = rename.new
                if done % 250 == 0:
                    print(f"  progress: {done}/{total}", flush=True)

        if failures:
            print(f"  failures: {len(failures)}", flush=True)
            for old, error in list(failures.items())[:10]:
                print(f"    {old}: {error[:120]}", flush=True)

        return aliases

    def apply_catalog(
        self,
        catalog_dir: Path,
        fixes: list[CatalogPathFix],
        row_deletes: list[CatalogRowDelete] | None = None,
        dry_run: bool = True,
    ) -> None:
        """Update/delete ``pysus.files`` rows in-place in a local duckdb."""
        row_deletes = row_deletes or []
        if not fixes and not row_deletes:
            return
        import duckdb

        for fix in fixes:
            print(
                f"{'DRY ' if dry_run else ''}catalog[{fix.catalog}] "
                f"{fix.old_path} -> {fix.new_path}"
            )
        for delete in row_deletes:
            print(
                f"{'DRY ' if dry_run else ''}catalog[{delete.catalog}] "
                f"DELETE {delete.path} ({delete.reason})"
            )
        if dry_run:
            return

        con = duckdb.connect(str(catalog_dir))
        try:
            for fix in fixes:
                con.execute(
                    "UPDATE pysus.files SET path = ? WHERE path = ?",
                    (fix.new_path, fix.old_path),
                )
            for delete in row_deletes:
                con.execute(
                    "DELETE FROM pysus.file_columns WHERE file_id IN "
                    "(SELECT id FROM pysus.files WHERE path = ?)",
                    (delete.path,),
                )
                con.execute(
                    "DELETE FROM pysus.files WHERE path = ?",
                    (delete.path,),
                )
            con.execute("CHECKPOINT")
        finally:
            con.close()

    # ------------------------------------------------------------------
    # hierarchical relayout
    # ------------------------------------------------------------------
    def _enrich(
        self,
        origin: str,
        dataset: str,
        name: str,
        group: str | None,
        year: int | None,
        month: int | None,
        state: str | None,
    ) -> dict:
        """Fill attribute gaps using the dataset formatter.

        Catalog values win; formatter output fills missing values and
        replaces legacy directory names (e.g. group ``"Dados"``) with the
        parsed group code.
        """
        enriched = {
            "group": group,
            "year": year,
            "month": month,
            "state": state,
        }
        formatter = formatter_for(origin, dataset)
        if formatter is None:
            return enriched
        try:
            parsed = formatter(name)
        except Exception:  # noqa
            parsed = {}

        parsed_group = parsed.get("group")
        if parsed_group and isinstance(parsed_group, dict):
            parsed_group = parsed_group.get("name")

        if parsed_group and str(parsed_group) != enriched["group"]:
            # formatters are curated; catalog groups may be legacy
            # (e.g. directory names) or NULL
            enriched["group"] = str(parsed_group)
        if enriched["year"] is None and parsed.get("year"):
            enriched["year"] = int(parsed["year"])
        if enriched["month"] is None and parsed.get("month"):
            enriched["month"] = int(parsed["month"])
        if enriched["state"] is None and parsed.get("state"):
            enriched["state"] = str(parsed["state"])
        return enriched

    def survey_relayout(
        self,
        catalog_dir: Path,
        object_keys: set[str],
    ) -> NormalizePlan:
        """Plan the hierarchical relayout for one per-dataset catalog."""
        import duckdb

        plan = NormalizePlan()
        catalog = catalog_dir.name.removesuffix(".duckdb").removeprefix(
            "catalog_"
        )
        con = duckdb.connect(str(catalog_dir), read_only=True)
        try:
            rows = con.execute(
                "SELECT f.path, f.year, f.month, f.state, g.name, "
                "f.origin_path FROM pysus.files f "
                "LEFT JOIN pysus.dataset_groups g ON f.group_id = g.id"
            ).fetchall()
        finally:
            con.close()

        by_new: dict[str, list[tuple[str, str]]] = defaultdict(list)
        for path, year, month, state, group, origin_path in rows:
            origin, dataset = self._split_key(path)
            if origin is None or dataset is None:
                plan.broken_rows.append((catalog, path))
                continue

            source_name = (
                Path(origin_path).name if origin_path else Path(path).name
            )
            enriched = self._enrich(
                origin, dataset, source_name, group, year, month, state
            )
            new_key = compose_s3_key(
                origin=origin,
                dataset=dataset,
                name=source_name,
                group=enriched["group"],
                year=enriched["year"],
                month=enriched["month"],
                state=enriched["state"],
            )
            if new_key == path:
                continue
            by_new[new_key].append((path, source_name))

        for new_key, candidates in by_new.items():
            existing = [
                (old, src) for old, src in candidates if old in object_keys
            ]
            if not existing:
                for old, _ in candidates:
                    plan.catalog_row_deletes.append(
                        CatalogRowDelete(
                            catalog=catalog,
                            path=old,
                            reason="object missing (stale duplicate)",
                        )
                    )
                continue

            winner, _ = existing[0]
            plan.object_renames.append(ObjectRename(old=winner, new=new_key))
            plan.catalog_fixes.append(
                CatalogPathFix(
                    catalog=catalog, old_path=winner, new_path=new_key
                )
            )
            for old, _src in existing[1:]:
                plan.object_deletes.append(old)
                plan.catalog_row_deletes.append(
                    CatalogRowDelete(
                        catalog=catalog,
                        path=old,
                        reason=f"duplicate of {new_key}",
                    )
                )
            for old, _src in candidates:
                if old in object_keys:
                    continue
                plan.catalog_row_deletes.append(
                    CatalogRowDelete(
                        catalog=catalog,
                        path=old,
                        reason="object missing (stale duplicate)",
                    )
                )

        return plan

    @staticmethod
    def _split_key(path: str) -> tuple[str | None, str | None]:
        """Return ``(origin, dataset)`` from a ``public/data/...`` key."""
        parts = path.split("/")
        if len(parts) < 4 or parts[0] != "public" or parts[1] != "data":
            return None, None
        return parts[2].lower(), parts[3]

    def relocate_uncataloged(
        self,
        object_keys: set[str],
        cataloged_paths: set[str],
    ) -> NormalizePlan:
        """Plan hierarchical keys for objects missing from the catalogs."""
        plan = NormalizePlan()
        for key in sorted(object_keys):
            if key in cataloged_paths:
                continue
            origin, dataset = self._split_key(key)
            if origin is None or dataset is None:
                plan.raw_objects.append(key)
                continue
            name = Path(key).name
            enriched = self._enrich(
                origin, dataset, name, None, None, None, None
            )
            new_key = compose_s3_key(
                origin=origin,
                dataset=dataset,
                name=name,
                group=enriched["group"],
                year=enriched["year"],
                month=enriched["month"],
                state=enriched["state"],
            )
            if new_key == key:
                plan.raw_objects.append(key)
                continue
            plan.object_renames.append(ObjectRename(old=key, new=new_key))
        return plan
