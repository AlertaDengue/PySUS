"""Typer CLI for the management module (S3 database sync).

This sub-app is only registered when the management module is installed.
It is excluded from the published PyPI package (see ``pyproject.toml``)
and is meant for developers who maintain the S3 bucket.

Commands
--------
pysus management check [NAME...]   Check every source for files to
                                   update/upload on S3 (dry run)
pysus management check --apply     Actually update/upload the files
pysus management check --json      Stream one JSON object per file
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import typer
from pysus import CACHEPATH
from pysus.management.records import load_journal_keys

app = typer.Typer(help="Manage the S3 databases (dev only, not on PyPI)")


def _load_env(path: str = ".env") -> dict[str, str]:
    """Read ACCESS_KEY/SECRET_KEY/DADOSGOV_TOKEN from an env file."""
    env_path = Path(path)
    if not env_path.exists():
        return {}
    env: dict[str, str] = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def _parse_date(value: str | None) -> datetime | None:
    """Parse a ``YYYY-MM-DD`` CLI value into a midnight UTC datetime."""
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d")


def _journal_path(
    resume: str | None, reupload_before: str | None
) -> Path | None:
    """Return the resume journal path for this invocation."""
    if resume:
        return Path(resume)
    if reupload_before:
        return (
            Path(CACHEPATH)
            / "management"
            / "journal"
            / f"reupload-{reupload_before}.jsonl"
        )
    return None


@app.command()
def check(
    name: list[str] = typer.Argument(  # noqa: B008
        None,
        metavar="NAME",
        help="Database(s) to check (e.g. SINAN). Omit to check all.",
    ),
    force: bool = typer.Option(  # noqa: B008
        False,
        "--force",
        help="Reprocess files even when the catalog is current",
    ),
    reupload_before: str = typer.Option(  # noqa: B008
        None,
        "--reupload-before",
        metavar="DATE",
        help="Reprocess files cataloged before DATE (YYYY-MM-DD), e.g. "
        "to regenerate artifacts from an older converter",
    ),
    resume: str = typer.Option(  # noqa: B008
        None,
        "--resume",
        metavar="PATH",
        help="Resume journal to continue from (default: derived from "
        "--reupload-before)",
    ),
    apply: bool = typer.Option(  # noqa: B008
        False,
        "--apply",
        help="Actually update/upload the files (default is a check only)",
    ),
    json_out: bool = typer.Option(  # noqa: B008
        False,
        "--json",
        help="Stream one JSON object per file to stdout",
    ),
    workers: int = typer.Option(  # noqa: B008
        16, "--workers", help="Concurrent ingestion workers"
    ),
    ftp_connections: int = typer.Option(  # noqa: B008
        6, "--ftp-connections", help="FTP connection pool size"
    ),
    checkpoint_every: int = typer.Option(  # noqa: B008
        500, "--checkpoint-every", help="Upload catalogs to S3 every N uploads"
    ),
    env_file: str = typer.Option(  # noqa: B008
        ".env",
        "--env-file",
        help="Path to a file with ACCESS_KEY/SECRET_KEY/DADOSGOV_TOKEN",
    ),
):
    """Check every source against the S3 databases.

    By default this is a dry run: it only reports which files would need
    to be updated/uploaded (``needs_update``) and which are already at
    the most updated version (``skipped``), without touching S3. Pass
    ``--apply`` to actually download, convert, upload and catalog the
    outdated files. Use ``--json`` to stream machine-readable results.

    A run is resumable: each completed file is appended to a journal
    (``--resume``, or derived from ``--reupload-before``), and a paused
    run can be resumed with the same command to skip already-processed
    files.
    """
    from pysus.api.client import _run_sync
    from pysus.management.sync import SyncEngine

    env = _load_env(env_file)
    engine = SyncEngine(
        access_key=env.get("ACCESS_KEY"),
        secret_key=env.get("SECRET_KEY"),
        dadosgov_token=env.get("DADOSGOV_TOKEN"),
    )

    journal = _journal_path(resume, reupload_before)
    resume_keys = set()
    if apply and journal is not None and journal.exists():
        resume_keys = load_journal_keys(journal)

    counts: dict[str, int] = {}

    def on_outcome(outcome) -> None:
        counts[outcome.status] = counts.get(outcome.status, 0) + 1
        if json_out:
            typer.echo(
                json.dumps(
                    {
                        "status": outcome.status,
                        "dataset": outcome.key.dataset,
                        "group": outcome.key.group,
                        "year": outcome.key.year,
                        "month": outcome.key.month,
                        "state": outcome.key.state,
                        "stem": outcome.key.stem,
                        "origin": outcome.origin,
                        "detail": outcome.detail,
                    },
                    default=str,
                )
            )
            return
        if outcome.status == "needs_update":
            typer.echo(f"[needs_update] {outcome.detail}")
        elif outcome.status in ("uploaded", "failed", "needs_token"):
            typer.echo(f"[{outcome.status}] {outcome.detail}")
        total = sum(counts.values())
        if total % 500 == 0:
            typer.echo(f"progress: {counts}", err=True)

    def _flush() -> None:
        import sys

        sys.stdout.flush()
        sys.stderr.flush()

    async def _run():
        async with engine:
            report = await engine.run(
                datasets=[d.upper() for d in name] if name else None,
                force=force,
                reupload_before=_parse_date(reupload_before),
                dry_run=not apply,
                workers=workers,
                ftp_connections=ftp_connections,
                checkpoint_every=checkpoint_every if apply else None,
                on_outcome=on_outcome,
                resume=resume_keys or None,
                journal=journal if apply else None,
            )
        summary = report.summary()
        _flush()
        if json_out:
            typer.echo(json.dumps({"summary": summary}), err=True)
            _flush()
            return summary
        typer.echo(
            "\n"
            f"  total: {summary['total']}\n"
            f"  needs_update: {summary['needs_update']}\n"
            f"  uploaded: {summary['uploaded']}\n"
            f"  skipped: {summary['skipped']}\n"
            f"  failed: {summary['failed']}\n"
            f"  needs_token: {summary['needs_token']}"
        )
        _flush()
        return summary

    summary = _run_sync(_run())
    if summary["failed"]:
        raise typer.Exit(code=1)
