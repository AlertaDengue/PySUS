from collections.abc import Callable
from pathlib import Path

import boto3
import httpx
from anyio import fail_after, sleep, to_thread
from botocore import UNSIGNED
from botocore.config import Config
from pysus.api import types

ALIAS_META_HEADER = "x-amz-meta-pysus-alias"
MAX_ALIAS_HOPS = 5


def alias_marker(target_key: str) -> str:
    """Return the alias marker content pointing to *target_key*."""
    import json

    return json.dumps({"pysus-alias": target_key})


def _url_for_key(key: str) -> str:
    key = str(key).replace("\\", "/")
    return f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{key}"


async def _resolve_alias(url: str, client: httpx.AsyncClient) -> str:
    """Follow ``pysus-alias`` marker objects up to ``MAX_ALIAS_HOPS``."""
    for _ in range(MAX_ALIAS_HOPS):
        head = await client.head(url)
        if head.status_code == 404:
            return url
        head.raise_for_status()
        target = head.headers.get(ALIAS_META_HEADER)
        if not target or not isinstance(target, str):
            return url
        url = _url_for_key(target)
    raise RuntimeError(f"Too many alias hops resolving {url}")


async def download_http(
    remote_path: str,
    local_path: Path,
    callback: Callable[[int, int], None] | None = None,
) -> None:
    remote_path = str(remote_path).replace("\\", "/")
    url = f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{remote_path}"
    max_retries = 5

    timeout = httpx.Timeout(15.0, read=60.0, write=20.0, connect=15.0)
    limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://github.com/AlertaDengue/PySUS",
    }

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(
                headers=headers,
                follow_redirects=True,
                verify=False,
                limits=limits,
                timeout=timeout,
            ) as client:
                url = await _resolve_alias(url, client)
                async with client.stream("GET", url) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    downloaded = 0

                    with open(local_path, "wb") as f:
                        async for chunk in r.aiter_bytes(chunk_size=64 * 1024):
                            await to_thread.run_sync(f.write, chunk)
                            downloaded += len(chunk)
                            if callback:
                                callback(downloaded, total)
            return
        except (
            OSError,
            httpx.HTTPStatusError,
            httpx.ConnectError,
            httpx.ReadError,
            httpx.RemoteProtocolError,
        ) as e:
            if local_path.exists():
                try:
                    local_path.unlink()
                except OSError:
                    pass
            if attempt < max_retries - 1:
                await sleep(2 * (attempt + 1))
            else:
                raise e


async def upload_s3(
    local_path: Path,
    remote_path: str,
    access_key: str,
    secret_key: str,
    callback: Callable[[int, int], None] | None = None,
) -> None:
    max_retries = 5

    def _get_client_args():
        args: dict = {
            "service_name": "s3",
            "endpoint_url": f"https://{types.S3_ENDPOINT}",
            "region_name": types.S3_REGION,
        }
        if access_key and secret_key:
            args["aws_access_key_id"] = access_key
            args["aws_secret_access_key"] = secret_key
            args["config"] = Config(
                signature_version="s3v4",
                connect_timeout=20,
                read_timeout=120,
                retries={"max_attempts": 3, "mode": "standard"},
            )
        else:
            args["config"] = Config(signature_version=UNSIGNED)
        return args

    def _upload(client_args, total_size: int):
        client = boto3.client(**client_args)
        uploaded = 0

        def boto_callback(bytes_amount):
            nonlocal uploaded
            uploaded += bytes_amount
            if callback:
                callback(uploaded, total_size)

        client.upload_file(
            Filename=str(local_path),
            Bucket=types.S3_BUCKET,
            Key=remote_path,
            Callback=boto_callback if callback else None,
        )

    for attempt in range(max_retries):
        try:
            client_args = _get_client_args()
            total_size = local_path.stat().st_size
            with fail_after(600):
                await to_thread.run_sync(_upload, client_args, total_size)
            return
        except Exception as e:  # noqa
            if attempt < max_retries - 1:
                await sleep(1)
            else:
                raise e
