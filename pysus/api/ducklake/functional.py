from pathlib import Path
from typing import Callable

from anyio import sleep, to_thread
import httpx
import boto3
from botocore.config import Config
from botocore import UNSIGNED

from pysus.api import types


async def download_http(
    remote_path: str,
    local_path: Path,
    callback: Callable[[int, int], None] | None = None,
) -> None:
    """Download *remote_path* to *local_path* with HTTP streaming and retries.

    Parameters
    ----------
    remote_path : str
        Object key within the bucket.
    local_path : Path
        Local destination path.
    callback : Callable[[int, int], None], optional
        Progress callback receiving ``(downloaded, total)`` bytes.
    """
    url = f"https://{types.S3_ENDPOINT}/{types.S3_BUCKET}/{remote_path}"
    max_retries = 5

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(follow_redirects=True) as client:
                async with client.stream("GET", url) as r:
                    r.raise_for_status()
                    total = int(r.headers.get("Content-Length", 0))
                    downloaded = 0
                    with open(local_path, "wb") as f:
                        async for chunk in r.aiter_bytes(chunk_size=1024 * 1024):
                            await to_thread.run_sync(f.write, chunk)
                            downloaded += len(chunk)
                            if callback:
                                callback(downloaded, total)
            return
        except OSError as e:
            if attempt < max_retries - 1:
                await sleep(1)
            else:
                raise e


async def download_s3(
    remote_path: str,
    local_path: Path,
    access_key: str | None = None,
    secret_key: str | None = None,
    callback: Callable[[int, int], None] | None = None,
) -> None:
    """Download *remote_path* to *local_path* using boto3 with optional credentials.

    Parameters
    ----------
    remote_path : str
        Object key within the bucket.
    local_path : Path
        Local destination path.
    access_key : str, optional
        S3 access key ID.
    secret_key : str, optional
        S3 secret access key.
    callback : Callable[[int, int], None], optional
        Progress callback receiving ``(downloaded, total)`` bytes.
    """
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
            args["config"] = Config(signature_version="s3v4")
        else:
            args["config"] = Config(signature_version=UNSIGNED)
        return args

    def _get_total_size(client_args) -> int:
        try:
            client = boto3.client(**client_args)
            meta = client.head_object(Bucket=types.S3_BUCKET, Key=remote_path)
            return int(meta.get("ContentLength", 0))
        except Exception:
            return 0

    def _download(client_args, total_size: int):
        client = boto3.client(**client_args)
        downloaded = 0

        def boto_callback(bytes_amount):
            nonlocal downloaded
            downloaded += bytes_amount
            if callback:
                callback(downloaded, total_size)

        client.download_file(
            Bucket=types.S3_BUCKET,
            Key=remote_path,
            Filename=str(local_path),
            Callback=boto_callback if callback else None,
        )

    for attempt in range(max_retries):
        try:
            client_args = _get_client_args()
            total_size = await to_thread.run_sync(_get_total_size, client_args)
            await to_thread.run_sync(_download, client_args, total_size)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                await sleep(1)
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
            args["config"] = Config(signature_version="s3v4")
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
            await to_thread.run_sync(_upload, client_args, total_size)
            return
        except Exception as e:
            if attempt < max_retries - 1:
                await sleep(1)
            else:
                raise e
