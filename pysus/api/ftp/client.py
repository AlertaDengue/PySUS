from __future__ import annotations

import pathlib
from ftplib import FTP
from typing import Final, Optional, Protocol, runtime_checkable

from pysus import CACHEPATH

__cachepath__: Final[pathlib.Path] = pathlib.Path(CACHEPATH)
__cachepath__.mkdir(exist_ok=True)


@runtime_checkable
class Downloadable(Protocol):
    async def download(self, local_dir: str):
        """Protocol for downloadable objects"""
        ...


class FTPSingleton:
    """Singleton FTP client manager"""

    _instance: Optional[FTP] = None

    @classmethod
    def get_instance(cls) -> FTP:
        """Get or create the singleton FTP instance"""
        if cls._instance is None or not cls._instance.sock:
            cls._instance = FTP("ftp.datasus.gov.br")
            cls._instance.login()
        return cls._instance

    @classmethod
    def close(cls) -> None:
        """Close the singleton FTP instance"""
        if cls._instance and cls._instance.sock:
            cls._instance.close()
            cls._instance = None
