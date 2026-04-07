from __future__ import annotations

import pathlib
from datetime import datetime
from ftplib import FTP as FTPLib
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, TypedDict

import anyio
from pydantic import PrivateAttr
from pysus.api.models import BaseRemoteClient

if TYPE_CHECKING:
    from .models import Dataset, File


class FTPGroupInfo(TypedDict):
    name: str
    long_name: Optional[str]
    description: Optional[str]


class FTPFileInfo(TypedDict):
    name: str
    size: int
    type: str
    modify: datetime
    group: Optional[FTPGroupInfo]
    year: Optional[int]
    month: Optional[int]
    state: Optional[str]


class FTP(BaseRemoteClient):
    host: str = "ftp.datasus.gov.br"

    _ftp: Optional[FTPLib] = PrivateAttr(default=None)

    @property
    def name(self) -> str:
        return "FTP"

    @property
    def long_name(self) -> str:
        return "Pysus FTP Client"

    @property
    def description(self) -> str:
        return """
            O cliente FTP do pysus foi desenvolvido para fornecer uma interface
            assíncrona e moderna para navegação e extração de dados diretamente
            dos servidores do DATASUS. Ele resolve a complexidade de lidar com
            o protocolo FTP legado, transformando listagens de diretórios brutas
            em objetos Python estruturados e pesquisáveis.
        """

    @property
    def ftp(self) -> FTPLib:
        return self._ftp

    async def connect(self) -> None:
        def _connect():
            if self.ftp is None:
                self._ftp = FTPLib(self.host)
                self.ftp.login()

        await anyio.to_thread.run_sync(_connect)

    async def login(self, **kwargs) -> None:
        await self.connect()

    async def close(self) -> None:
        def _close():
            if self.ftp:
                try:
                    self.ftp.quit()
                except Exception:
                    self.ftp.close()
                finally:
                    self.ftp = None

        await anyio.to_thread.run_sync(_close)

    async def datasets(self, **kwargs) -> List[Dataset]:
        from .databases import AVAILABLE_DATABASES

        if self.ftp is None:
            raise ConnectionError(
                "FTP client is not connected. Call 'await client.login()'"
                " before accessing datasets."
            )

        return [d(client=self) for d in AVAILABLE_DATABASES]

    async def _download_file(
        self,
        file: File,
        output: pathlib.Path,
        callback: Optional[Callable[[int], None]] = None,
    ) -> pathlib.Path:
        def _fetch():
            try:
                self.ftp.voidcmd("NOOP")
            except (BrokenPipeError, Exception):
                self.connect()

            with open(output, "wb") as f:

                def _write_and_callback(chunk):
                    f.write(chunk)
                    if callback:
                        callback(len(chunk))

                self.ftp.retrbinary(f"RETR {file.path}", _write_and_callback)
            return output

        return await anyio.to_thread.run_sync(_fetch)

    @staticmethod
    def _line_parser(
        file_line: str,
        formatter: Optional[Callable[[str], Dict[str, Any]]] = None,
    ) -> FTPFileInfo:
        parts = file_line.strip().split()
        if len(parts) < 4:
            raise ValueError(f"Invalid FTP line: {file_line}")

        date_str, time_str = parts[0], parts[1]
        is_dir = parts[2].upper() == "<DIR>"
        name = " ".join(parts[3:])

        try:
            modify = datetime.strptime(
                f"{date_str} {time_str}", "%m-%d-%y %I:%M%p"
            )
        except ValueError:
            modify = datetime.now()

        info: FTPFileInfo = {
            "name": name,
            "size": 0 if is_dir else int(parts[2]),
            "type": "dir" if is_dir else "file",
            "modify": modify,
            "group": None,
            "year": None,
            "month": None,
            "state": None,
        }

        if formatter and not is_dir:
            info.update(formatter(name))

        return info

    async def _list_directory(
        self,
        path: str,
        formatter: Optional[Callable[[str], Dict[str, Any]]] = None,
    ) -> List[FTPFileInfo]:
        def _list():
            self.ftp.cwd(path)
            lines = []
            self.ftp.retrlines("LIST", lines.append)
            return [self._line_parser(line, formatter) for line in lines]

        return await anyio.to_thread.run_sync(_list)
