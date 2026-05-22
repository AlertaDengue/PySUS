"""Async FTP client wrapping the standard ftplib for DATASUS data access."""

from __future__ import annotations

import pathlib
from collections.abc import Callable
from datetime import datetime
from ftplib import FTP as FTPLib
from typing import TYPE_CHECKING, Any, TypedDict

from anyio import to_thread
from pydantic import PrivateAttr
from pysus.api.models import BaseRemoteClient, BaseRemoteFile

if TYPE_CHECKING:
    from pysus.api.ftp.models import Dataset
    from pysus.api.types import State


class FTPGroupInfo(TypedDict):
    """Metadata describing a file group within a dataset."""

    name: str
    long_name: str | None
    description: str | None


class FTPFileInfo(TypedDict):
    """Parsed metadata for a file or directory entry from an FTP listing."""

    name: str
    size: int
    type: str
    modify: datetime
    group: FTPGroupInfo | None
    year: int | None
    month: int | None
    state: State | None


class FTP(BaseRemoteClient):
    """Async FTP client for navigating and downloading DATASUS data."""

    host: str = "ftp.datasus.gov.br"
    timeout: int = 60

    _ftp: FTPLib | None = PrivateAttr(default=None)

    @property
    def name(self) -> str:
        """Return the short name of this client.

        Returns
        -------
        str
            The client short name ("FTP").
        """
        return "FTP"

    @property
    def long_name(self) -> str:
        """Return the human-readable name of this client.

        Returns
        -------
        str
            The human-readable client name.
        """
        return "Pysus FTP Client"

    @property
    def description(self) -> str:
        """Return a description of this client's purpose.

        Returns
        -------
        str
            A description string explaining the FTP client's capabilities.
        """
        return """
            O cliente FTP do pysus foi desenvolvido para fornecer uma interface
            assíncrona e moderna para navegação e extração de dados diretamente
            dos servidores do DATASUS. Ele resolve a complexidade de lidar com
            o protocolo FTP legado, transformando listagens de diretórios brutas
            em objetos Python estruturados e pesquisáveis.
        """

    @property
    def ftp(self) -> FTPLib | None:
        """Return the underlying ftplib.FTP, or None if not connected.

        Returns
        -------
        FTPLib | None
            The ftplib.FTP instance, or None if not connected.
        """
        return self._ftp

    async def connect(self) -> None:
        """Establish the FTP connection to the remote host.

        Raises
        ------
        Exception
            Any exception raised by ftplib during connection.
        """

        def _connect():
            if self.ftp is None:
                self._ftp = FTPLib(self.host, timeout=self.timeout)
                self.ftp.login()

        await to_thread.run_sync(_connect)

    async def login(self, **kwargs) -> None:
        """Authenticate and connect to the FTP server (alias for connect).

        Parameters
        ----------
        ``**kwargs``
            Forwarded to connect() (currently unused).

        Raises
        ------
        Exception
            Any exception raised by ftplib during authentication.
        """
        await self.connect()

    async def close(self) -> None:
        """Close the FTP connection and reset the internal client state.

        Raises
        ------
        Exception
            Any exception raised by ftplib during disconnection.
        """

        def _close():
            if self.ftp:
                try:
                    self.ftp.quit()
                except Exception:  # noqa
                    self.ftp.close()
                finally:
                    self._ftp = None

        await to_thread.run_sync(_close)

    async def datasets(self, **kwargs) -> list[Dataset]:
        """Return a list of all available dataset instances for this client.

        Returns
        -------
        list[:class:`~pysus.api.ftp.models.Dataset`]
            A list of Dataset instances for all available databases.

        Raises
        ------
        ConnectionError
            If the FTP client is not connected.
        """
        from .databases import AVAILABLE_DATABASES

        if self.ftp is None:
            raise ConnectionError(
                "FTP client is not connected. Call 'await client.login()'"
                " before accessing datasets."
            )

        return [d(client=self) for d in AVAILABLE_DATABASES]

    async def _download_file(
        self,
        file: BaseRemoteFile,
        output: pathlib.Path,
        callback: Callable[..., None] | None = None,
    ) -> pathlib.Path:
        """Download a remote file locally, optionally reporting progress."""

        async def _fetch():
            try:
                self.ftp.voidcmd("NOOP")
            except BrokenPipeError:
                await self.connect()

            total_size = self.ftp.size(str(file.path)) or 0
            current_size = 0

            with open(output, "wb") as f:

                def _write_and_callback(chunk):
                    nonlocal current_size
                    f.write(chunk)
                    current_size += len(chunk)
                    if callback:
                        callback(current_size, total_size)

                self.ftp.retrbinary(f"RETR {file.path}", _write_and_callback)
            return output

        return await _fetch()

    @staticmethod
    def _line_parser(
        file_line: str,
        formatter: Callable[[str], dict[str, Any]] | None = None,
    ) -> FTPFileInfo:
        """Parse a line from a DATASUS FTP LIST response into FTPFileInfo."""
        parts = file_line.strip().split()
        if len(parts) < 4:
            raise ValueError(f"Invalid FTP line: {file_line}")

        date_str, time_str = parts[0], parts[1]
        is_dir = parts[2].upper() == "<DIR>"
        name = " ".join(parts[3:])

        try:
            modify = datetime.strptime(
                f"{date_str} {time_str}",
                "%m-%d-%y %I:%M%p",
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
            info.update(formatter(name))  # type: ignore

        return info

    async def _list_directory(
        self,
        path: str,
        formatter: Callable[[str], dict[str, Any]] | None = None,
    ) -> list[FTPFileInfo]:
        """List the contents of a remote directory and parse each entry."""

        def _list():
            self.ftp.cwd(path)
            lines = []
            self.ftp.retrlines("LIST", lines.append)
            return [self._line_parser(line, formatter) for line in lines]

        return await to_thread.run_sync(_list)
