from __future__ import annotations

__all__ = ["File", "Directory", "Database"]

import asyncio
import os
import pathlib
from datetime import datetime
from ftplib import FTP
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    TypedDict,
)

from aioftp import Client
from loguru import logger
from tqdm import tqdm
from typing_extensions import Self

from pysus import CACHEPATH
from pysus.api.models import BaseFormatter, BaseRemoteFile, FileDescription
from pysus.data.local import ParquetSet
from pysus.utils import to_list
from .client import FTPSingleton


DIRECTORY_CACHE: Dict[str, "Directory"] = {}
FileContent = Dict[str, Union["Directory", "File"]]


class FileInfo(TypedDict):
    """File information dictionary type"""

    size: Union[int, str]
    type: str
    modify: datetime


class File(BaseRemoteFile):
    def __init__(
        self,
        path: str,
        name: str,
        info: Dict[str, Any],
        **kwargs,
    ) -> None:
        name_no_ext, ext = os.path.splitext(name)
        path = f"{path}/{name}" if not path.endswith("/") else f"{path}{name}"

        super().__init__(
            basename=name,
            path=path,
            extension=ext,
            parent_path=path,
            **kwargs,
        )
        self._info = info

    @property
    def info(self) -> Dict[str, str]:
        modify = self._info.get("modify")

        return {
            "size": str(self._info.get("size", 0)),
            "type": f"{self.extension[1:].upper()}",
            "modify": (
                modify.strftime("%Y-%m-%d %I:%M%p")
                if isinstance(modify, datetime)
                else str(modify)
            ),
        }

    def describe(
        self,
        formatter: Optional[BaseFormatter] = None,
    ) -> FileDescription:
        if formatter:
            data = formatter.parse_filename(self.basename)
        else:
            data = {}

        return FileDescription(
            name=self.basename,
            group=data.get("group", "unknown"),
            year=data.get("year", 0),
            size=int(self._info.get("size", 0)),
            last_update=self._info.get("modify", datetime.now()),
            uf=data.get("uf"),
            month=data.get("month"),
        )

    async def _download(self, destination: pathlib.Path) -> ParquetSet:
        for ext in (".parquet", ".dbf", ""):
            existing = destination.with_suffix(ext)
            if existing.exists():
                return ParquetSet(str(existing))

        async with Client.context(
            host="ftp.datasus.gov.br", parse_list_line_custom=self._line_parser
        ) as client:
            await client.login()
            await client.download(self.path, str(destination), write_into=True)

        return ParquetSet(str(destination))

    @staticmethod
    def _line_parser(file_line: bytes) -> Tuple[str, Dict[str, Any]]:
        line = file_line.decode("utf-8")
        if "<DIR>" in line:
            date, time, _, *name_parts = line.strip().split()
            info = {"size": 0, "type": "dir"}
            name = " ".join(name_parts)
        else:
            date, time, size, name = line.strip().split()
            info = {"size": size, "type": "file"}

        modify = datetime.strptime(f"{date} {time}", "%m-%d-%y %I:%M%p")
        info["modify"] = modify
        return name, info

    def __str__(self) -> str:
        return self.basename

    def __repr__(self) -> str:
        return self.basename

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.path == other.path
        return False


class Directory:
    """
    Directory class with caching and lazy loading.

    The Directory class represents a directory in a file system and includes
    mechanisms for caching instances and lazy loading of directory content.
    When a Directory instance is created, it normalizes the provided path
    and caches the instance. The content of the directory is not loaded
    immediately; instead, it is loaded when the `content` property or the
    `load` method is accessed or called.

    Attributes:
        path (str): The normalized path of the directory.
        name (str): The name of the directory.
        parent (Directory): The parent directory instance.
        loaded (bool): Indicates whether the directory content has been loaded.
        __content__ (Dict[str, Union[File, Directory]]): A dictionary
            containing the directory's content, with names as keys and File or
            Directory instances as values.

    Methods:
        _normalize_path(path: str) -> str: Normalizes the given path.
        _get_root_directory() -> Directory: Returns the root directory
            instance, creating it if necessary.
        _init_root_child(name: str) -> None: Initializes a root child
            directory.
        _init_regular(parent_path: str, name: str) -> None: Initializes a
            regular directory.
        content() -> List[Union[Directory, File]]: Returns the content of the
            directory, loading it if necessary.
        load() -> Self: Loads the content of the directory and marks it as
            loaded.
    """

    name: str
    path: str
    parent: "Directory"
    loaded: bool
    __content__: Dict[str, Union[File, "Directory"]]

    def __new__(cls, path: str, _is_root_child: bool = False) -> "Directory":
        normalized_path = os.path.normpath(path)

        # Handle root directory case
        if normalized_path == "/":
            return cls._get_root_directory()

        # Return cached instance if exists
        if normalized_path in DIRECTORY_CACHE:
            return DIRECTORY_CACHE[normalized_path]

        # Use os.path.split for reliable path splitting
        parent_path, name = os.path.split(normalized_path)

        # Handle empty parent path
        if not parent_path:
            parent_path = "/"
        # Handle parent paths that don't start with /
        elif not parent_path.startswith("/"):
            parent_path = "/" + parent_path

        # Create new instance
        instance = super().__new__(cls)
        instance.path = normalized_path

        if _is_root_child:
            instance._init_root_child(name)
        else:
            instance._init_regular(parent_path, name)

        DIRECTORY_CACHE[normalized_path] = instance
        return instance

    @staticmethod
    def _normalize_path(path: str) -> str:
        """Normalizes the given path"""
        path = f"/{path}" if not path.startswith("/") else path
        return path.removesuffix("/")

    @classmethod
    def _get_root_directory(cls) -> Directory:
        """Returns the root directory instance, creating it if necessary"""
        if "/" not in DIRECTORY_CACHE:
            root = super().__new__(cls)
            root.parent = root
            root.name = "/"
            root.path = "/"
            root.loaded = False
            root.__content__ = {}
            DIRECTORY_CACHE["/"] = root
        return DIRECTORY_CACHE["/"]

    def _init_root_child(self, name: str) -> None:
        """Initializes a root child directory"""
        self.parent = DIRECTORY_CACHE["/"]
        self.name = name
        self.loaded = False
        self.__content__ = {}

    def _init_regular(self, parent_path: str, name: str) -> None:
        """Initializes a regular directory"""
        self.parent = Directory(parent_path)
        self.name = name
        self.loaded = False
        self.__content__ = {}

    @property
    def content(self) -> List[Union[Directory, File]]:
        """Returns the content of the directory, loading it if necessary"""
        if not self.loaded:
            self.load()
        return list(self.__content__.values())

    def load(self) -> Self:
        """Loads the content of the directory and marks it as loaded"""
        self.__content__ |= load_directory_content(self.path)
        self.loaded = True
        return self

    def reload(self):
        """
        Reloads the content of the Directory
        """
        self.loaded = False
        return self.load()

    def __str__(self) -> str:
        return self.path

    def __repr__(self) -> str:
        return self.path

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, Directory):
            return self.path == other.path
        return False


def load_directory_content(path: str) -> FileContent:
    """Directory content loading"""
    content: FileContent = {}

    try:
        ftp = FTPSingleton.get_instance()
        ftp.cwd(path)
        path = path.removesuffix("/")

        def line_parser(line: str):
            if "<DIR>" in line:
                date, time, _, name = line.strip().split(maxsplit=3)
                modify = datetime.strptime(f"{date} {time}", "%m-%d-%y %I:%M%p")
                info = {"size": 0, "type": "dir", "modify": modify}
                xpath = f"{path}/{name}"
                content[name] = Directory(xpath)
            else:
                date, time, size, name = line.strip().split(maxsplit=3)
                modify = datetime.strptime(f"{date} {time}", "%m-%d-%y %I:%M%p")
                info: FileInfo = {
                    "size": size,
                    "type": "file",
                    "modify": modify,
                }
                content[name] = File(path, name, info)

        ftp.retrlines("LIST", line_parser)
    except Exception as exc:
        raise exc
    finally:
        FTPSingleton.close()

    to_remove = [
        name
        for name in content
        if name.upper().endswith(".DBF")
        and name.upper().replace(".DBF", ".DBC") in content
    ]

    for name in to_remove:
        del content[name]

    return content


class Database:
    """
    Base class for PySUS databases. Contains common functions
    for accessing DataSUS FTP server. With this class, it is
    possible to construct database classes for different DataSUS
    files, sharing state and functionalities.

    Parameters
        ftp [FTP]: ftplib.FTP object for connecting in DataSUS server.
        name [str]: database name
        paths [list[Directory]]: server paths where the files are located
        files [list[Files]]: list of parsed Files from Database content
        metadata [dict]: dict containing database's metadata information

    Methods
        load(): Loads the database paths content to its own content
        describe(file): describes a file according to each database's
                        spec. Returns a dict with file information
        format(file): extracts from file name database related info, such as
                      year, month, UF and/or other useful info for the DB
        get_files(Any): filters files using database related format, depending
                        on the database's files specs
    """

    ftp: FTP
    name: str
    paths: Tuple[Directory, ...]
    metadata: dict
    __content__: Dict[str, Union[Directory, File]]

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.__content__ = {}

    def __repr__(self) -> str:
        return f"{self.name} - {self.metadata['long_name']}"

    @property
    def content(self) -> List[Union[Directory, File]]:
        """
        Lists Database content. The `paths` will be loaded if this property is
        called or if explicitly using `load()`. To add specific Directory
        inside content, `load()` the directory and call `content` again.
        """
        if not self.__content__:
            logger.info(
                "content is not loaded, use `load()` to load default paths")
            return []
        return sorted(list(self.__content__.values()), key=str)

    @property
    def files(self) -> List[File]:
        """
        Lists Files inside content. To load a specific Directory inside
        content, just `load()` this directory and list files again.
        """
        return [f for f in self.content if isinstance(f, File)]

    def load(
        self,
        directories: Optional[
            Union[Directory, List[Directory], Tuple[Directory, ...]]
        ] = None,
    ) -> Database:
        """
        Loads specific directories to Database content. Will aggregate the
        files found within Directories into Database.content.
        """
        if not directories:
            directories = list(self.paths)

        directories_list = to_list(directories)

        for directory in directories_list:
            if not isinstance(directory, Directory):
                raise ValueError("Invalid directory provided.")

            directory.load()
            self.__content__.update(directory.__content__)
        return self

    def describe(self, file: File) -> dict:
        """
        Receives a `File` and returns a dict with its information,
        according to the database's specifications. This method is
        helpful to return the FTP's file in a humanized format

        Parameters
            file [File]: a `File` instance
        """
        ...

    def format(self, file: File) -> tuple:
        """
        Formats a File based on the database specifications,
        extracting its name's parameters given a pattern.

        Parameters
            file [File]: a `File` instance
        """
        ...

    def get_files(self, *args, **kwargs) -> list[File]:
        """
        Filters the list of `File`s according to each database file
        pattern, as UFs, Groups, Years, Months, etc. This method will
        also be responsible to look for wrong values within the file
        pattern and possible extra characters in its basename
        """
        ...

    def download(self, files: List[File], local_dir: str = CACHEPATH) -> List[str]:
        """
        Downloads a list of Files.
        """
        files = to_list(files)
        pbar = tqdm(total=len(files), dynamic_ncols=True)
        dfiles = []
        for file in files:
            if isinstance(file, File):
                dfiles.append(file.download(local_dir=local_dir, _pbar=pbar))
        pbar.close()
        if len(dfiles) == 1:
            return dfiles[0]
        return dfiles

    async def async_download(self, files: List[File], local_dir: str = CACHEPATH):
        """
        Asynchronously downloads a list of files
        """

        async def download_file(file):
            if isinstance(file, File):
                await file.async_download(local_dir=local_dir)

        tasks = [download_file(file) for file in files]
        await asyncio.gather(*tasks)
