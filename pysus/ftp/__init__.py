import asyncio
import os
import pathlib
from datetime import datetime
from ftplib import FTP
from typing import Any, Dict, List, Optional, Self, Set, Union

import humanize
from aioftp import Client
from loguru import logger

from pysus.data.local import Data


CACHEPATH = os.getenv(
    "PYSUS_CACHEPATH", os.path.join(str(pathlib.Path.home()), "pysus")
)


def to_list(ite: Any) -> list:
    """Parse any builtin data type into a list"""
    return (
        [ite] if type(ite) in [str, float, int, Directory, File] else list(ite)
    )


class File:
    """
    FTP File class. This class will contain methods for interacting with
    files inside DataSUS FTP server. The databases will be responsible for
    parsing the files found for each db into File classes, enabling the
    databases' files to share state and its reusability.

    Parameters
        path [str]: entire directory path where the file is located
                              inside the FTP server
        name [str]: basename of the file
        info [dict]: a dict containing the keys [size, type, modify], which
                     are present in every FTP server. In PySUS, this info
                     is extract using `line_file_parser` with FTP LIST.

    Methods
        download(local_dir): extract the file to local_dir
        async_download(local_dir): async extract the file to local_dir
    """

    name: str
    extension: str
    basename: str
    path: str
    __info__: Set[Union[int, str, datetime]]

    def __init__(self, path: str, name: str, info: dict) -> None:
        name, extension = os.path.splitext(name)
        self.name = name
        self.extension = extension
        self.basename = self.name + self.extension
        self.path = (
            path + self.basename
            if path.endswith("/")
            else path + "/" + self.basename
        )
        self.__info__ = info

    def __str__(self) -> str:
        return str(self.basename)

    def __repr__(self) -> str:
        return str(self.basename)

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.path == other.path
        return False

    @property
    def info(self):
        """
        Parse File info to human format
        """
        info = {}
        info["size"] = humanize.naturalsize(self.__info__["size"])
        info["type"] = self.extension[1:].upper() + " file"
        info["modify"] = self.__info__["modify"].strftime("%Y-%m-%d %I:%M%p")
        return info

    def download(self, local_dir: str = CACHEPATH) -> Data:
        _dir = pathlib.Path(local_dir)
        _dir.mkdir(exist_ok=True, parents=True)
        filepath = _dir / self.basename

        if filepath.with_suffix(".DBF").exists():
            return Data(str(filepath.with_suffix(".DBF")))

        if filepath.exists():
            return Data(str(filepath))

        try:
            ftp = ftp = FTP("ftp.datasus.gov.br")
            ftp.login()
            output = open(f"{filepath}", "wb")
            ftp.retrbinary(
                f"RETR {self.path}",
                output.write,
            )
        except Exception as exc:
            raise exc
        finally:
            ftp.close()
            output.close()

        return Data(str(filepath))

    async def async_download(self, local_dir: str = CACHEPATH) -> Data:
        # aioftp.Client.parse_list_line_custom
        def line_file_parser(file_line):
            line = file_line.decode("utf-8")
            info = {}
            if "<DIR>" in line:
                date, time, _, *name = str(line).strip().split()
                info["size"] = 0
                info["type"] = "dir"
                name = " ".join(name)
            else:
                date, time, size, name = str(line).strip().split()
                info["size"] = size
                info["type"] = "file"

            modify = datetime.strptime(
                " ".join([date, time]), "%m-%d-%y %I:%M%p"
            )
            info["modify"] = modify.strftime("%m/%d/%Y %I:%M%p")

            return name, info

        _dir = pathlib.Path(local_dir)
        _dir.mkdir(exist_ok=True, parents=True)
        filepath = _dir / self.basename

        output = (
            local_dir + str(self.basename)
            if local_dir.endswith("/")
            else local_dir + "/" + str(self.basename)
        )

        if filepath.exists():
            return Data(output)

        async with Client.context(
            host="ftp.datasus.gov.br",
            parse_list_line_custom=line_file_parser,
        ) as client:
            await client.login()
            await client.download(self.path, output, write_into=True)

        return Data(output)


CACHE: Dict = {}


class Directory:
    """
    FTP Directory class. The Directory does not load its content when called.
    Instead, it will cache all the parents Directories until root "/". To load
    the content, the attr content or the method load() should be called. When
    firstly instantiated, it will CWD into the path provided and store self and
    all parents in cache

    Parameters
        path [str]: entire directory path where the directory is located
                    inside the FTP server
    Attrs
        name [str]: Directory name
        path [str]: Directory path
        parent [Directory]: parent Directory
        loaded [bool]: True if content is loaded
        content [dict[str:[File, Directory]]]: A dictionary with name and File
            or Directory inside the Directory (e.g: "name": Directory("name"))
    """

    name: str
    path: str
    parent: Self  # Directory?
    loaded: bool = False
    __content__: Dict = {}

    def __new__(cls, path: str, _is_root_child=False) -> Self:
        ftp = FTP("ftp.datasus.gov.br")
        path = f"/{path}" if not str(path).startswith("/") else path
        path = path[:-1] if path.endswith("/") else path

        if not path:  # if root ("/")
            path = "/"
            try:
                directory = CACHE["/"]
            except KeyError:
                directory = object.__new__(cls)
                directory.parent = directory
                directory.name = "/"
                directory.path = "/"
                directory.loaded = False
                directory.__content__ = {}
                CACHE["/"] = directory
            return directory

        parent_path, name = path.rsplit("/", maxsplit=1)

        if _is_root_child:
            # WARNING: This parameter is for internal meanings, do not use
            directory = object.__new__(cls)
            directory.parent = CACHE["/"]
            directory.name = name
            CACHE[path] = directory
            return directory

        try:
            directory = CACHE[path]  # Recursive and cached instantiation
        except KeyError:
            try:
                ftp.connect()
                ftp.login()
                ftp.cwd(path)  # Checks if parent dir exists on DATASUS
            except Exception as exc:
                if "cannot find the path" in str(exc):
                    logger.error(f"Not a directory {path}")
                elif "access is denied" in str(exc).lower():
                    #  Forbidden access, exists in ftp but returns Forbidden
                    directory = object.__new__(cls)
                    directory.parent = Directory(parent_path)  # Recursive
                    directory.name = name
                    directory.loaded = False
                    directory.__content__ = {}
                    CACHE[path] = directory
                    return directory
                raise exc
            finally:
                ftp.close()

            directory = object.__new__(cls)
            # TODO: In next step, all the parent directories will be generated,
            # but it cwds into every parent, while its certain that they exist
            # in ftp server. The best approach should be to skip the cwds
            directory.parent = Directory(parent_path)  # Recursive
            directory.name = name
            directory.loaded = False
            directory.__content__ = {}
            CACHE[path] = directory
        return directory

    def __init__(self, path: str, _is_root_child=False) -> None:
        path = f"/{path}" if not str(path).startswith("/") else path
        path = path[:-1] if path.endswith("/") else path
        if not path:
            path = "/"
        self.path = path

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

    def __truediv__(self, path: str):
        if isinstance(path, str):
            path = f"/{path}" if not path.startswith("/") else path
            path = path[:-1] if path.endswith("/") else path
            return Directory(self.path + path)
        raise ValueError("Unsupported division")

    @property
    def content(self):
        """
        Returns a list of Files and Directories in the Directory, will load
        if needed
        """
        if not self.loaded:
            self.load()
        return list(self.__content__.values())

    def load(self):
        """
        The content of a Directory must be explicity loaded
        """
        self.__content__ |= load_path(self.path)
        self.loaded = True
        return self

    def reload(self):
        """
        Reloads the content of the Directory
        """
        self.loaded = False
        return self.load()


CACHE["/"] = Directory("/")


def load_path(path: str) -> Dict[str, Union[Directory, File]]:
    """
    This method is responsible for listing all the FTP directory's.
    Converts the items found within a valid DATASUS path into `File`s or
    Directories, returning its content.
    """
    path = str(path)
    content = {}
    ftp = FTP("ftp.datasus.gov.br")

    try:
        ftp.connect()
        ftp.login()
        ftp.cwd(path)

        def line_file_parser(file_line):
            info = {}
            if "<DIR>" in file_line:
                date, time, _, *name = str(file_line).strip().split()
                info["size"] = 0
                info["type"] = "dir"
                name = " ".join(name)
                modify = datetime.strptime(
                    " ".join([date, time]), "%m-%d-%y %I:%M%p"
                )
                info["modify"] = modify
                xpath = (
                    path + name if path.endswith("/") else path + "/" + name
                )
                content[name] = Directory(xpath)
            else:
                date, time, size, name = str(file_line).strip().split()
                info["size"] = size
                info["type"] = "file"
                modify = datetime.strptime(
                    " ".join([date, time]), "%m-%d-%y %I:%M%p"
                )
                info["modify"] = modify
                content[name] = File(path, name, info)

        ftp.retrlines("LIST", line_file_parser)
    except Exception as exc:
        raise exc
    finally:
        ftp.close()

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
    paths: List[Directory]
    metadata: dict
    __content__: Dict[str, Union[Directory, File]]

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.__content__ = {}

    def __repr__(self) -> str:
        return f'{self.name} - {self.metadata["long_name"]}'

    @property
    def content(self) -> List[Union[Directory, File]]:
        """
        Lists Database content. The `paths` will be loaded if this property is
        called or if explicty using `load()`. To add specific Directory inside
        content, `load()` the directory and call `content` again.
        """
        if not self.__content__:
            logger.info(
                "content is not loaded, use `load()` to load default paths"
            )
            return []
        return sorted(list(self.__content__.values()), key=str)

    @property
    def files(self) -> List[File]:
        """
        Lists Files inside content. To load a specific Directory inside
        content, just `load()` this directory and list files again.
        """
        return list(filter(lambda f: isinstance(f, File), self.content))

    def load(
        self, directories: Optional[Union[Directory, List[Directory]]] = None
    ) -> Self:
        """
        Loads specific directories to Database content. Will aggregate the
        files found within Directories into Database.content.
        """
        if not directories:
            directories = self.paths

        directories = to_list(directories)

        for i, path in enumerate(directories):
            if isinstance(path, str):
                path = Directory(path)
                directories[i] = path

            if not isinstance(path, Directory):
                raise ValueError("path must a valid DATASUS directory")

        for directory in directories:
            directory.load()
            self.__content__ |= directory.__content__
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

    def download(
        self, files: List[File], local_dir: str = CACHEPATH
    ) -> List[str]:
        """
        Downloads a list of Files.
        """
        dfiles = []
        for file in files:
            if isinstance(file, File):
                dfiles.append(file.download(local_dir=local_dir))
        return dfiles

    async def async_download(
        self, files: List[File], local_dir: str = CACHEPATH
    ):
        """
        Asynchronously downloads a list of files
        """

        async def download_file(file):
            if isinstance(file, File):
                await file.async_download(local_dir=local_dir)

        tasks = [download_file(file) for file in files]
        await asyncio.gather(*tasks)
