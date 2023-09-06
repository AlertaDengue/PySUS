import os
import pathlib
import asyncio
from datetime import datetime
from ftplib import FTP
from typing import Any, List, Optional, Set, Union

from aioftp import Client
from loguru import logger

CACHEPATH = os.getenv(
    "PYSUS_CACHEPATH", os.path.join(str(pathlib.Path.home()), "pysus")
)


def to_list(ite: Any) -> list:
    """Parse any builtin data type into a list"""
    return [ite] if type(ite) in [str, float, int] else list(ite)


class File:
    """
    FTP File class. This class will contain methods for interacting with
    files inside DataSUS FTP server. The databases will be responsible for
    parsing the files found for each db into File classes, enabling the
    databases' files to share state and its reusability.

    Parameters
        path [PurePosixPath]: entire directory path where the file is located
                              inside the FTP server
        name [str]: basename of the file
        info [dict]: a dict containing the keys [size, type, modify], which
                     are present in every FTP server. In PySUS, this info
                     is extract using `line_file_parser` with FTP LIST.

    Methods
        download(local_dir): extract the file to local_dir
        async_download(local_dir): async extract the file to local_dir
    """

    def __init__(self, path: str, name: str, info: dict) -> None:
        name, extension = os.path.splitext(name)
        self.name = name
        self.extension = extension
        self.basename = pathlib.PurePosixPath(self.name + self.extension)
        self.path = pathlib.PurePosixPath(
            path + str(self.basename)
            if path.endswith("/")
            else path + "/" + str(self.basename)
        )
        self.info = info

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

    def download(self, local_dir: str = CACHEPATH) -> str:
        dir = pathlib.Path(local_dir)
        dir.mkdir(exist_ok=True, parents=True)
        filepath = dir / self.basename

        if filepath.exists():
            return str(filepath)

        ftp = ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.retrbinary(
            f"RETR {self.path}",
            open(f"{filepath}", "wb").write,
        )
        ftp.close()
        logger.info(f"{self.basename} downloaded at {local_dir}")
        return str(filepath)

    async def async_download(self, local_dir: str = CACHEPATH) -> None:
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

            return pathlib.PurePosixPath(name), info

        dir = pathlib.Path(local_dir)
        dir.mkdir(exist_ok=True, parents=True)
        filepath = dir / self.basename

        output = (
            local_dir + str(self.basename)
            if local_dir.endswith("/")
            else local_dir + "/" + str(self.basename)
        )

        if filepath.exists():
            logger.debug(output)
            pass

        async with Client.context(
            host="ftp.datasus.gov.br", parse_list_line_custom=line_file_parser
        ) as client:
            await client.login()
            await client.download(self.path, output, write_into=True)
            logger.debug(output)


class Directory:
    """
    FTP Directory class.

    Parameters
        path [PurePosixPath]: entire directory path where the directory is located
                              inside the FTP server
        name [str]: directory name
        info [dict]: a dict containing the keys [size, type, modify], which
                     are present in every FTP server. In PySUS, this info
                     is extract using `line_file_parser` with FTP LIST.
                     OBS: A Directory should have size=0
    """

    __content__: Optional[List] = None

    def __init__(
        self,
        path: str,
        info: dict,
    ) -> None:
        self.path = pathlib.PurePosixPath(path)
        self.name = str(self.path).rsplit("/", maxsplit=1)[-1]
        self.info = info

    def __str__(self) -> str:
        return str(self.path)

    def __repr__(self) -> str:
        return str(self.path)

    def __hash__(self):
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, Directory):
            return self.path == other.path
        return False

    @property
    def content(self):
        """
        Returns a list of Files and Directories in the Directory
        """
        if not self.__content__:
            self.load()
        return self.__content__

    def load(self):
        """
        The content of a Directory must be explicity loaded
        """
        self.__content__ = list_path(self.path)


def list_path(path: str) -> List[Union[Directory, File]]:
    """
    This method is responsible for listing all the database's
    files when the database class is firstly called. It will
    convert the files found within the paths into `File`s,
    returning a list of Files and Directories.
    """
    path = str(path)
    content = []
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
                content.append(Directory(xpath, info))
            else:
                date, time, size, name = str(file_line).strip().split()
                info["size"] = size
                info["type"] = "file"
                modify = datetime.strptime(
                    " ".join([date, time]), "%m-%d-%y %I:%M%p"
                )
                info["modify"] = modify
                content.append(File(path, name, info))

        ftp.retrlines("LIST", line_file_parser)
    except Exception as exc:
        raise RuntimeError("Bad FTP Connection" + f"\n{exc}")
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
        paths [list[str]]: server paths where the files are located
        files [list[Files]]: list of parsed Files that will be loaded at
                             database instantiation.
        metadata [dict]: dict containing database's metadata information

    Methods
        describe(file): describes a file according to each database's
                        spec. Returns a dict with file information
        all_files(): runs at instantiation, enters the FTP server and reads
                     files within the paths, parsing them into File classes
                     and returning all Files as a list
    """

    ftp: FTP
    name: str
    paths: List[str]
    metadata: dict
    __content__: Set[Union[Directory, File]]

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.__content__ = set()

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
        return sorted(list(self.__content__), key=str)

    @property
    def files(self) -> List[File]:
        """
        Lists Files inside content. To load a specific Directory inside
        content, just `load()` this directory and list files again.
        """
        if not self.__content__:
            logger.info(
                "content is not loaded, use `load()` to load default paths"
            )
            return []
        return sorted(
            list(filter(lambda f: isinstance(f, File), self.__content__)),
            key=str,
        )

    def load(self, paths: Optional[List[str]] = None):
        """
        Loads specific paths to Database content, can receive Directories as well.
        It will convert the files found within the paths into Database.content.
        """
        if not paths:
            paths = self.paths

        if not isinstance(paths, list):
            raise ValueError("paths must a list")

        content = []
        for path in paths:
            content.extend(list_path(str(path)))
        self.__content__.update(set(content))
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
