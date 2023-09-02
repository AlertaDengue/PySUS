from loguru import logger
from datetime import datetime
from ftplib import FTP
import os
import pathlib
from aioftp import Client
from typing import List, Optional, Union

from pysus.online_data import CACHEPATH


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
        size [int]: file size in bytes
        date [datetime]: last update date of the file in the FTP server

    Methods
        download(): TODO

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
        path [str]: entire directory path where the directory is located
                    inside the FTP server
        name [str]: directory name
    """

    def __init__(
        self,
        path: str,
        info: dict,
        content: Optional[List] = None,
    ) -> None:
        self.path = pathlib.PurePosixPath(path)
        self.info = info
        self.content = content

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


def list_path(path: str) -> List[Union[Directory, File]]:
    """
    This method is responsible for listing all the database's
    files when the database class is firstly called. It will
    convert the files found within the paths into `File`s,
    returning a list of Files and Directories.
    """
    content = list()
    ftp = FTP("ftp.datasus.gov.br")
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
            xpath = path + name if path.endswith("/") else path + "/" + name
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
    content: List[Union[Directory, File]]
    files: List[File]
    metadata: dict

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.content = self.load(self.paths)
        self.files = [f for f in self.content if isinstance(f, File)]

    def __repr__(self) -> str:
        return f'{self.name} - {self.metadata["long_name"]}'

    def load(self, paths: List[str]):
        """
        This method is responsible for listing all the database's
        files when the database class is firstly called. It will
        convert the files found within the paths into `File`s,
        and `Directory`(ies).
        """
        content = []
        for path in paths:
            content.extend(list_path(path))
        return content

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
