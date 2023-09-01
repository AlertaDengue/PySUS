from datetime import datetime
from ftplib import FTP
from functools import lru_cache
import os
import pathlib
from urllib.parse import urljoin


# aioftp.Client.parse_list_line_custom
def line_file_parser(file_line):
    file_line = file_line.decode(encoding="utf-8").rstrip()
    info = {}
    if "<DIR>" in file_line:
        date, time, _, *name = file_line.strip().split()
        info["size"] = 0
        info["type"] = "dir"
        name = " ".join(name)
    else:
        date, time, size, name = file_line.strip().split()
        info["size"] = size
        info["type"] = "file"

    modify = datetime.strptime(" ".join([date, time]), "%m-%d-%y %I:%M%p")
    info["modify"] = modify.strftime("%m/%d/%Y %I:%M%p")
    return pathlib.PurePosixPath(name), info


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

    def __init__(self, path: str, name: str, size: int, date: str) -> None:
        name, extension = os.path.splitext(name)
        self.name = name
        self.extension = extension
        self.basename = self.name + self.extension
        self.path = (
            path+"/"+self.basename 
            if not path.endswith("/") 
            else path+self.basename
        )
        self.size = size
        self.date = self.parse_date(date)

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return str(self.name)

    def __hash__(self): 
        return hash(self.path)

    def __eq__(self, other):
        if isinstance(other, File):
            return self.path == other.path
        return False

    def parse_date(self, date: str) -> datetime:
        return datetime.strptime(date, "%m-%d-%y %I:%M%p")

    def download(self):  # -> task.run
        """TODO"""
        ...


class Directory:
    """
    FTP Directory class. 

    Parameters
        path [str]: entire directory path where the directory is located
                    inside the FTP server
        name [str]: directory name
    """

    def __init__(self, path: str, name: str) -> None:
        self.basename = name
        self.path = urljoin(path, self.basename)

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
    paths: list
    files: list
    metadata: dict

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.files = self.all_files()

    def __repr__(self) -> str:
        return f'{self.name} - {self.metadata["long_name"]}'

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

    @lru_cache
    def all_files(self) -> list[File]:
        """
        This method is responsible for listing all the database's
        files when the database class is firstly called. It will
        convert the files found within the paths into `File`s,
        returning a cached list
        """
        files = list()

        self.ftp.login()
        for path in self.paths:
            self.ftp.cwd(path)

            def file_parse(line: str):
                date, time, size, name = line.strip().split()
                date = " ".join([date, time])
                if size == "<DIR>":
                    files.append(Directory(path, name))
                files.append(File(path, name, int(size), date))

            self.ftp.retrlines("LIST", file_parse)
        self.ftp.close()

        return files
