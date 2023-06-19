from ftplib import FTP
from dateparser import parse
import datetime
from functools import lru_cache


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

    def __init__(
        self, path: str, name: str, size: int, date: datetime.datetime
    ) -> None:
        try:
            name, extension = name.split(".")
            self.name = name
            self.extension = extension
            self.basename = (".").join([name, extension])
        except ValueError:
            self.name = name
            self.extension = None
            self.basename = name
        self.path = path
        self.size = size
        self.date = date

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return str(self.name)

    def download(self):  # -> task.run
        """TODO"""
        ...


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
                data = line.strip().split()
                date = parse(" ".join([data[0], data[1]]))
                size = 0 if data[2] == "<DIR>" else int(data[2])
                name = data[3]
                files.append(File(path, name, size, date))

            self.ftp.retrlines("LIST", file_parse)
        self.ftp.close()

        return files
