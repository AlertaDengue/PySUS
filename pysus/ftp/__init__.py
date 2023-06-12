from ftplib import FTP
from dateparser import parse
import datetime
from functools import lru_cache


class File:
    def __init__(
        self, path: str, name: str, size: int, date: datetime.datetime
    ) -> None:
        self.path = path
        self.name = name
        self.size = size
        self.date = date

    def __str__(self) -> str:
        return str(self.name)

    def __repr__(self) -> str:
        return str(self.name)

    def download(self):  # -> task.run
        ...


class Database:
    """
    Base class for PySUS databases. Contains common functions
    for accessing DataSUS FTP server.
    """

    ftp: FTP
    name: str
    paths: list
    files: list
    metadata: dict

    def __init__(self) -> None:
        self.ftp = FTP("ftp.datasus.gov.br")
        self.files = self.all_files()

    def describe(self, file: File):
        pass

    def filter_files(self):
        pass

    @lru_cache
    def all_files(self) -> list:
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
