import os
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Dict, List, Union

from loguru import logger
from pysus.data import dbc_to_dbf, dbf_to_parquet


class ParquetSet:
    """
    A local parquet directory or file
    """

    __path__: Union[PurePosixPath, PureWindowsPath]
    info: Dict

    def __init__(self, path: str) -> None:
        info = {}
        path = Path(path)

        if path.suffix.lower() not in [".parquet", ".dbc", ".dbf"]:
            raise NotImplementedError(f"Unknown file type: {path.suffix}")

        if path.suffix.lower() == ".dbc":
            path = Path(dbc_to_dbf(path))

        if path.suffix.lower() == ".dbf":
            path = Path(dbf_to_parquet(path))

        if path.is_dir():
            info["size"] = sum(
                f.stat().st_size for f in path.glob("**/*") if f.is_file()
            )
        else:
            info["size"] = os.path.getsize(str(path))

        self.__path__ = path
        self.info = info

    def __str__(self):
        return str(self.__path__)

    def __repr__(self):
        return str(self.__path__)

    def __hash__(self):
        return hash(str(self.__path__))

    @property
    def path(self) -> str:
        return str(self.__path__)


def parse_data_content(
    path: Union[List[str], str]
) -> Union[ParquetSet, List[ParquetSet]]:
    if isinstance(path, str):
        path = [path]
    else:
        path = list(path)

    content = []
    for _path in path:
        data_path = Path(_path)

        if not data_path.exists():
            continue

        if data_path.suffix.lower() in [".dbc", ".dbf", ".parquet"]:
            content.append(ParquetSet(str(data_path)))
        else:
            continue

    if not content:
        logger.warning("path must be absolute")

    if len(content) == 1:
        return content[0]
    return content


class Data:
    """
    A class parser. Receives an (or a list of) absolute path(s) and returns
    either the corresponding DataSet or ParquetSet instances.
    """

    def __new__(
        cls, path: Union[List[str], str]
    ) -> Union[ParquetSet, List[ParquetSet]]:
        return parse_data_content(path)
