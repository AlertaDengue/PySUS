"""
Download data from the national immunization program
"""
from typing import Literal, Union

from loguru import logger
from pysus.ftp import CACHEPATH
from pysus.ftp.databases.pni import PNI
from pysus.ftp.utils import parse_UFs


class _LazyPNI:
    """Lazy wrapper for PNI database to defer FTP connection until needed."""

    def __init__(self):
        self._instance = None

    def _ensure_loaded(self):
        """Ensure the PNI database is loaded."""
        if self._instance is None:
            self._instance = PNI().load()
        return self._instance

    def get_files(self, *args, **kwargs):
        return self._ensure_loaded().get_files(*args, **kwargs)

    def describe(self, *args, **kwargs):
        return self._ensure_loaded().describe(*args, **kwargs)

    def download(self, *args, **kwargs):
        return self._ensure_loaded().download(*args, **kwargs)


pni = _LazyPNI()


def get_available_years(group, states):
    """
    Fetch available years for `group` and/or `months`.
    :param group: PNI group, options are "CPNI" or "DPNI"
    :param state: UF code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :return: list of available years
    """
    ufs = parse_UFs(states)

    years = dict()
    for uf in ufs:
        files = pni.get_files(group, uf=uf)
        years[uf] = set(sorted([pni.describe(f)["year"] for f in files]))

    if len(set([len(v) for v in years.values()])) > 1:
        logger.warning(f"Distinct years were found for UFs: {years}")

    return sorted(list(set.intersection(*map(set, years.values()))))


def download(
    group: Union[list, Literal["CNPI", "DPNI"]],
    states: Union[str, list],
    years: Union[str, list, int],
    data_dir: str = CACHEPATH,
) -> list:
    """
    Download imunization records for a given States and years.
    :param group: PNI group, options are "CPNI" or "DPNI"
    :param state: uf two letter code, can be a list. E.g: "SP" or ["SP", "RJ"]
    :param year: year in 4 digits, can be a list. E.g: 1 or [1, 2, 3]
    :param data_dir: directory where data will be downloaded
    :return: list of downloaded ParquetData
    """
    files = pni.get_files(group, uf=states, year=years)
    return pni.download(files, local_dir=data_dir)
