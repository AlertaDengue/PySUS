from typing import Union
from pysus.online_data import FTP_Downloader, FTP_Inspect, CACHEPATH, FTP_SINAN


def list_diseases() -> list:
    """List available diseases on SINAN"""
    return list(FTP_SINAN.diseases.keys())


def get_available_years(disease: str) -> list:
    """
    Fetch available years for data related to specific disease
    :param disease: Disease name. See `SINAN.list_diseases` for valid names
    :return: A list of DBC files from a specific disease found in the FTP Server.
    """
    return FTP_Inspect("SINAN").list_available_years(SINAN_disease=disease)


def download(
    disease, years: Union[str, list, int], data_path: str = CACHEPATH
) -> list:
    """
    Downloads SINAN data directly from Datasus ftp server.
    :param disease: Disease according to `agravos`.
    :param years: 4 digit integer, can be a list of years.
    :param data_path: The directory where the chunks will be downloaded to.
    :return: list of downloaded parquet directories.
    """
    return FTP_Downloader("SINAN").download(
        SINAN_disease=disease, years=years, local_dir=data_path
    )
