import datetime
from typing import Union

from pysus.ftp import to_list
from pysus.utilities.brasil import MONTHS, UFs  # noqa


def zfill_year(year: Union[str, int]) -> int:
    """
    Formats a len(2) year into len(4) with the correct year preffix
    E.g: 20 -> 2020; 99 -> 1999
    """
    year = str(year)[-2:].zfill(2)
    current_year = str(datetime.datetime.now().year)[-2:]
    suffix = "19" if str(year) > current_year else "20"
    return int(suffix + str(year))


def parse_UFs(UF: Union[list[str], str]) -> list:
    """
    Formats states abbreviations into correct format and retuns a list.
    Also checks if there is an incorrect UF in the list.
    E.g: ['SC', 'mt', 'ba'] -> ['SC', 'MT', 'BA']
    """
    ufs = [uf.upper() for uf in to_list(UF)]
    if not all(uf in list(UFs) for uf in ufs):
        raise ValueError(f"Unknown UF(s): {set(ufs).difference(list(UFs))}")
    return ufs
