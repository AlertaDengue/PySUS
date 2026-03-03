import datetime
from typing import Union, TypeVar, List, Tuple

from .brasil import *  # noqa


T = TypeVar("T")


def to_list(item: Union[T, List[T], Tuple[T, ...], None]) -> List[T]:
    """Parse any builtin data type into a list"""
    if item is None:
        return []
    return [item] if not isinstance(item, (list, tuple)) else list(item)


def zfill_year(year: Union[str, int]) -> int:
    """
    Formats a len(2) year into len(4) with the correct year preffix
    E.g: 20 -> 2020; 99 -> 1999
    """
    year = str(year)[-2:].zfill(2)
    current_year = str(datetime.datetime.now().year)[-2:]
    suffix = "19" if str(year) > current_year else "20"
    return int(suffix + str(year))
