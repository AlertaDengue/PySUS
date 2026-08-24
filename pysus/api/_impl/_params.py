"""Shared parameter validation and normalisation for _impl functions.

All dataset convenience functions (sinan, sih, etc.) delegate parameter
normalisation here so that users can pass scalars, lists, or ``None``
and get a consistent ``list[int] | list[str] | None`` result.
"""

from __future__ import annotations

from collections.abc import Sequence


def _normalise_list(
    value: int | str | Sequence[int | str] | None,
) -> list[int] | list[str] | None:
    """Coerce *value* into a list, or return ``None``.

    Handles ``int | str | list[int] | list[str] | None`` and also
    tuples.  Strings are upper-cased (two-letter state codes).
    """
    if value is None:
        return None
    if isinstance(value, (int, str)):
        items = [value]
    elif isinstance(value, Sequence):
        items = list(value)
    else:
        items = [value]

    result: list[int] | list[str]
    if all(isinstance(v, int) for v in items):
        result = [int(v) for v in items]
    elif all(isinstance(v, str) for v in items):
        result = [str(v).upper() for v in items]
    else:
        result = [str(v).upper() for v in items]
    return result


def _normalise_state(state: str | None) -> str | None:
    """Upper-case a two-letter state code, or return ``None``."""
    return state.upper() if state else None


def _normalise_year(
    year: int | list[int] | None,
) -> list[int] | None:
    """Coerce year into ``list[int] | None``."""
    if year is None:
        return None
    if isinstance(year, int):
        return [year]
    return list(year)


def _normalise_month(
    month: int | list[int] | None,
) -> list[int] | None:
    """Coerce month into ``list[int] | None``."""
    if month is None:
        return None
    if isinstance(month, int):
        return [month]
    return list(month)
