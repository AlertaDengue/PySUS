"""Input validation with fuzzy suggestions.

Uses ``difflib.get_close_matches`` to suggest corrections for typos.

Usage::

    from pysus.api.validate import validate_choice

    validate_choice("sinam", ["SINAN", "SIA", "SIM"])
    # raises ValidationError with suggestion: Did you mean 'SINAN'?
"""

from __future__ import annotations

from difflib import get_close_matches

from pysus.api.errors import ValidationError


def validate_choice(
    value: str,
    choices: list[str],
    label: str = "value",
    cutoff: float = 0.6,
) -> str:
    """Validate that *value* is in *choices*, suggesting corrections.

    Parameters
    ----------
    value : str
        User-provided value.
    choices : list[str]
        Allowed values.
    label : str
        Human-readable name for the parameter (for the error message).
    cutoff : float
        Similarity threshold for suggestions (0-1).

    Returns
    -------
    str
        The matched choice (case-insensitive match returned as canonical).

    Raises
    ------
    ValidationError
        If value is not in choices and no close match is found.
    """
    # Case-insensitive exact match
    for choice in choices:
        if choice.lower() == value.lower():
            return choice

    # Fuzzy suggestions (case-insensitive)
    lower_choices = [c.lower() for c in choices]
    matches_raw = get_close_matches(
        value.lower(), lower_choices, n=3, cutoff=cutoff
    )
    matches = [choices[lower_choices.index(m)] for m in matches_raw]

    if matches:
        suggestions = ", ".join(f"'{m}'" for m in matches)
        raise ValidationError(
            f"Invalid {label}: '{value}'. " f"Did you mean {suggestions}?",
            hint=f"Valid {label} values: {', '.join(choices)}",
        )

    raise ValidationError(
        f"Invalid {label}: '{value}'.",
        hint=f"Valid {label} values: {', '.join(choices)}",
    )


def validate_dataset(name: str) -> str:
    """Validate a PySUS dataset name.

    Parameters
    ----------
    name : str
        Dataset name (e.g. 'sinan', 'SIH').

    Returns
    -------
    str
        Canonical dataset name.

    Raises
    ------
    ValidationError
        If dataset name is not recognised.
    """
    datasets = [
        "CIHA",
        "CNES",
        "IBGEDATASUS",
        "PNI",
        "SIA",
        "SIH",
        "SIM",
        "SINAN",
        "SINASC",
        "COVID19",
    ]
    return validate_choice(name.upper(), datasets, label="dataset")


def validate_origin(origin: str) -> str:
    """Validate a data origin.

    Parameters
    ----------
    origin : str
        Origin name (e.g. 'ftp', 'dadosgov').

    Returns
    -------
    str
        Canonical origin name.

    Raises
    ------
    ValidationError
        If origin is not recognised.
    """
    origins = ["FTP", "DADOSGOV", "SAUDE", "DUCKLAKE"]
    return validate_choice(origin.upper(), origins, label="origin")
