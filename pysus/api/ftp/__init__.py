from .client import *  # noqa
from .databases import *  # noqa


AVAILABLE_DATABASES = [
    CIHA,
    CNES,
    IBGEDATASUS,
    PNI,
    SIA,
    SIH,
    SIM,
    SINAN,
    SINASC,
]

__all__ = [
    "CIHA",
    "CNES",
    "IBGEDATASUS",
    "PNI",
    "SIA",
    "SIH",
    "SIM",
    "SINAN",
    "SINASC",
    "AVAILABLE_DATABASES",
]
