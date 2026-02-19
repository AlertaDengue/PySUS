from .ciha import CIHA
from .cnes import CNES
from .ibge_datasus import IBGEDATASUS
from .pni import PNI
from .sia import SIA
from .sih import SIH
from .sim import SIM
from .sinan import SINAN
from .sinasc import SINASC

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
