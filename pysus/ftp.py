"""DATASUS FTP origin — ``pysus.ftp``.

Access to the DATASUS FTP datasets via the S3/DuckLake catalog mirror.
This is the primary origin for clinical health records: SINAN, SINASC,
SIM, SIH, SIA, PNI, IBGE, CNES, CIHA and COVID-19.

Import styles
─────────────
Both of these work::

    import pysus
    pysus.ftp.sinan(disease="deng", year=2017, as_dataframe=True)

    from pysus.ftp import sinan
    sinan(disease="deng", year=2017)

Fetching (read data)
────────────────────
    pysus.ftp.sinan(disease, year, ...)        SINAN — notifiable diseases
    pysus.ftp.sinasc(state, year, ...)         SINASC — live births
    pysus.ftp.sim(state, year, ...)            SIM — mortality
    pysus.ftp.sih(state, year, month, ...)     SIH — hospital admissions
    pysus.ftp.sia(state, year, month, ...)     SIA — ambulatory care
    pysus.ftp.pni(state, year, ...)            PNI — immunisations
    pysus.ftp.ibge(year, ...)                  IBGE — census data
    pysus.ftp.cnes(state, year, month, ...)    CNES — health facilities
    pysus.ftp.ciha(state, year, month, ...)    CIHA — hospital records
    pysus.ftp.covid19(...)                     COVID-19 confirmed cases

Discovery
─────────
    pysus.ftp.list_files("SINAN", year=2020, state="RJ")  → DataFrame
    pysus.ftp.info()                                      list FTP datasets
    pysus.ftp.get_origin_meta()                           origin metadata

The ``source`` parameter
────────────────────────
Every fetcher defaults to ``source="catalog"`` (read from the S3 mirror).
To bypass the catalog and query the DATASUS FTP server directly::

    pysus.ftp.sinan(disease="deng", year=2017, source="origin")

Do not pass ``origin=`` to namespaced calls — it is already fixed here::

    pysus.ftp.sinan(disease="deng", year=2017, origin="DadosGov")  # ERROR

See ``pysus.__all__`` / ``dir(pysus.ftp)`` for every available name.
"""

import sys

from pysus.api._impl import source as _source

__all__: list[str] = []

_source.install_origin_module(sys.modules[__name__], "ftp", "FTP")
