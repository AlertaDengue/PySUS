"""DadosGov open-data origin — ``pysus.dadosgov``.

Access to dados.gov.br (ckan.saude.gov.br) datasets via the S3/DuckLake
catalog mirror.  Only the databases DadosGov actually publishes are
exposed here: SINAN, SINASC, SIM, CNES, PNI and COVID-19.

The other databases (SIH, SIA, CIHA, IBGE, ...) are **not** exposed on
this namespace because CKAN does not publish them.

Import styles
─────────────
Both of these work::

    import pysus
    pysus.dadosgov.sinan(disease="deng", year=2020, as_dataframe=True)

    from pysus.dadosgov import sinan

Fetching (read data)
────────────────────
    pysus.dadosgov.sinan(disease, year, ...)   SINAN — notifiable diseases
    pysus.dadosgov.sinasc(state, year, ...)    SINASC — live births
    pysus.dadosgov.sim(state, year, ...)       SIM — mortality
    pysus.dadosgov.cnes(state, year, ...)      CNES — health facilities
    pysus.dadosgov.pni(state, year, ...)       PNI — immunisations
    pysus.dadosgov.covid19(...)                COVID-19 confirmed cases

Discovery
─────────
    pysus.dadosgov.list_files("SINAN", year=2020)      → DataFrame
    pysus.dadosgov.info()                              list DadosGov datasets
    pysus.dadosgov.get_origin_meta()                   origin metadata

The ``source`` parameter
────────────────────────
Fetchers default to ``source="catalog"`` (read from the S3 mirror).  To
query dados.gov.br directly (requires a ``DADOSGOV_TOKEN``)::

    pysus.dadosgov.sinan(disease="deng", year=2020, source="origin")

Do not pass ``origin=`` to namespaced calls — it is already fixed here::

    pysus.dadosgov.sinan(disease="deng", year=2020, origin="FTP")  # ERROR

See ``pysus.__all__`` / ``dir(pysus.dadosgov)`` for every available name.
"""

import sys

from pysus.api._impl import source as _source

__all__: list[str] = []

_source.install_origin_module(sys.modules[__name__], "dadosgov", "DADOSGOV")
