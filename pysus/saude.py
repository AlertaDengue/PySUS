"""Saude portal origin — ``pysus.saude``.

Access to the Saude open-data portal (dadosabertos.saude.gov.br) theme
datasets: arboviroses, vacinacao, vigilancia_meio_ambiente, and 16 more.

Import styles
─────────────
Both of these work::

    import pysus
    pysus.saude.arboviroses(as_dataframe=True)

    from pysus.saude import vacinacao

Fetching (read data, 19 themes)
───────────────────────────────
    pysus.saude.arboviroses(...)               dengue/chikungunya/zika
    pysus.saude.assistencia_saude(...)         hospital & facilities
    pysus.saude.atencao_primaria(...)          primary care (SISAB)
    pysus.saude.bnafar(...)                    pharmaceutical assistance
    pysus.saude.ciencia_tecnologia(...)        science & technology
    pysus.saude.cnes(...)                      CNES health-facility registers
    pysus.saude.diagnosticos_tratamentos(...)  diagnostics & treatments
    pysus.saude.economia_saude(...)            health economics
    pysus.saude.educacao_saude(...)            health education
    pysus.saude.macro_saude(...)               macro-regions
    pysus.saude.ouvidoria(...)                 SUS ombudsman
    pysus.saude.outros_temas(...)              miscellaneous
    pysus.saude.pda(...)                       digital health plan
    pysus.saude.prevencao_promocao(...)        prevention & promotion
    pysus.saude.saude_indigena(...)            indigenous health
    pysus.saude.sisagua(...)                   water quality
    pysus.saude.sisvan(...)                    food & nutrition
    pysus.saude.vacinacao(...)                 vaccination (PNI/ESAVI)
    pysus.saude.vigilancia_meio_ambiente(...)  environmental surveillance

Discovery
─────────
    pysus.saude.info()                         list Saude theme datasets
    pysus.saude.get_origin_meta()              origin metadata

The ``source`` parameter
────────────────────────
The Saude portal has no catalog mirror — all fetchers read directly from
the CKAN portal regardless of ``source``.  ``source="origin"`` is accepted
for consistency with the other origins.

Do not pass ``origin=`` to namespaced calls — it is already fixed here::

    pysus.saude.arboviroses(origin="FTP")  # ERROR

See ``pysus.__all__`` / ``dir(pysus.saude)`` for every available name.
"""

import sys

from pysus.api._impl import source as _source

__all__: list[str] = []

_source.install_origin_module(sys.modules[__name__], "saude", "SAUDE")
