"""Pre-configured dataset registry for the OpenDataSUS portal.

Each :class:`DatasetSpec` describes one theme of the SUS open-data
portal. The spec carries:

- the canonical dataset ``name`` (uppercase, source-scoped);
- the CKAN ``group`` slug used to select packages from the catalog
  (the portal groups its 138 datasets into 14 themes);
- ``slug_patterns`` / ``exclude_patterns`` — extra regex filters
  applied over the full catalog listing (used when a theme is spread
  across groups or when one group hosts several themes, e.g. SISAGUA
  inside ``vigilancia-e-meio-ambiente``);
- the DEMAS ``tags`` and their REST ``endpoints`` (the structured
  query API — consumed from Stage 3 onwards).

Source differentiation: datasets that also exist on dados.gov.br or
DATASUS FTP (CNES, PNI, SIM, SINASC, Arboviroses) are declared **here
too**, as Saude-source declarations. The same logical dataset in
another source keeps its own declaration there; both are linked by
``identity.cross_origin_id`` (the shared CKAN UUID) at merge time,
never collapsed at declaration time.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

_YEAR_RE = re.compile(r"(20\d{2}|19\d{2})")


@dataclass(frozen=True)
class DatasetSpec:
    """Static description of one Saude theme dataset."""

    name: str
    long_name: str
    description: str
    ckan_group: str | None = None
    slug_patterns: tuple[str, ...] = ()
    exclude_patterns: tuple[str, ...] = ()
    demas_tags: tuple[str, ...] = ()
    endpoints: tuple[str, ...] = ()

    def matches(self, slug: str) -> bool:
        """Return True when *slug* belongs to this spec."""
        if self.exclude_patterns and any(
            re.search(pat, slug, re.I) for pat in self.exclude_patterns
        ):
            return False
        if not self.slug_patterns:
            return True
        return any(re.search(pat, slug, re.I) for pat in self.slug_patterns)


def parse_year(name: str) -> int | None:
    """Extract a 4-digit year from a resource/endpoint name, if any."""
    match = _YEAR_RE.search(name or "")
    if not match:
        return None
    year = int(match.group(1))
    return year if 1970 <= year <= 2100 else None


DATASET_SPECS: tuple[DatasetSpec, ...] = (
    DatasetSpec(
        name="ARBOVIROSES",
        long_name="Arboviroses",
        description=(
            "Notificações de arboviroses (dengue, chikungunya, zika e "
            "febre amarela) registradas no Sinan, por ano e município."
        ),
        ckan_group="arboviroses",
        demas_tags=("Agravo Arboviroses",),
        endpoints=(
            "/arboviroses/dengue",
            "/arboviroses/chikungunya",
            "/arboviroses/zikavirus",
            "/arboviroses/febre-amarela-humanos-primatas-nao-humanos",
            "/arboviroses/febre-amarela-epzootias",
        ),
    ),
    DatasetSpec(
        name="ASSISTENCIASAUDE",
        long_name="Assistência à Saúde",
        description=(
            "Estabelecimentos hospitalares, leitos, Unidades Básicas "
            "de Saúde e registros de ocupação hospitalar."
        ),
        ckan_group="assistencia-a-saude",
        demas_tags=("Assistência à Saúde",),
        endpoints=(
            "/assistencia-a-saude/hospitais-e-leitos",
            "/assistencia-a-saude/unidade-basicas-de-saude",
            "/assistencia-a-saude/registro-de-ocupacao-hospitalar-covid-19",
        ),
    ),
    DatasetSpec(
        name="ATENCAOPRIMARIA",
        long_name="Atenção Primária",
        description=(
            "Programa Mais Médicos (PMMB), Previne Brasil, SISAB e "
            "ENANI-2019."
        ),
        ckan_group="atencao-primaria",
        demas_tags=("Atenção Primária",),
        endpoints=(
            "/atencao-primaria/enani-2019",
            "/atencao-primaria/pmmb-consolidado",
            "/atencao-primaria/pmmb-serie-historica",
            "/atencao-primaria/pmmb-relacao-nominal-coparticipacao",
            "/atencao-primaria/pmmb-relatorio-historico-cadastro-cnes",
            "/atencao-primaria/pmmb-especialista-consolidado",
            "/atencao-primaria/pmmb-especialista-serie-historica",
            "/atencao-primaria/pmmb-relacao-nominal-ativo",
            "/atencao-primaria/cadastro-vinculado-programa-previne-brasil",
            "/atencao-primaria/indicador-desempenho-programa-previne-brasil",
            "/atencao-primaria/pmmb-especialista-relacao-nominal-ativo",
        ),
    ),
    DatasetSpec(
        name="BNAFAR",
        long_name=(
            "Base Nacional de Dados de Ações e Serviços da "
            "Assistência Farmacêutica"
        ),
        description=(
            "Estoque de medicamentos do Sistema Nacional de Gestão da "
            "Assistência Farmacêutica (Hórus)."
        ),
        ckan_group="assistencia-farmaceutica",
        demas_tags=("BNAFAR",),
        endpoints=("/daf/estoque-medicamentos-bnafar-horus",),
    ),
    DatasetSpec(
        name="CNES",
        long_name="Cadastro Nacional de Estabelecimentos de Saúde",
        description=(
            "Estabelecimentos de saúde e tipos de unidade "
            "cadastrados no CNES."
        ),
        slug_patterns=("cnes",),
        demas_tags=("CNES",),
        endpoints=(
            "/cnes/tipounidades",
            "/cnes/tipounidades/{codigo_tipo_unidade}",
            "/cnes/estabelecimentos",
            "/cnes/estabelecimentos/{codigo_cnes}",
        ),
    ),
    DatasetSpec(
        name="CIENCIATECNOLOGIA",
        long_name="Ciência & Tecnologia",
        description=(
            "Conitec (PCDT, demandas, consultas públicas), Plataforma "
            "Brasil, pesquisas em saúde Decit e indicadores RIPSA."
        ),
        ckan_group="ciencia-tecnologia",
        slug_patterns=(
            "^ripsa",
            "^pesquisa_saude",
            "^contribuicoes-de-consultas-publicas",
            "^tecnologias-e-diretrizes",
            "^pcdt",
        ),
        demas_tags=("Ciência & Tecnologia",),
        endpoints=(
            "/ciencia-tecnologia/dgits-contribuicoes-consultas-publicas",
            "/ciencia-tecnologia/dgits-controle-demandas-conitec",
            "/ciencia-tecnologia/dgits-controle-pcdt",
            "/ciencia-tecnologia/dgits-tecnologias-diretrizes",
            "/ciencia-tecnologia/plataformabr-pesquisa-saude",
            "/ciencia-tecnologia/plataformabr-projeto-aprovado",
        ),
    ),
    DatasetSpec(
        name="DIAGNOSTICOSTRATAMENTOS",
        long_name="Diagnósticos e Tratamentos",
        description=(
            "Protocolos Clínicos e Diretrizes Terapêuticas (PCDT) e "
            "tecnologias para tratamento e prevenção."
        ),
        ckan_group="diagnosticos-e-tratamentos",
    ),
    DatasetSpec(
        name="ECONOMIASAUDE",
        long_name="Economia da Saúde",
        description=("Banco de Preços em Saúde (BPS), ApuraSUS e SIOPS."),
        ckan_group="economia-da-saude",
        demas_tags=("Economia da Saúde",),
        endpoints=(
            "/economia-da-saude/bps",
            "/economia-da-saude/sistema-de-apuracao-e-gestao-de-custos-"
            "do-sus-apurasus",
        ),
    ),
    DatasetSpec(
        name="EDUCACAOSAUDE",
        long_name="Educação em Saúde",
        description=(
            "Programa De Volta Para Casa (PVC) — desinstitucionalização "
            "de pessoas com transtornos mentais."
        ),
        ckan_group="educacao-em-saude",
        demas_tags=("Educação em Saúde",),
        endpoints=("/educacao-em-saude/pvc",),
    ),
    DatasetSpec(
        name="MACROSAUDE",
        long_name="Macrorregião e Região de Saúde",
        description=(
            "Municípios com as informações de macrorregião e região "
            "de saúde, e indicadores de gestão municipal (MGDI)."
        ),
        ckan_group="indicadores-de-saude",
        demas_tags=("Macrorregião e Região de Saúde",),
        endpoints=("/macrorregiao-e-regiao-de-saude/municipio",),
    ),
    DatasetSpec(
        name="OUVIDORIA",
        long_name="Ouvidoria",
        description=(
            "Manifestações registradas na Ouvidoria do SUS, por UF, "
            "assunto e problema."
        ),
        slug_patterns=("ouvidor",),
        demas_tags=("Ouvidoria",),
        endpoints=("/ouvidoria/ouvidor2", "/ouvidoria/ouvidor3"),
    ),
    DatasetSpec(
        name="OUTROSTEMAS",
        long_name="Outros Temas",
        description=(
            "Coordenação de Estratégia de Dados (CED) — demandas "
            "abertas para a equipe de banco de dados."
        ),
        slug_patterns=("^ced-coordenacao",),
        demas_tags=("Outros Temas",),
        endpoints=("/outros-temas/ced",),
    ),
    DatasetSpec(
        name="PDA",
        long_name="Saúde Digital",
        description=(
            "Plano de Dados Abertos e ações de saúde digital do "
            "Ministério da Saúde."
        ),
        ckan_group="pda",
    ),
    DatasetSpec(
        name="PREVENCAOPROMOCAO",
        long_name="Prevenção e Promoção da Saúde",
        description=(
            "Distribuição de equipamentos de proteção individual e insumos."
        ),
        ckan_group="prevencao-e-promocao-da-saude",
        demas_tags=("Prevenção e Promoção",),
        endpoints=("/prevencao-e-promocao/distribuicao-epi-insumo",),
    ),
    DatasetSpec(
        name="SISAGUA",
        long_name=(
            "Sistema de Informação da Vigilância da Qualidade da Água "
            "para Consumo Humano"
        ),
        description=(
            "Vigilância e controle mensal da qualidade da água para "
            "consumo humano, captação, tratamento e abastecimento."
        ),
        ckan_group="vigilancia-e-meio-ambiente",
        slug_patterns=("sisagua",),
        demas_tags=("SISAGUA",),
        endpoints=(
            "/sisagua/vigilancia-parametros-basicos",
            "/sisagua/controle-semestral",
            "/sisagua/controle-mensal-parametros-basicos",
            "/sisagua/pontos-de-captacao",
            "/sisagua/cadastro-carro-pipa-populacao",
            "/sisagua/cadastro-carro-pipa-procedencia",
            "/sisagua/controle-mensal-amostras-fora-do-padrao",
            "/sisagua/controle-mensal-demais-parametros",
            "/sisagua/controle-mensal-infraestrutura-operacional",
            "/sisagua/controle-mensal-plano-amostragem",
            "/sisagua/populacao-abastecida",
            "/sisagua/tratamento-de-agua",
            "/sisagua/vigilancia-cianobacterias-e-cianotoxinas",
            "/sisagua/vigilancia-demais-parametros",
        ),
    ),
    DatasetSpec(
        name="SISVAN",
        long_name="Sistema de Vigilância Alimentar e Nutricional",
        description=("Acompanhamento de estado nutricional da população."),
        slug_patterns=("sisvan",),
        demas_tags=("SISVAN",),
        endpoints=("/sisvan/estado-nutricional",),
    ),
    DatasetSpec(
        name="SAUDEINDIGENA",
        long_name="Saúde Indígena",
        description=(
            "Siasi/SasiSUS e Sesai: morbidades, imunização, saúde "
            "bucal, saneamento, óbitos e demografia da população "
            "indígena assistida."
        ),
        ckan_group="saude-indigena",
        demas_tags=("Saúde Indígena",),
        endpoints=(
            "/saude-indigena/sasisus-esgotamento-sanitario",
            "/saude-indigena/sasi-sus-gerenciamento-de-residuos-solidos",
            "/saude-indigena/acompanhamento-obra-infraestrutura-saude",
            "/saude-indigena/"
            "planilha-de-fornecimento-e-monitoramento-da-qualidade-da-"
            "agua-acesso-a-agua",
            "/saude-indigena/"
            "planilha-registros-habilitacao-recebimento-incentivo",
            "/saude-indigena/"
            "indicadores-enfrentamento-monitoramento-covid19-indigenas",
            "/saude-indigena/"
            "sistema-de-atencao-a-saude-indigena-modulo-de-vigilancia-"
            "alimentar-e-nutricional",
            "/saude-indigena/siasi-acompanhamento-gestacional",
            "/saude-indigena/siasi-modulo-morbidades",
            "/saude-indigena/sesai-atendimentos",
            "/saude-indigena/sesai-recursos-humanos",
            "/saude-indigena/siasi-modulo-saude-bucal-ficha3",
            "/saude-indigena/siasi-modulo-saude-bucal-ficha4",
            "/saude-indigena/siasi-modulo-saude-bucal-ficha7",
        ),
    ),
    DatasetSpec(
        name="VACINACAO",
        long_name="Vacinação",
        description=(
            "Doses aplicadas pelo PNI por ano, ESAVI e insumos "
            "estratégicos (SIES)."
        ),
        ckan_group="vacinacao",
        demas_tags=("Vacinação",),
        endpoints=(
            "/vacinacao/doses-aplicadas-pni-2020",
            "/vacinacao/doses-aplicadas-pni-2021",
            "/vacinacao/doses-aplicadas-pni-2022",
            "/vacinacao/doses-aplicadas-pni-2023",
            "/vacinacao/doses-aplicadas-pni-2024",
            "/vacinacao/doses-aplicadas-pni-2025",
            "/vacinacao/doses-aplicadas-pni-2026",
            "/vacinacao/esavi",
            "/vacinacao/sistema-de-informacao-de-insumos-estrategicos",
        ),
    ),
    DatasetSpec(
        name="VIGILANCIAMEIOAMBIENTE",
        long_name="Vigilância e Meio Ambiente",
        description=(
            "SRAG, síndrome gripal, SIM, Sinasc e mpox — dados de "
            "vigilância epidemiológica e ambiental."
        ),
        ckan_group="vigilancia-e-meio-ambiente",
        exclude_patterns=("sisagua",),
        demas_tags=("Vigilância e Meio Ambiente",),
        endpoints=(
            "/vigilancia-e-meio-ambiente/"
            "notificacoes-de-sindrome-gripal-leve-2020",
            "/vigilancia-e-meio-ambiente/"
            "notificacoes-de-sindrome-gripal-leve-2021",
            "/vigilancia-e-meio-ambiente/"
            "notificacoes-de-sindrome-gripal-leve-2022",
            "/vigilancia-e-meio-ambiente/"
            "notificacoes-de-sindrome-gripal-leve-2023",
            "/vigilancia-e-meio-ambiente/"
            "notificacoes-de-sindrome-gripal-leve-2024",
            "/vigilancia-e-meio-ambiente/"
            "sistema-de-informacao-sobre-mortalidade",
            "/vigilancia-e-meio-ambiente/"
            "sistema-de-informacao-sobre-nascidos-vivos",
            "/vigilancia-e-meio-ambiente/srag-2009-2012",
            "/vigilancia-e-meio-ambiente/srag-2013-2018",
            "/vigilancia-e-meio-ambiente/srag-2019-2026",
            "/vigilancia-e-meio-ambiente/mpox",
        ),
    ),
)

#: Lookup by canonical name.
SPECS_BY_NAME: dict[str, DatasetSpec] = {
    spec.name: spec for spec in DATASET_SPECS
}


def spec_for(slug: str, groups: tuple[str, ...] = ()) -> DatasetSpec | None:
    """Return the most specific spec whose filters match *slug*.

    ``groups`` is the package's CKAN group membership (from the
    catalog listing); specs that declare a ``ckan_group`` only match
    packages that actually belong to that group. Specs with explicit
    ``slug_patterns`` take precedence over group-only specs.
    """
    candidates = [
        spec
        for spec in DATASET_SPECS
        if spec.matches(slug)
        and (not spec.ckan_group or spec.ckan_group in groups)
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda spec: (
            bool(spec.slug_patterns),
            len(spec.slug_patterns),
        ),
    )


__all__ = [
    "DATASET_SPECS",
    "DatasetSpec",
    "SPECS_BY_NAME",
    "parse_year",
    "spec_for",
]
