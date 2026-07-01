"""Translation dictionaries for the PySUS Streamlit UI."""

from typing import Final

EN: Final[dict[str, str]] = {
    "lang_label": "Language",
    "home_page": "Home",
    "sidebar_title": "Datasets",
    "sidebar_select": "Select a dataset",
    "home_title": "PySUS",
    "home_subtitle": "Tools for dealing with Brazil's Public health data (SUS — Sistema Único de Saúde).",
    "coming_soon": "coming soon",
    "datasets": "Datasets",
    "data_sources": "Data sources",
    "about_title": "About PySUS",
    "about_intro": "PySUS v{version} — Tools for dealing with Brazil's Public health data (SUS — Sistema Único de Saúde).",
    "sinan_desc": "Notifiable Diseases Information System",
    "sinasc_desc": "Live Births Information System",
    "sim_desc": "Mortality Information System",
    "sih_desc": "Hospital Information System",
    "sia_desc": "Ambulatory Information System",
    "pni_desc": "National Immunization Program",
    "ibge_desc": "Brazilian Institute of Geography and Statistics",
    "cnes_desc": "National Registry of Health Facilities",
    "ciha_desc": "Hospital Admission Communication",
    "ducklake_desc": "Modern data lake backend (default).",
    "ftp_desc": "Legacy FTP downloads from DATASUS.",
    "dadosgov_desc": "Brazilian open-data portal (dados.gov.br).",
    "browser_title": "Datasets",
    "browser_choose": "Choose a dataset",
    "browser_placeholder": "{dataset} browser — coming soon.",
    "state": "State",
    "year": "Year",
    "month": "Month",
    "group": "Group",
    "fetch": "Fetch data",
}

PT: Final[dict[str, str]] = {
    "lang_label": "Idioma",
    "home_page": "Início",
    "sidebar_title": "Bases de dados",
    "sidebar_select": "Selecione uma base",
    "home_title": "PySUS",
    "home_subtitle": "Ferramentas para dados públicos de saúde do Brasil (SUS — Sistema Único de Saúde).",
    "coming_soon": "em breve",
    "datasets": "Bases de dados",
    "data_sources": "Fontes de dados",
    "about_title": "Sobre o PySUS",
    "about_intro": "PySUS v{version} — Ferramentas para dados públicos de saúde do Brasil (SUS — Sistema Único de Saúde).",
    "sinan_desc": "Sistema de Informação de Agravos de Notificação",
    "sinasc_desc": "Sistema de Informações sobre Nascidos Vivos",
    "sim_desc": "Sistema de Informação sobre Mortalidade",
    "sih_desc": "Sistema de Informações Hospitalares",
    "sia_desc": "Sistema de Informações Ambulatoriais",
    "pni_desc": "Programa Nacional de Imunizações",
    "ibge_desc": "Instituto Brasileiro de Geografia e Estatística",
    "cnes_desc": "Cadastro Nacional de Estabelecimentos de Saúde",
    "ciha_desc": "Comunicação de Internação Hospitalar e Ambulatorial",
    "ducklake_desc": "Backend moderno de data lake (padrão).",
    "ftp_desc": "Downloads legados via FTP do DATASUS.",
    "dadosgov_desc": "Portal de dados abertos do governo (dados.gov.br).",
    "browser_title": "Bases de dados",
    "browser_choose": "Escolha uma base",
    "browser_placeholder": "Navegador {dataset} — em breve.",
    "state": "Estado",
    "year": "Ano",
    "month": "Mês",
    "group": "Grupo",
    "fetch": "Baixar dados",
}

TRANSLATIONS: Final[dict[str, dict[str, str]]] = {
    "en": EN,
    "pt": PT,
}


def t(
    key: str, lang: str = "en", **kwargs: str
) -> str:
    text = TRANSLATIONS.get(lang, EN).get(key, key)
    if kwargs:
        text = text.format(**kwargs)
    return text
