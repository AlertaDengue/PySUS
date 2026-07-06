"""Translation dictionaries for the PySUS Streamlit UI."""

from typing import Final

EN: Final[dict[str, str]] = {
    "lang_label": "Language",
    "home_page": "Home",
    "sidebar_title": "Datasets",
    "sidebar_select": "Select a dataset",
    "home_title": "PySUS",
    "home_subtitle": (
        "Tools for dealing with Brazil's Public health data"
        " (SUS — Sistema Único de Saúde)."
    ),
    "coming_soon": "coming soon",
    "datasets": "Datasets",
    "data_sources": "Data sources",
    "about_title": "About PySUS",
    "about_intro": (
        "PySUS v{version} — Tools for dealing with Brazil's"
        " Public health data (SUS — Sistema Único de Saúde)."
    ),
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
    "ducklake_label": "PySUS s3",
    "ftp_label": "FTP DataSUS",
    "dadosgov_label": "API DataSUS",
    "source_label": "Data source",
    "loading_catalog": "Loading DuckLake catalog...",
    "catalog_failed": "Could not connect to DuckLake catalog.",
    "catalog_query_failed": "Could not query catalog: {error}",
    "ftp_failed": "Could not connect to DATASUS FTP.",
    "select_groups": "Select groups...",
    "select_states": "SP, RJ, ...",
    "select_years": "Select years...",
    "select_months": "Select months...",
    "invalid_year": "Invalid year format.",
    "invalid_month": "Invalid month format.",
    "api_token": "API Token",
    "token_placeholder": "Chave de API do dados.gov.br",
    "token_required": "Enter your dados.gov.br API token to browse datasets.",
    "connect": "Connect",
    "connecting_dadosgov": "Connecting to DadosGov...",
    "select_dataset": "Please select a dataset.",
    "querying": "Querying {client}...",
    "no_files": "No files matching the criteria.",
    "query_failed": "Query failed: {error}",
    "files_found": "Found {count} file(s).",
    "browser_title": "Datasets",
    "browser_choose": "Choose a dataset",
    "browser_placeholder": "{dataset} browser — coming soon.",
    "state": "State",
    "year": "Year",
    "month": "Month",
    "group": "Group",
    "dataset": "Dataset",
    "fetch": "Fetch files",
    "size": "Size",
    "size_bytes": "Size (bytes)",
    "add_to_queue": "➕ Add to queue",
    "results_title": "🗃️ Results ({count} files)",
    "queue_title": "📥 Download Queue ({count} files)",
    "queue_empty": "No files in the download queue.",
    "save_to": "Save to",
    "browse": "Browse",
    "browse_dir_title": "Select download folder",
    "select_dir": "Select",
    "download_dir_placeholder": "Download directory...",
    "remove": "✕ Remove",
    "clear": "🗑 Clear",
    "download": "⬇ Download",
    "downloading": "Downloading {name} ({i}/{total})",
    "download_start": "Starting downloads...",
    "download_done": "Done!",
    "download_failed": "Failed: {name} — {error}",
    "download_success": "Downloaded {count} file(s) to {dir}",
    "python_snippet": "📋 Python snippet",
    "intro_welcome": "PySUS Web",
    "intro_desc": (
        "This is a local web interface for browsing, querying,"
        " and downloading Brazil's public health datasets (SUS)."
        " Use the sidebar to navigate between sections and select"
        " your preferred data source."
    ),
    "intro_available": "Available Data Sources",
    "intro_source_ducklake": (
        "**PySUS s3** — the default backend. Browse and query"
        " datasets from the PySUS cloud data lake. Fast parquet"
        " downloads with column selection. Best for programmatic"
        " access and large-scale analysis."
    ),
    "intro_source_ftp": (
        "**FTP DataSUS** — the legacy DATASUS FTP server."
        " Download raw DBF files for SINAN, SINASC, SIM, SIH,"
        " SIA, PNI, CNES, CIHA and more. Covers historical"
        " data back to the 1990s."
    ),
    "intro_source_dadosgov": (
        "**API DataSUS** — the dados.gov.br open-data portal."
        " Query datasets via REST API with metadata and"
        " filtering. Requires a free API token from the"
        " Brazilian government portal."
    ),
    "intro_package": (
        "PySUS is a Python package. For package documentation,"
        " API reference, programmatic usage, and this web server,"
        " visit [pysus.readthedocs.io](https://pysus.readthedocs.io)."
    ),
    "intro_github": (
        "Found an issue or want to contribute?"
        " Visit the [GitHub repository]"
        "(https://github.com/AlertaDengue/PySUS)."
    ),
    "databases_title": "Available Databases",
    "databases_desc": (
        "PySUS provides access to the following DATASUS databases:"
    ),
    "databases_source": "Official source",
    "db_sinan_desc": (
        "Notifiable Diseases Information System. Data on"
        " dengue, zika, chikungunya, and other mandatory-report"
        " diseases."
    ),
    "db_sinasc_desc": (
        "Live Births Information System. Birth records with"
        " maternal, gestational, and neonatal data."
    ),
    "db_sim_desc": (
        "Mortality Information System. Death records with"
        " cause, location, and demographic data."
    ),
    "db_sih_desc": (
        "Hospital Information System. Hospital admission"
        " records funded by SUS."
    ),
    "db_sia_desc": (
        "Ambulatory Information System. Outpatient care"
        " records from SUS providers."
    ),
    "db_pni_desc": (
        "National Immunization Program. Vaccination coverage"
        " and doses administered across Brazil."
    ),
    "db_cnes_desc": (
        "National Registry of Health Facilities. Data on"
        " hospitals, clinics, and health units."
    ),
    "db_ciha_desc": (
        "Hospital Admission Communication. Complementary"
        " hospital admission and outpatient data."
    ),
    "db_ibge_desc": (
        "Brazilian Institute of Geography and Statistics."
        " Population estimates and demographic data."
    ),
}

PT: Final[dict[str, str]] = {
    "lang_label": "Idioma",
    "home_page": "Início",
    "sidebar_title": "Bases de dados",
    "sidebar_select": "Selecione uma base",
    "home_title": "PySUS",
    "home_subtitle": (
        "Ferramentas para dados públicos de saúde do Brasil"
        " (SUS — Sistema Único de Saúde)."
    ),
    "coming_soon": "em breve",
    "datasets": "Bases de dados",
    "data_sources": "Fontes de dados",
    "about_title": "Sobre o PySUS",
    "about_intro": (
        "PySUS v{version} — Ferramentas para dados públicos"
        " de saúde do Brasil (SUS — Sistema Único de Saúde)."
    ),
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
    "ducklake_label": "PySUS s3",
    "ftp_label": "FTP DataSUS",
    "dadosgov_label": "API DataSUS",
    "source_label": "Base de dados",
    "loading_catalog": "Carregando catálogo...",
    "catalog_failed": "Não foi possível conectar ao catálogo DuckLake.",
    "catalog_query_failed": "Não foi possível consultar o catálogo: {error}",
    "ftp_failed": "Não foi possível conectar ao FTP do DATASUS.",
    "select_groups": "Selecionar grupos...",
    "select_states": "SP, RJ, ...",
    "select_years": "Selecionar anos...",
    "select_months": "Selecionar meses...",
    "invalid_year": "Formato de ano inválido.",
    "invalid_month": "Formato de mês inválido.",
    "api_token": "Token da API",
    "token_placeholder": "Chave de API do dados.gov.br",
    "token_required": "Insira seu token da API do dados.gov.br para navegar.",
    "connect": "Conectar",
    "connecting_dadosgov": "Conectando ao DadosGov...",
    "select_dataset": "Selecione uma base de dados.",
    "querying": "Consultando {client}...",
    "no_files": "Nenhum arquivo corresponde aos critérios.",
    "query_failed": "Falha na consulta: {error}",
    "files_found": "{count} arquivo(s) encontrado(s).",
    "browser_title": "Bases de dados",
    "browser_choose": "Escolha uma base",
    "browser_placeholder": "Navegador {dataset} — em breve.",
    "state": "Estado",
    "year": "Ano",
    "month": "Mês",
    "group": "Grupo",
    "dataset": "Base de dados",
    "fetch": "Buscar arquivos",
    "size": "Tamanho",
    "size_bytes": "Tamanho (bytes)",
    "add_to_queue": "➕ Adicionar à fila",
    "results_title": "🗃️ Resultados ({count} arquivos)",
    "queue_title": "📥 Fila de download ({count} arquivos)",
    "queue_empty": "Nenhum arquivo na fila de download.",
    "save_to": "Salvar em",
    "browse": "Procurar",
    "browse_dir_title": "Selecionar pasta de download",
    "select_dir": "Selecionar",
    "download_dir_placeholder": "Diretório de download...",
    "remove": "✕ Remover",
    "clear": "🗑 Limpar",
    "download": "⬇ Baixar",
    "downloading": "Baixando {name} ({i}/{total})",
    "download_start": "Iniciando downloads...",
    "download_done": "Concluído!",
    "download_failed": "Falha: {name} — {error}",
    "download_success": "{count} arquivo(s) baixado(s) para {dir}",
    "python_snippet": "📋 Código Python",
    "intro_welcome": "PySUS Web",
    "intro_desc": (
        "Esta é uma interface web local para navegar, consultar"
        " e baixar bases de dados públicos de saúde do Brasil"
        " (SUS). Use a barra lateral para navegar entre as"
        " seções e selecionar sua fonte de dados preferida."
    ),
    "intro_available": "Fontes de Dados Disponíveis",
    "intro_source_ducklake": (
        "**PySUS s3** — o backend padrão. Navegue e consulte"
        " bases do data lake do PySUS. Downloads rápidos em"
        " parquet com seleção de colunas. Ideal para acesso"
        " programático e análises em larga escala."
    ),
    "intro_source_ftp": (
        "**FTP DataSUS** — o servidor FTP legado do DATASUS."
        " Baixe arquivos DBF brutos de SINAN, SINASC, SIM,"
        " SIH, SIA, PNI, CNES, CIHA e outros. Cobre dados"
        " históricos desde a década de 1990."
    ),
    "intro_source_dadosgov": (
        "**API DataSUS** — o portal de dados abertos"
        " dados.gov.br. Consulte bases via API REST com"
        " metadados e filtros. Requer um token gratuito de"
        " API do portal do governo brasileiro."
    ),
    "intro_package": (
        "PySUS é um pacote Python. Para documentação do pacote,"
        " referência da API, uso programático e este servidor"
        " web, acesse [pysus.readthedocs.io]"
        "(https://pysus.readthedocs.io)."
    ),
    "intro_github": (
        "Encontrou um problema ou quer contribuir?"
        " Acesse o [repositório no GitHub]"
        "(https://github.com/AlertaDengue/PySUS)."
    ),
    "databases_title": "Bases de Dados Disponíveis",
    "databases_desc": "O PySUS oferece acesso às seguintes bases do DATASUS:",
    "databases_source": "Fonte oficial",
    "db_sinan_desc": (
        "Sistema de Informação de Agravos de Notificação."
        " Dados de dengue, zika, chikungunya e outras doenças"
        " de notificação obrigatória."
    ),
    "db_sinasc_desc": (
        "Sistema de Informações sobre Nascidos Vivos."
        " Registros de nascimento com dados maternos,"
        " gestacionais e neonatais."
    ),
    "db_sim_desc": (
        "Sistema de Informação sobre Mortalidade. Registros"
        " de óbito com dados de causa, local e perfil"
        " demográfico."
    ),
    "db_sih_desc": (
        "Sistema de Informações Hospitalares. Registros de"
        " internações financiadas pelo SUS."
    ),
    "db_sia_desc": (
        "Sistema de Informações Ambulatoriais. Registros de"
        " atendimento ambulatorial do SUS."
    ),
    "db_pni_desc": (
        "Programa Nacional de Imunizações. Cobertura vacinal"
        " e doses aplicadas em todo o Brasil."
    ),
    "db_cnes_desc": (
        "Cadastro Nacional de Estabelecimentos de Saúde."
        " Dados de hospitais, clínicas e unidades de saúde."
    ),
    "db_ciha_desc": (
        "Comunicação de Informação Hospitalar e Ambulatorial."
        " Dados complementares de internação e atendimento"
        " ambulatorial."
    ),
    "db_ibge_desc": (
        "Instituto Brasileiro de Geografia e Estatística."
        " Estimativas populacionais e dados demográficos."
    ),
}

TRANSLATIONS: Final[dict[str, dict[str, str]]] = {
    "en": EN,
    "pt": PT,
}


def t(key: str, lang: str = "pt", **kwargs: str) -> str:
    text = TRANSLATIONS.get(lang, EN).get(key, key)
    if kwargs:
        text = text.format(**kwargs)
    return text
