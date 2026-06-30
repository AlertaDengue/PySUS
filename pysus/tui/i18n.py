TRANSLATIONS: dict[str, dict[str, str | dict[str, str]]] = {
    "en": {
        "welcome": "Welcome to PySUS Client",
        "clients": "Clients",
        "local": "Local",
        "remote": "Remote",
        "search": "Search or leave empty to list all",
        "loading_err": "Failed to load",
        "loading": "Loading",
        "settings": "Settings",
        "quit": "Quit",
        "files": "Files",
        "ftp_browser": "FTP",
        "ducklake_browser": "DuckLake",
        "fetching": "Fetching datasets...",
        "name": "Name",
        "type": "Type",
        "info": "Info",
        "path": "Path",
        "size": "Size",
        "year": "Year",
        "month": "Month",
        "modified": "Modified",
        "state": "State",
        "description": "Description",
        "group": "Group",
        "months": {
            "1": "Jan",
            "2": "Feb",
            "3": "Mar",
            "4": "Apr",
            "5": "May",
            "6": "Jun",
            "7": "Jul",
            "8": "Aug",
            "9": "Sep",
            "10": "Oct",
            "11": "Nov",
            "12": "Dec",
        },
        "esc": "Press ESC to close",
    },
    "pt": {
        "welcome": "Bem-vindo ao Cliente PySUS",
        "clients": "Clientes",
        "local": "Local",
        "remote": "Remoto",
        "search": "Busque ou deixe em branco para listar tudo",
        "loading_err": "Erro ao carregar",
        "loading": "Carregando",
        "settings": "Configurações",
        "quit": "Sair",
        "files": "Arquivos",
        "ftp_browser": "FTP",
        "ducklake_browser": "DuckLake",
        "fetching": "Carregando datasets...",
        "name": "Nome",
        "type": "Tipo",
        "info": "Info",
        "path": "Path",
        "size": "Tamanho",
        "year": "Ano",
        "month": "Mês",
        "modified": "Modificado",
        "state": "Estado",
        "description": "Descrição",
        "group": "Grupo",
        "months": {
            "1": "Jan",
            "2": "Fev",
            "3": "Mar",
            "4": "Abr",
            "5": "Mai",
            "6": "Jun",
            "7": "Jul",
            "8": "Ago",
            "9": "Set",
            "10": "Out",
            "11": "Nov",
            "12": "Dez",
        },
        "esc": "ESC para fechar",
    },
}

SUPPORTED_LANGUAGES = tuple(TRANSLATIONS.keys())


def t(field: str, default: str = "", lang: str = "en") -> str:
    if lang not in TRANSLATIONS:
        lang = "en"

    data: dict = TRANSLATIONS[lang]
    keys = field.split(".")

    for key in keys:
        value = data.get(key)
        if isinstance(value, str):
            return value
        if isinstance(value, dict):
            data = value
        else:
            return default

    return default
