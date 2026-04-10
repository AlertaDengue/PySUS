TRANSLATIONS = {
    "en": {
        "welcome": "Welcome to PySUS Client",
        "clients": "Clients",
        "local": "Local",
        "search": "Search or leave empty to list all",
        "loading_err": "Failed to load",
        "settings": "Settings",
        "quit": "Quit",
        "files": "Files",
        "ftp_browser": "FTP",
        "fetching": "Fetching datasets...",
        "name": "Name",
        "type": "Type",
        "info": "Info",
        "path": "Path",
        "size": "Size",
        "year": "Year",
        "month": "Month",
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
        "search": "Busque ou deixe em branco para listar tudo",
        "loading_err": "Erro ao carregar",
        "settings": "Configurações",
        "quit": "Sair",
        "files": "Arquivos",
        "ftp_browser": "FTP",
        "fetching": "Carregando datasets...",
        "name": "Nome",
        "type": "Tipo",
        "info": "Info",
        "path": "Path",
        "size": "Tamanho",
        "year": "Ano",
        "month": "Mês",
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


def t(field: str, default: str = "", lang: str = "en"):
    keys = field.split(".")
    data = TRANSLATIONS.get(lang, TRANSLATIONS["en"])

    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return default

    return data or default
