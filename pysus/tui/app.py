import asyncio
from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.binding import Binding
from textual.widgets import (
    Header,
    Footer,
    Static,
    DataTable,
    Tree,
    LoadingIndicator,
    ContentSwitcher,
)
from textual.containers import Center, Middle, Horizontal, Vertical
from textual import work

from pysus import __version__
from pysus.api import PySUSClient
from pysus.api.ftp.models import File

TRANSLATIONS = {
    "en": {
        "welcome": "Welcome to PySUS Client",
        "clients": "Clients",
        "local": "Local",
        "loading_err": "Failed to load",
        "quit": "Quit",
        "files": "Files",
        "ftp_browser": "FTP",
        "fetching": "Fetching datasets...",
        "name": "Name",
        "type": "Type",
        "info": "Info",
    },
    "pt": {
        "welcome": "Bem-vindo ao Cliente PySUS",
        "clients": "Clientes",
        "local": "Local",
        "loading_err": "Erro ao carregar",
        "quit": "Sair",
        "files": "Arquivos",
        "ftp_browser": "FTP",
        "fetching": "Carregando datasets...",
        "name": "Nome",
        "type": "Tipo",
        "info": "Info",
    },
}


def t(field: str, default: str = None, lang: str = "en"):
    return TRANSLATIONS.get(lang, {}).get(field, default)


class FTPScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.current_items = []
        self.history = []

    def compose(self) -> ComposeResult:
        lang = self.app.lang
        yield Header()
        with Horizontal():
            with Vertical(id="main-container"):
                yield Static(t("ftp_browser", lang=lang), id="panel-label")
                yield DataTable(id="ftp-data-table")
            with Vertical(id="sidebar"):
                yield Static(t("local", lang=lang), id="sidebar-label")
                yield Tree(f"{t('files', lang=lang)}", id="ftp-local-tree")
        yield Footer()

    async def on_mount(self) -> None:
        lang = self.app.lang
        table = self.query_one("#ftp-data-table", DataTable)
        table.cursor_type = "row"
        table.add_columns(
            t("name", lang=lang),
            t("type", lang=lang),
            t("info", lang=lang),
        )
        table.loading = True
        self.fetch_root()

    @work
    async def fetch_root(self) -> None:
        table = self.query_one("#ftp-data-table", DataTable)
        try:
            ftp = await self.app.pysus.get_ftp()
            datasets = await ftp.datasets()
            self.update_table(datasets)
        except Exception as e:
            self.app.notify(f"Root Error: {e}", severity="error")
            table.loading = False

    def update_table(self, items: list) -> None:
        table = self.query_one("#ftp-data-table", DataTable)
        table.clear()
        self.current_items = items

        for item in items:
            name = getattr(item, "name", str(item))
            item_type = item.__class__.__name__
            extra = ""

            if hasattr(item, "size"):
                extra = f"{item.size / 1024:.1f} KB"
            elif hasattr(item, "long_name"):
                extra = item.long_name

            table.add_row(name, item_type, extra)

        table.loading = False
        table.focus()

    async def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        selected_item = self.current_items[event.cursor_row]

        if hasattr(selected_item, "content") or hasattr(
            selected_item, "_fetch_content"
        ):
            table = self.query_one("#ftp-data-table", DataTable)
            table.loading = True
            self.history.append(list(self.current_items))
            self.load_item_content(selected_item)
        elif isinstance(selected_item, File):
            self.app.notify(f"Downloading {selected_item.name}...", title="FTP")

    @work
    async def load_item_content(self, item) -> None:
        try:
            if hasattr(item, "_fetch_content"):
                content = await item._fetch_content()
            else:
                content = await item.content
            self.update_table(content)
        except Exception as e:
            self.app.notify(f"Content Error: {e}", severity="error")
            self.query_one("#ftp-data-table").loading = False

    def action_back(self) -> None:
        if self.history:
            previous_items = self.history.pop()
            self.update_table(previous_items)
        else:
            self.app.pop_screen()


class MainScreen(Screen):
    def compose(self) -> ComposeResult:
        lang = self.app.lang
        yield Header()
        with Horizontal():
            with Vertical(id="main-container"):
                yield Static("DuckLake", id="panel-label")
                with ContentSwitcher(id="client-switcher", initial="ducklake"):
                    yield DataTable(id="ducklake")
                    yield DataTable(id="ftp")
                    yield DataTable(id="dadosgov")

            with Vertical(id="sidebar"):
                yield Static(t("local", lang=lang), id="sidebar-label")
                yield Tree(f"{t('files', lang=lang)}", id="local-tree")
        yield Footer()


class LoadingScreen(Screen):
    def compose(self) -> ComposeResult:
        lang = self.app.lang
        yield Header()
        with Middle():
            yield Static(t("welcome", lang=lang), id="welcome-text")
        with Center():
            yield LoadingIndicator(id="loader")
        yield Footer()


class PySUS(App):
    TITLE = "PySUS"
    SUB_TITLE = f"v{__version__}"

    BINDINGS = [
        Binding("f1", "switch_client('ducklake')", "DuckLake"),
        Binding("f2", "push_screen('ftp_screen')", "FTP"),
        Binding("f3", "switch_client('dadosgov')", "DadosGov"),
        Binding("q", "quit", "Quit"),
        Binding("escape", "back", "Back"),
        Binding("h", "focus_previous", "Focus Prev", show=False),
        Binding("l", "focus_next", "Focus Next", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
    ]

    SCREENS = {
        "main": MainScreen,
        "ftp_screen": FTPScreen,
    }

    def __init__(self, lang="en", **kwargs):
        self.lang = lang if lang in TRANSLATIONS else "en"
        super().__init__(**kwargs)

    CSS = """
    LoadingScreen Middle {
        height: 90%;
        width: 100%;
        align: center middle;
    }

    #welcome-text {
        text-style: bold;
        color: white;
        width: 100%;
        text-align: center;
    }

    #loader {
        width: auto;
        height: auto;
    }

    #main-container {
        width: 70%;
        margin-right: 1;
    }

    #sidebar {
        width: 30%;
    }

    #panel-label, #sidebar-label {
        padding-left: 1;
        text-style: bold;
        color: white;
    }

    #client-switcher, #ftp-data-table, #ftp-local-tree {
        height: 1fr;
        border: round white;
        padding: 1;
    }

    #client-switcher:focus-within, #ftp-data-table:focus, #ftp-local-tree:focus {
        border: double white;
    }

    #local-tree {
        height: 1fr;
        border: round white;
        padding: 1;
        background: transparent;
    }

    #local-tree:focus-within {
        border: double white;
    }

    DataTable {
        height: 1fr;
        border: none;
    }
    """

    async def on_mount(self) -> None:
        self.pysus = PySUSClient()
        await self.push_screen(LoadingScreen())
        self.init()

    @work
    async def init(self) -> None:
        try:
            await self.pysus.__aenter__()
            await asyncio.sleep(2)
            self.switch_screen("main")
        except Exception as e:
            err_msg = t("loading_err", lang=self.lang)
            self.notify(f"{err_msg}: {e}", severity="error")

    def action_back(self) -> None:
        if isinstance(self.screen, FTPScreen):
            self.screen.action_back()

    def action_switch_client(self, client_id: str) -> None:
        if self.screen_stack and isinstance(self.screen, MainScreen):
            switcher = self.query_one("#client-switcher", ContentSwitcher)
            switcher.current = client_id

            client_names = {
                "ducklake": "DuckLake",
                "ftp": "FTP",
                "dadosgov": "DadosGov",
            }
            self.query_one("#panel-label").update(
                client_names.get(client_id, "Unknown")
            )

            self.query_one(f"#{client_id}").focus()

    def action_cursor_down(self) -> None:
        if isinstance(self.focused, (DataTable, Tree)):
            self.focused.action_cursor_down()

    def action_cursor_up(self) -> None:
        if isinstance(self.focused, (DataTable, Tree)):
            self.focused.action_cursor_up()

    async def action_quit(self) -> None:
        await self.pysus.__aexit__(None, None, None)
        self.exit()


if __name__ == "__main__":
    app = PySUS(lang="pt")
    app.run()
