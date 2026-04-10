from pathlib import Path

import humanize
from pysus.tui.i18n import TRANSLATIONS, t
from pysus.tui.models import ContentManager
from textual import work
from textual.app import ComposeResult
from textual.containers import Center, Grid, Horizontal, Middle, Vertical
from textual.screen import ModalScreen, Screen
from textual.widgets import (
    Button,
    ContentSwitcher,
    DataTable,
    Footer,
    Header,
    Input,
    Label,
    LoadingIndicator,
    ProgressBar,
    Select,
    Static,
    Switch,
    Tree,
)


class LoadingScreen(Screen):
    def compose(self) -> ComposeResult:
        lang = self.app.lang
        yield Header()
        with Middle():
            yield Static(t("welcome", lang=lang), id="welcome-text")
        with Center():
            yield LoadingIndicator(id="loader")
        yield Footer()

    def on_key(self, event) -> None:
        if event.key == "q":
            return
        event.stop()
        event.prevent_default()


class MainScreen(Screen):
    def on_mount(self) -> None:
        self.app.populate_local_tree()

    def compose(self) -> ComposeResult:
        lang = self.app.lang
        home = str(Path.home())
        cachepath = str(self.app.pysus.cachepath).replace(home, "~")

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
                yield Tree(
                    f"{t('files', lang=lang)} ({cachepath})",
                    id="local-tree",
                )
        yield Footer()


class ConfigScreen(Screen):
    def compose(self) -> ComposeResult:
        lang = self.app.lang
        yield Header()
        with Center():
            with Vertical(id="config-container"):
                yield Static(t("settings", lang=lang), id="config-title")

                with Grid(id="config-grid"):
                    yield Label("Language / Idioma")
                    yield Select(
                        [(lang.upper(), lang) for lang in TRANSLATIONS.keys()],
                        value=lang,
                        id="cfg-lang",
                    )

                    yield Label("Dark Mode")
                    yield Switch(value=True, id="cfg-dark")

                yield Button("Save & Apply", variant="success", id="cfg-save")
        yield Footer()

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "cfg-save":
            new_lang = self.query_one("#cfg-lang", Select).value
            if new_lang:
                self.app.lang = new_lang
            self.app.pop_screen()


class InfoModal(ModalScreen):
    BINDINGS = [("escape", "dismiss", "Close")]

    def __init__(self, item, **kwargs):
        super().__init__(**kwargs)
        self.item = item

    def compose(self) -> ComposeResult:
        name = getattr(self.item, "name", "Unknown")
        long_name = getattr(self.item, "long_name", None)
        title = f"{name} ({long_name})" if long_name else name
        lang = self.app.lang

        with Vertical(id="modal-content-wrapper"):
            yield Static(title, id="modal-title")

            info_text = []
            attrs = [
                "description",
                "path",
                "size",
                "year",
                "month",
                "state",
            ]
            for attr in attrs:
                if hasattr(self.item, attr):
                    val = getattr(self.item, attr)
                    if val:
                        label = t(
                            attr,
                            default=attr.replace("_", " ").title(),
                            lang=lang,
                        )

                        if attr == "size":
                            val = humanize.naturalsize(val, binary=True)
                        elif attr == "month":
                            val = t(
                                f"months.{val}", default=str(val), lang=lang
                            )

                        info_text.append(f"[b]{label}:[/b] {val}")

            yield Static(
                "\n".join(info_text) if info_text else "No metadata",
                id="modal-content",
            )
            yield Static(t("esc", lang=lang), id="modal-footer")

    def action_dismiss(self) -> None:
        self.dismiss()


class SearchModal(ModalScreen):
    def compose(self) -> ComposeResult:
        with Center():
            yield Input(placeholder=t("search"), id="search-input")

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)


class FTPScreen(Screen):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.manager = ContentManager()
        self.history = []

    def compose(self) -> ComposeResult:
        lang = self.app.lang
        home = str(Path.home())
        cachepath = str(self.app.pysus.cachepath).replace(home, "~")

        yield Header()
        with Horizontal():
            with Vertical(id="main-container"):
                yield Static(t("ftp_browser", lang=lang), id="panel-label")
                yield DataTable(id="ftp-data-table")
            with Vertical(id="sidebar"):
                yield Static(t("local", lang=lang), id="sidebar-label")
                yield Tree(
                    f"{t('files', lang=lang)} ({cachepath})", id="local-tree"
                )
                yield ProgressBar(id="download-progress", show_percentage=True)
        yield Footer()

    async def on_mount(self) -> None:
        lang = self.app.lang
        table = self.query_one("#ftp-data-table", DataTable)
        table.cursor_type = "row"
        table.add_columns(
            t("name", lang=lang), t("type", lang=lang), t("info", lang=lang)
        )
        table.loading = True
        self.fetch_root()
        self.app.populate_local_tree()

    @work
    async def fetch_root(self) -> None:
        try:
            ftp = await self.app.pysus.get_ftp()
            datasets = await ftp.datasets()

            completed_paths = self.app.pysus.get_completed_remote_paths()

            self.manager.set_items(datasets, downloaded_paths=completed_paths)
            self.manager.populate(self.query_one("#ftp-data-table"))
        except Exception as e:
            self.app.notify(f"Root Error: {e}", severity="error")
        self.query_one("#ftp-data-table").loading = False

    async def on_data_table_row_selected(
        self,
        event: DataTable.RowSelected,
    ) -> None:
        selected_wrapper = self.manager.filtered[event.cursor_row]
        raw_item = selected_wrapper.raw
        if hasattr(raw_item, "content") or hasattr(raw_item, "_fetch_content"):
            self.query_one("#ftp-data-table").loading = True
            self.history.append(self.manager.items)
            self.load_item_content(raw_item)

    @work
    async def load_item_content(self, item) -> None:
        try:
            content = (
                await item._fetch_content()
                if hasattr(item, "_fetch_content")
                else await item.content
            )
            completed_paths = self.app.pysus.get_completed_remote_paths()

            self.manager.set_items(content, downloaded_paths=completed_paths)
            self.manager.populate(self.query_one("#ftp-data-table"))
        except Exception as e:
            self.app.notify(f"Content Error: {e}", severity="error")
        self.query_one("#ftp-data-table").loading = False

    def action_search(self) -> None:
        def perform_search(val: str | None) -> None:
            self.manager.apply_filter(val)
            self.manager.populate(self.query_one("#ftp-data-table"))

        self.app.push_screen(SearchModal(), perform_search)

    def action_back(self) -> None:
        if self.history:
            completed_paths = self.app.pysus.get_completed_remote_paths()

            self.manager.set_items(
                [wrapper.raw for wrapper in self.history.pop()],
                downloaded_paths=completed_paths,
            )
            self.manager.populate(self.query_one("#ftp-data-table"))
        else:
            self.app.pop_screen()
