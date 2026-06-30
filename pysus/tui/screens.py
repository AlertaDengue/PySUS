from __future__ import annotations

from typing import TYPE_CHECKING

import humanize
from pysus.tui.i18n import TRANSLATIONS, t
from pysus.tui.models import ContentManager
from textual import work
from textual.app import ComposeResult
from textual.binding import Binding
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

if TYPE_CHECKING:
    from pysus.tui.types import PySUSApp  # type: ignore


class PySUSScreen(Screen):
    app: PySUSApp


def _get_app(screen: Screen) -> PySUSApp:
    return screen.app  # type: ignore[return-value]


class LoadingScreen(PySUSScreen):
    def compose(self) -> ComposeResult:
        app = _get_app(self)
        lang = app.lang
        yield Header()
        with Vertical(id="loading-container"):
            with Middle():
                yield Static(t("welcome", lang=lang), id="welcome-text")
                yield Static(t("fetching", lang=lang), id="loading-status")
            with Center():
                yield LoadingIndicator(id="loader")
        yield Footer()

    def on_key(self, event) -> None:
        if event.key == "q":
            return
        event.stop()
        event.prevent_default()


class MainScreen(Screen):
    BINDINGS = [
        Binding("f1", "switch_client('ducklake')", "DuckLake", priority=True),
        Binding("f2", "switch_client('ftp')", "FTP", priority=True),
        # Binding("f3", "switch_client('dadosgov')", "DadosGov", priority=True),
        Binding("f10", "push_screen('config')", "Config", priority=True),
        Binding("i", "show_info", "Info"),
        Binding("d", "download", "Download"),
        Binding("/", "search", "Search"),
        Binding("escape", "back", "Back"),
        Binding("q", "quit", "Quit"),
    ]

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.ducklake_manager = ContentManager()
        self.ftp_manager = ContentManager()
        self.dadosgov_manager = ContentManager()
        self._nav_stack: list[tuple[ContentManager, list]] = []

    def compose(self) -> ComposeResult:
        app = _get_app(self)
        lang = app.lang
        yield Header()
        with Horizontal():
            with Vertical(id="main-container"):
                yield Static(t("remote", lang=lang), id="panel-label")
                with ContentSwitcher(id="client-switcher", initial="ducklake"):
                    yield DataTable(id="ducklake")
                    yield DataTable(id="ftp")
                    yield DataTable(id="dadosgov")
                yield Static("", id="download-text")
                yield ProgressBar(id="download-progress", show_percentage=True)
            with Vertical(id="local-sidebar"):
                yield Static(t("local", lang=lang), id="sidebar-label")
                yield Tree(t("files", lang=lang), id="local-tree")
        yield Footer()

    async def on_mount(self) -> None:
        app: PySUSApp = self.app
        app.populate_local_tree()
        self.fetch_ducklake()

    def action_switch_client(self, client_id: str) -> None:
        switcher = self.query_one("#client-switcher", ContentSwitcher)
        switcher.current = client_id
        self._update_panel_label()

        if client_id == "ducklake":
            self.fetch_ducklake()
        elif client_id == "ftp":
            self.fetch_ftp()
        elif client_id == "dadosgov":
            self.fetch_dadosgov()

        self.call_later(self._focus_current_table)

    def _focus_current_table(self) -> None:
        switcher = self.query_one("#client-switcher", ContentSwitcher)
        table = switcher.query_one(f"#{switcher.current}", DataTable)
        table.focus()

    def _update_panel_label(self) -> None:
        try:
            switcher = self.query_one("#client-switcher", ContentSwitcher)
            label = self.query_one("#panel-label", Static)
            client = switcher.current
            if not client:
                client = "ducklake"
            client = client.upper()
            if self._nav_stack:
                label.update(f"{t('remote', lang=self.app.lang)} - {client} ⬅")
            else:
                label.update(f"{t('remote', lang=self.app.lang)} - {client}")
        except Exception:  # noqa
            pass

    def action_back(self) -> None:
        if not self._nav_stack:
            return

        switcher = self.query_one("#client-switcher", ContentSwitcher)
        current_client = switcher.current
        manager = getattr(self, f"{current_client}_manager")
        table = self.query_one(f"#{current_client}", DataTable)

        previous = self._nav_stack.pop()
        manager.set_items(previous[1], clear=True)
        manager.populate(table)
        self._update_panel_label()

    @work
    async def on_data_table_row_selected(
        self, event: DataTable.RowSelected
    ) -> None:
        switcher = self.query_one("#client-switcher", ContentSwitcher)
        current_client = switcher.current
        table = event.data_table

        manager = getattr(self, f"{current_client}_manager")

        if event.cursor_row >= len(manager.filtered):
            return

        selected_wrapper = manager.filtered[event.cursor_row]
        selected_item = selected_wrapper.raw

        table.loading = True
        label = self.query_one("#panel-label", Static)
        label.update(f"{t('loading', lang=self.app.lang)}...")
        self._nav_stack.append((manager, [item.raw for item in manager.items]))

        try:
            new_raw_data = []

            if hasattr(selected_item, "_fetch_content"):
                new_raw_data = await selected_item._fetch_content()
            elif hasattr(selected_item, "_fetch_files"):
                new_raw_data = await selected_item._fetch_files()
            elif hasattr(selected_item, "groups") and selected_item.groups:
                new_raw_data = [
                    g for g in selected_item.groups if hasattr(g, "record")
                ]
            elif hasattr(selected_item, "files") and selected_item.files:
                new_raw_data = list(selected_item.files)
            else:
                if self._nav_stack:
                    self._nav_stack.pop()
                table.loading = False
                return

            completed_paths = self.app.pysus.get_completed_remote_paths()

            manager.set_items(
                new_raw_data,
                downloaded_paths=completed_paths,
                source=current_client,
            )

            manager.populate(table)
            self._update_panel_label()

        except Exception as e:  # noqa
            if self._nav_stack:
                self._nav_stack.pop()
            self.app.notify(f"Navigation Error: {e}", severity="error")
        finally:
            table.loading = False

    @work
    async def fetch_ducklake(self) -> None:
        table = self.query_one("#ducklake", DataTable)
        if table.row_count > 0:
            return

        table.cursor_type = "row"
        app = _get_app(self)
        lang = app.lang

        table.clear(columns=True)
        table.add_columns(
            t("name", lang=lang),
            t("type", lang=lang),
            t("modified", lang=lang),
            t("size", lang=lang),
            t("info", lang=lang),
        )
        table.loading = True
        try:
            ducklake = await app.pysus.get_ducklake()
            datasets = await ducklake.datasets()
            completed_paths = app.pysus.get_completed_remote_paths()
            self.ducklake_manager.set_items(
                datasets, downloaded_paths=completed_paths, source="ducklake"
            )
            self.ducklake_manager.populate(table)
        except Exception as e:  # noqa
            app.notify(f"DuckLake Error: {e}", severity="error")
        finally:
            table.loading = False

    @work
    async def fetch_ftp(self) -> None:
        table = self.query_one("#ftp", DataTable)

        if table.row_count > 0:
            return

        table.cursor_type = "row"
        app = _get_app(self)
        lang = app.lang

        table.clear(columns=True)
        table.add_columns(
            t("name", lang=lang),
            t("type", lang=lang),
            t("modified", lang=lang),
            t("size", lang=lang),
            t("info", lang=lang),
        )

        table.loading = True
        try:
            ftp = await app.pysus.get_ftp()
            files = await ftp.datasets()
            completed_paths = app.pysus.get_completed_remote_paths()

            self.ftp_manager.set_items(
                files, downloaded_paths=completed_paths, source="ftp"
            )
            self.ftp_manager.populate(table)
        except Exception as e:  # noqa
            app.notify(f"FTP Error: {e}", severity="error")
        finally:
            table.loading = False

    @work
    async def fetch_dadosgov(self) -> None:
        app: PySUSApp = self.app
        table = self.query_one("#dadosgov", DataTable)
        if table.row_count > 0:
            return

        table.cursor_type = "row"
        lang = app.lang
        table.clear(columns=True)
        table.add_columns(
            t("name", lang=lang),
            t("type", lang=lang),
            t("modified", lang=lang),
            t("size", lang=lang),
            t("info", lang=lang),
        )
        table.loading = True
        try:
            dadosgov = await app.pysus.get_dadosgov()
            datasets = await dadosgov.datasets()
            completed_paths = app.pysus.get_completed_remote_paths()
            self.dadosgov_manager.set_items(
                datasets, downloaded_paths=completed_paths, source="dadosgov"
            )
            self.dadosgov_manager.populate(table)
        except Exception as e:  # noqa
            self.app.notify(f"DadosGov Error: {e}", severity="error")
        finally:
            table.loading = False


class ConfigScreen(Screen):
    def compose(self) -> ComposeResult:
        app: PySUSApp = self.app
        lang = app.lang
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
        app: PySUSApp = self.app
        if event.button.id == "cfg-save":
            new_lang = self.query_one("#cfg-lang", Select).value
            if new_lang:
                app.lang = new_lang
            self.app.pop_screen()


class InfoModal(ModalScreen):
    BINDINGS = [("escape", "dismiss", "Close")]

    def __init__(self, item, **kwargs):
        super().__init__(**kwargs)
        self.item = item

    def compose(self) -> ComposeResult:
        app: PySUSApp = self.app
        name = getattr(self.item, "name", "Unknown")
        long_name = getattr(self.item, "long_name", None)
        title = f"{name} ({long_name})" if long_name else name
        lang = app.lang

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
                                f"months.{val}",
                                default=str(val),
                                lang=lang,
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
            yield Input(
                placeholder=t("search", default="Search..."),
                id="search-input",
            )

    def on_mount(self) -> None:
        self.query_one(Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)
