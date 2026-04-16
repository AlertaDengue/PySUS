from __future__ import annotations

import asyncio

from pysus import __version__
from pysus.api import PySUSClient
from pysus.api.client import DownloadStatus
from pysus.tui.i18n import TRANSLATIONS, t
from pysus.tui.screens import (
    ConfigScreen,
    InfoModal,
    LoadingScreen,
    MainScreen,
    SearchModal,
)
from textual import work
from textual.app import App
from textual.binding import Binding
from textual.widgets import (
    ContentSwitcher,
    DataTable,
    ProgressBar,
    Static,
    Tree,
)


class PySUS(App):
    TITLE = "PySUS"
    SUB_TITLE = f"v{__version__}"
    CSS_PATH = "style.tcss"

    lang: str
    pysus: PySUSClient

    BINDINGS = [
        Binding("escape", "back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("f10", "push_screen('config')", "Config", priority=True),
        Binding("i", "show_info", "Info"),
        Binding("d", "download", "Download"),
        Binding("/", "search", "Search"),
        Binding("h", "focus_previous", "Focus Prev", show=False),
        Binding("l", "focus_next", "Focus Next", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
    ]

    SCREENS = {
        "main": MainScreen,
        "config": ConfigScreen,
    }

    def __init__(self, lang="en", **kwargs):
        self.lang = lang if lang in TRANSLATIONS else "en"
        super().__init__(**kwargs)

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
        except Exception as e:  # noqa
            err_msg = t("loading_err", lang=self.lang)
            self.notify(f"{err_msg}: {e}", severity="error")

    @work
    async def action_download(self) -> None:
        screen = self.screen
        if not isinstance(screen, MainScreen):
            return

        switcher = screen.query_one("#client-switcher", ContentSwitcher)
        current_client = switcher.current

        if current_client == "ducklake":
            table = screen.query_one("#ducklake", DataTable)
            manager = screen.ducklake_manager
        elif current_client == "ftp":
            table = screen.query_one("#ftp", DataTable)
            manager = screen.ftp_manager
        else:
            table = screen.query_one("#dadosgov", DataTable)
            manager = screen.dadosgov_manager

        if table.cursor_row is None:
            return

        saved_row_index = table.cursor_row
        selected_wrapper = manager.filtered[saved_row_index]
        file_item = selected_wrapper.raw

        progress_bar = screen.query_one(
            "#download-progress",
            ProgressBar,
        )
        progress_bar.add_class("visible")

        download_text = screen.query_one("#download-text", Static)
        download_text.update(f"Downloading: {file_item.name}")
        download_text.add_class("visible")

        def run_download():
            import anyio

            async def do_download():
                return await self.pysus.download_to_parquet(file_item)

            return anyio.run(do_download)

        await asyncio.get_event_loop().run_in_executor(None, run_download)

        progress_bar.remove_class("visible")
        download_text.remove_class("visible")
        download_text.update("")

        completed_paths = self.pysus.get_completed_remote_paths()
        manager.set_items(
            [w.raw for w in manager.items],
            downloaded_paths=completed_paths,
        )

        if hasattr(manager, "search_text") and manager.search_text:
            manager.apply_filter(manager.search_text)

        manager.populate(table)
        self.populate_local_tree()

    @work
    async def refresh_local_tree(self) -> None:
        await asyncio.sleep(0.5)
        self.populate_local_tree()

    def action_back(self) -> None:
        if isinstance(self.screen, ConfigScreen):
            self.pop_screen()

    def action_search(self) -> None:
        screen = self.screen
        if not isinstance(screen, MainScreen):
            return

        def perform_search(val: str | None) -> None:
            switcher = screen.query_one("#client-switcher", ContentSwitcher)
            current_client = switcher.current
            if current_client == "ducklake":
                screen.ducklake_manager.apply_filter(val)
                screen.ducklake_manager.populate(
                    screen.query_one("#ducklake", DataTable)
                )
            elif current_client == "ftp":
                screen.ftp_manager.apply_filter(val)
                screen.ftp_manager.populate(screen.query_one("#ftp", DataTable))
            else:
                screen.dadosgov_manager.apply_filter(val)
                screen.dadosgov_manager.populate(
                    screen.query_one("#dadosgov", DataTable)
                )

        self.push_screen(SearchModal(), perform_search)

    def action_show_info(self) -> None:
        screen = self.screen
        if not isinstance(screen, MainScreen):
            return

        switcher = screen.query_one("#client-switcher", ContentSwitcher)
        current_client = switcher.current

        if current_client == "ducklake":
            table = screen.query_one("#ducklake", DataTable)
            manager = screen.ducklake_manager
        elif current_client == "ftp":
            table = screen.query_one("#ftp", DataTable)
            manager = screen.ftp_manager
        else:
            table = screen.query_one("#dadosgov", DataTable)
            manager = screen.dadosgov_manager

        try:
            if table.cursor_row is not None:
                selected_wrapper = manager.filtered[table.cursor_row]
                self.push_screen(InfoModal(selected_wrapper.raw))
        except Exception as e:  # noqa
            self.notify(f"Metadata error: {e}", severity="error")

    def action_cursor_down(self) -> None:
        if isinstance(self.focused, (DataTable, Tree)):
            self.focused.action_cursor_down()

    def action_cursor_up(self) -> None:
        if isinstance(self.focused, (DataTable, Tree)):
            self.focused.action_cursor_up()

    async def action_quit(self) -> None:
        await self.pysus.__aexit__(None, None, None)
        self.exit()

    def on_screen_activated(self) -> None:
        if isinstance(self.screen, MainScreen):
            self.populate_local_tree()

    def populate_local_tree(self) -> None:
        screen = self.screen
        if not isinstance(screen, MainScreen):
            return
        try:
            tree = screen.query_one("#local-tree", Tree)
        except Exception:  # noqa
            return

        tree.clear()
        tree.root.expand_all()
        hierarchy = self.pysus.get_local_hierarchy()

        status_icons = {
            DownloadStatus.COMPLETED: "ok",
            DownloadStatus.DOWNLOADING: "⏳",
            DownloadStatus.FAILED: "❌",
            DownloadStatus.PENDING: "💤",
            DownloadStatus.MISSING: "❓",
        }

        for client, datasets in hierarchy.items():
            client_node = tree.root.add(f"📂 {client}", expand=True)
            for dataset, groups in datasets.items():
                ds_node = client_node.add(f"📦 {dataset}", expand=True)
                for group, files in groups.items():
                    parent = ds_node.add(f"📁 {group}") if group else ds_node
                    for f in files:
                        status = status_icons.get(f["status"], None)

                        if not status:
                            icon = "📄 "
                        elif status == "ok":
                            icon = ""
                        else:
                            icon = f"{status} "

                        parent.add_leaf(f"{icon}{f['name']}", data=f["record"])


if __name__ == "__main__":
    app = PySUS(lang="pt")
    app.run()
