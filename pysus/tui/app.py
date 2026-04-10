import asyncio

import humanize
from pysus import __version__
from pysus.api import PySUSClient
from pysus.api.client import DownloadStatus
from pysus.tui.i18n import TRANSLATIONS, t
from pysus.tui.screens import (
    ConfigScreen,
    FTPScreen,
    InfoModal,
    LoadingScreen,
    MainScreen,
)
from textual import work
from textual.app import App
from textual.binding import Binding
from textual.widgets import ContentSwitcher, DataTable, ProgressBar, Tree


class PySUS(App):
    TITLE = "PySUS"
    SUB_TITLE = f"v{__version__}"
    CSS_PATH = "style.tcss"

    BINDINGS = [
        Binding("f1", "switch_client('ducklake')", "DuckLake", priority=True),
        Binding("f2", "push_screen('ftp_screen')", "FTP", priority=True),
        Binding("f3", "switch_client('dadosgov')", "DadosGov", priority=True),
        Binding("f10", "push_screen('config')", "Config", priority=True),
        Binding("i", "show_info", "Info"),
        Binding("d", "download", "Download"),
        Binding("/", "search", "Search"),
        Binding("escape", "back", "Back"),
        Binding("q", "quit", "Quit"),
        Binding("h", "focus_previous", "Focus Prev", show=False),
        Binding("l", "focus_next", "Focus Next", show=False),
        Binding("j", "cursor_down", "Down", show=False),
        Binding("k", "cursor_up", "Up", show=False),
    ]

    SCREENS = {
        "main": MainScreen,
        "ftp_screen": FTPScreen,
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
        except Exception as e:
            err_msg = t("loading_err", lang=self.lang)
            self.notify(f"{err_msg}: {e}", severity="error")

    @work
    async def action_download(self) -> None:
        if not isinstance(self.screen, FTPScreen):
            return

        try:
            table = self.screen.query_one("#ftp-data-table", DataTable)
            if table.cursor_row is None:
                return

            saved_row_index = table.cursor_row
            selected_wrapper = self.screen.manager.filtered[saved_row_index]
            file_item = selected_wrapper.raw

            progress_bar = self.screen.query_one(
                "#download-progress", ProgressBar
            )
            progress_bar.add_class("visible")

            def progress_callback(current: int, total: int = None) -> None:
                if total and total > 0:
                    progress_bar.update(total=total, progress=current)
                else:
                    progress_bar.update(progress=current)

            await self.pysus.download_to_parquet(
                file_item,
                callback=progress_callback,
            )

            progress_bar.remove_class("visible")

            completed_paths = self.pysus.get_completed_remote_paths()
            self.screen.manager.set_items(
                [w.raw for w in self.screen.manager.items],
                downloaded_paths=completed_paths,
            )

            self.screen.manager.populate(table)

            if saved_row_index < table.row_count:
                table.move_cursor(row=saved_row_index)

            self.populate_local_tree()

        except Exception as e:
            if "200 Type set to I" in str(e):
                return

            self.notify(f"{e}", severity="error")
            try:
                self.screen.query_one("#download-progress").remove_class(
                    "visible"
                )
            except Exception:
                pass
            self.populate_local_tree()

    def action_back(self) -> None:
        if isinstance(self.screen, FTPScreen):
            self.screen.action_back()
        elif isinstance(self.screen, MainScreen):
            return
        elif len(self.screen_stack) > 1:
            self.pop_screen()

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

    def action_search(self) -> None:
        if isinstance(self.screen, FTPScreen):
            self.screen.action_search()

    def action_show_info(self) -> None:
        if isinstance(self.screen, FTPScreen):
            try:
                table = self.screen.query_one("#ftp-data-table", DataTable)
                if table.cursor_row is not None:
                    selected_wrapper = self.screen.manager.filtered[
                        table.cursor_row
                    ]
                    self.push_screen(InfoModal(selected_wrapper.raw))
            except Exception as e:
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

    def populate_local_tree(self) -> None:
        try:
            tree = self.screen.query_one("Tree")
        except Exception:
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
