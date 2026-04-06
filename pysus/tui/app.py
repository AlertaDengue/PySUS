from pysus.api.ducklake.client import DuckLake
from textual.app import App, ComposeResult
from textual.containers import Container
from textual.widgets import Footer, Header, Label, ListItem, ListView


class PySUS(App):
    CSS = """
    Screen {
        align: center middle;
    }
    Container {
        width: 90%;
        height: 90%;
        border: solid;
    }
    """

    BINDINGS = [("q", "quit", "Quit"), ("d", "download", "Download Selected")]

    def __init__(self):
        super().__init__()
        self.lake = DuckLake()

    async def on_mount(self):
        self.notify("Checking catalog updates...")
        await self.lake._load_catalog()
        self.notify("Catalog ready!")
        self.push_screen("browser")

    def compose(self) -> ComposeResult:
        yield Header()
        with Container():
            yield Label("Select a DataSUS File:")
            yield ListView(
                ListItem(Label("SIASUS - 2023")),
                ListItem(Label("SINASC - 2023")),
                id="file_list",
            )
        yield Footer()

    def action_download(self) -> None:
        self.notify("Download started...")


if __name__ == "__main__":
    app = PySUS()
    app.run()
