import humanize
from textual.widgets import DataTable


class BaseTUIItem:
    def __init__(self, raw):
        self.raw = raw
        self.name = getattr(raw, "name", str(raw))
        self.type = raw.__class__.__name__

    def get_columns(self) -> list[str]:
        return [self.name, self.type, ""]


class File(BaseTUIItem):
    def __init__(self, raw, is_downloaded: bool = False):
        super().__init__(raw)
        self.is_downloaded = is_downloaded

    @property
    def size(self) -> str:
        if hasattr(self.raw, "size") and self.raw.size is not None:
            return humanize.naturalsize(self.raw.size, binary=True)
        return "-"

    def get_columns(self) -> list[str]:
        display_name = self.name

        if self.is_downloaded:
            display_name = f"[green]{self.name}[/green]"

        return [display_name, "File", self.size]


class Group(BaseTUIItem):
    def get_columns(self) -> list[str]:
        desc = getattr(self.raw, "long_name", "Directory")
        return [self.name, "Group", desc]


class ContentManager:
    def __init__(self):
        self.items: list[BaseTUIItem] = []
        self.filtered: list[BaseTUIItem] = []

    def set_items(
        self,
        raw_items: list,
        downloaded_paths: set[str] | None = None,
    ) -> None:
        downloaded_paths = downloaded_paths or set()
        self.items = []
        for item in raw_items:
            cls_name = item.__class__.__name__
            if cls_name == "File":
                is_done = str(getattr(item, "path", None)) in downloaded_paths
                self.items.append(File(item, is_downloaded=is_done))
            elif cls_name in ("Group", "Dataset", "Directory"):
                self.items.append(Group(item))
            else:
                self.items.append(BaseTUIItem(item))
        self.filtered = list(self.items)

    def apply_filter(self, search_text: str) -> None:
        if not search_text:
            self.filtered = list(self.items)
        else:
            search_text = search_text.lower()
            self.filtered = [
                item for item in self.items if search_text in item.name.lower()
            ]

    def populate(self, table: DataTable) -> None:
        table.clear()
        for item in self.filtered:
            table.add_row(*item.get_columns())
