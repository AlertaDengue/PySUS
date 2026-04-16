from dataclasses import dataclass
from typing import Any

import humanize
from textual.widgets import DataTable


@dataclass
class SourceRef:
    source: str  # "ducklake", "ftp", "local"
    path: str | None = None
    is_downloaded: bool = False
    remote_modified: str | None = None


class BaseTUIItem:
    def __init__(self, raw):
        self.raw = raw
        self.name = getattr(raw, "name", str(raw))
        self.type = raw.__class__.__name__
        self._links: list[SourceRef] = []
        self.is_downloading: bool = False

    @property
    def source_key(self) -> str:
        parts = [self.name]
        for attr in ("year", "month", "state"):
            val = getattr(self.raw, attr, None)
            if val:
                parts.append(str(val))
        return ":".join(parts)

    def add_link(
        self,
        source: str,
        path: str | None = None,
        is_downloaded: bool = False,
        remote_modified: str | None = None,
    ):
        self._links.append(
            SourceRef(source, path, is_downloaded, remote_modified)
        )

    @property
    def links(self) -> list[SourceRef]:
        return self._links

    def get_columns(self) -> list[str]:
        return [self.name, self.type, "", ""]


class File(BaseTUIItem):
    def __init__(
        self,
        raw,
        is_downloaded: bool = False,
        is_downloading: bool = False,
        source: str = "unknown",
        path: str | None = None,
        remote_modified: str | None = None,
    ):
        super().__init__(raw)
        self.is_downloaded = is_downloaded
        self.is_downloading = is_downloading
        self._source = source
        if path:
            self.add_link(source, path, is_downloaded, remote_modified)

    @property
    def size(self) -> str:
        raw_size = getattr(self.raw, "size", None)
        if raw_size is not None and isinstance(raw_size, (int, float)):
            return humanize.naturalsize(raw_size, binary=True)
        return "-"

    @property
    def modified(self) -> str:
        raw_mod = getattr(self.raw, "modify", None)
        if raw_mod:
            if hasattr(raw_mod, "strftime"):
                return raw_mod.strftime("%Y-%m-%d")
            elif hasattr(raw_mod, "modified"):
                mod = raw_mod.modified
                if hasattr(mod, "strftime"):
                    return mod.strftime("%Y-%m-%d")
        raw_dt = getattr(self.raw, "modify_date", None)
        if raw_dt and hasattr(raw_dt, "strftime"):
            return raw_dt.strftime("%Y-%m-%d")
        return "-"

    def get_columns(self) -> list[str]:
        display_name = self.name
        link_indicators = []
        sources_seen = set()
        item_type = self.type
        downloaded = self.is_downloaded

        if self.is_downloading:
            link_indicators.append("[yellow]◐[/yellow]")

        for link in self.links:
            sources_seen.add(link.source)
            if link.is_downloaded:
                downloaded = True
                link_indicators.append("[green]✓[/green]")

        if hasattr(self, "_source") and self._source not in sources_seen:
            if downloaded or self.is_downloaded:
                link_indicators = ["[green]✓[/green]"]

        if not link_indicators:
            if downloaded or self.is_downloaded:
                link_indicators = ["[green]✓[/green]"]
            elif item_type in (
                "Dataset",
                "BaseRemoteDataset",
                "ConjuntoDados",
            ):
                link_indicators = ["[yellow]📦[/yellow]"]
                item_type = "Dataset"
            elif item_type in ("File", "CatalogFile"):
                link_indicators = ["[yellow] [/yellow]"]
            elif item_type in ("Group", "DatasetGroup"):
                link_indicators = ["[yellow]📁[/yellow]"]
                item_type = "Group"

        if link_indicators:
            display_name = f"{display_name} {''.join(link_indicators)}"

        long_name = getattr(self.raw, "long_name", None) or ""
        return [display_name, item_type, self.modified, self.size, long_name]


class Group(BaseTUIItem):
    def get_columns(self) -> list[str]:
        desc = getattr(self.raw, "long_name", "Directory")
        modified = "-"
        if hasattr(self.raw, "modify") and self.raw.modify:
            if hasattr(self.raw.modify, "strftime"):
                modified = self.raw.modify.strftime("%Y-%m-%d")
        return [self.name, "Group", desc, modified, ""]


class Dataset(BaseTUIItem):
    def get_columns(self) -> list[str]:
        long_name = getattr(self.raw, "long_name", self.name)
        modified = "-"
        if hasattr(self.raw, "record") and hasattr(self.raw.record, "modified"):
            mod = self.raw.record.modified
            if hasattr(mod, "strftime"):
                modified = mod.strftime("%Y-%m-%d")
        return [self.name, "File", long_name, modified, ""]


class ContentManager:
    def __init__(self):
        self.items: list[BaseTUIItem] = []
        self.filtered: list[BaseTUIItem] = []
        self._item_index: dict[str, BaseTUIItem] = {}
        self._search_text: str | None = None

    @property
    def search_text(self) -> str | None:
        return self._search_text

    def _normalize_key(self, name: str) -> str:
        return name.replace(".parquet", "").replace(".dbc", "").upper()

    def _get_key(self, item: Any) -> str:
        base = self._normalize_key(getattr(item, "name", ""))
        year = getattr(item, "year", None)
        month = getattr(item, "month", None)
        if year:
            base += f":{year}"
        if month:
            base += f":{month:02d}"
        return base

    def set_items(
        self,
        raw_items: list,
        downloaded_paths: set[str] | None = None,
        downloading_paths: set[str] | None = None,
        source: str = "unknown",
        clear: bool = True,
    ) -> None:
        if clear:
            self.items = []
            self._item_index = {}

        downloaded_paths = downloaded_paths or set()
        downloading_paths = downloading_paths or set()

        new_items = []
        for item in raw_items:
            key = self._get_key(item)
            is_done = str(getattr(item, "path", None)) in downloaded_paths
            is_downloading = (
                str(getattr(item, "path", None)) in downloading_paths
            )
            remote_modified = None

            if hasattr(item, "remote_modified"):
                remote_modified = str(item.remote_modified)
            elif hasattr(item, "modify_date"):
                remote_modified = str(item.modify_date)

            file_obj = File(
                item,
                is_downloaded=is_done,
                is_downloading=is_downloading,
                source=source,
                path=getattr(item, "path", None),
                remote_modified=remote_modified,
            )

            if key in self._item_index:
                existing = self._item_index[key]
                for link in file_obj.links:
                    existing.add_link(
                        link.source,
                        link.path,
                        link.is_downloaded,
                        link.remote_modified,
                    )
            else:
                self._item_index[key] = file_obj
                new_items.append(file_obj)

        self.items.extend(new_items)
        self.filtered = list(self.items)

    def set_downloading(self, path: str, is_downloading: bool) -> None:
        for item in self.items:
            if str(getattr(item.raw, "path", None)) == path:
                item.is_downloading = is_downloading
                break

    def apply_filter(self, search_text: str | None) -> None:
        self._search_text = search_text
        if not search_text:
            self.filtered = list(self.items)
        else:
            search_text = search_text.lower()
            self.filtered = [
                item for item in self.items if search_text in item.name.lower()
            ]

    def populate(self, table: DataTable, reset_cursor: bool = False) -> None:
        if not reset_cursor:
            cursor_row = table.cursor_row
        else:
            cursor_row = None
        table.clear()
        for item in self.filtered:
            table.add_row(*item.get_columns())
        if cursor_row is not None and cursor_row < table.row_count:
            table.move_cursor(row=cursor_row)
