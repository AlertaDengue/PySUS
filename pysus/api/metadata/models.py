from dataclasses import dataclass, field

from pysus.api.types import ColumnType, Origin


def lookup_column_meta(name: str) -> dict[str, str] | None:
    """Look up column metadata from the global columns.py constants.

    Returns the {dataset: description} dict if the column name exists
    as a constant in columns.py, or None if not found.
    """
    try:
        from pysus.api.ducklake.catalog import columns as _cols

        return getattr(_cols, name.upper(), None)
    except ImportError:
        return None


def pick_description(meta: dict[str, str] | None) -> str:
    """Pick the best description from a column metadata dict."""
    if meta is None:
        return ""
    for desc in meta.values():
        if desc:
            return desc
    return ""


@dataclass
class Dataset:
    name: str
    long_name: str
    description: str


@dataclass
class DatasetGroup:
    name: str
    long_name: str
    description: str


@dataclass
class FileMeta:
    name: str
    path: str
    size: int
    state: str | None = None
    uf: str | None = None
    year: int | None = None
    month: int | None = None
    origin_path: str | None = None
    origin_size: int | None = None


@dataclass
class File:
    origin: Origin
    dataset: Dataset | None = None
    group: DatasetGroup | None = None
    columns: list["Column"] = field(default_factory=list)
    _meta: FileMeta | None = None


@dataclass
class Column:
    name: str
    description: str
    dtype: ColumnType

    @classmethod
    def from_schema(cls, name: str, dtype: ColumnType) -> "Column":
        """Create a Column from a file schema, looking up description from columns.py."""
        return cls(
            name=name,
            description=pick_description(lookup_column_meta(name)),
            dtype=dtype,
        )
