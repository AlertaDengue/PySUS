from dataclasses import dataclass, field

from pysus.api.types import ColumnType, Origin


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
    def from_schema(
        cls, name: str, dtype: ColumnType, description: str = ""
    ) -> "Column":
        """Create a Column with a description provided from the database."""
        return cls(
            name=name,
            description=description,
            dtype=dtype,
        )
