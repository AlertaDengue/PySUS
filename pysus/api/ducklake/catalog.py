import enum
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Sequence,
    String,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


file_columns = Table(
    "file_columns",
    Base.metadata,
    Column("file_id", Integer, ForeignKey("pysus.files.id"), primary_key=True),
    Column(
        "column_id",
        Integer,
        ForeignKey("pysus.dataset_columns.id"),
        primary_key=True,
    ),
    schema="pysus",
)


class CatalogTable(Base):
    __abstract__ = True
    __table_args__: tuple = ({"schema": "pysus"},)


class Origin(enum.Enum):
    FTP = "ftp"
    API = "api"


class CatalogDataset(CatalogTable):
    __tablename__ = "datasets"

    id = Column(
        Integer,
        Sequence("datasets_id_seq", schema="pysus"),
        primary_key=True,
    )
    name = Column(String, nullable=False, unique=True, index=True)
    long_name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    origin = Column(Enum(Origin), nullable=False)

    groups = relationship(
        "DatasetGroup",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    files = relationship(
        "CatalogFile",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )
    columns = relationship(
        "ColumnDefinition",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class ColumnDefinition(CatalogTable):
    __tablename__ = "dataset_columns"

    id = Column(
        Integer,
        Sequence("columns_id_seq", schema="pysus"),
        primary_key=True,
    )
    dataset_id = Column(
        Integer,
        ForeignKey("pysus.datasets.id"),
        nullable=False,
        index=True,
    )
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    description = Column(String, nullable=True)
    nullable = Column(Boolean, nullable=False, default=True)

    dataset = relationship("CatalogDataset", back_populates="columns")
    files = relationship(
        "CatalogFile",
        secondary=file_columns,
        back_populates="columns",
    )

    __table_args__ = (
        Index("ix_columns_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )


class DatasetGroup(CatalogTable):
    __tablename__ = "dataset_groups"

    id = Column(
        Integer,
        Sequence("groups_id_seq", schema="pysus"),
        primary_key=True,
    )
    name = Column(String, nullable=False)
    dataset_id = Column(
        Integer,
        ForeignKey("pysus.datasets.id"),
        nullable=False,
        index=True,
    )
    long_name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    dataset = relationship("CatalogDataset", back_populates="groups")
    files = relationship(
        "CatalogFile",
        back_populates="group",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_groups_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )


class CatalogFile(CatalogTable):
    __tablename__ = "files"

    id: Mapped[int] = mapped_column(
        Integer,
        Sequence("files_id_seq", schema="pysus"),
        primary_key=True,
    )
    dataset_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("pysus.datasets.id"), nullable=False, index=True
    )
    group_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pysus.dataset_groups.id"),
        nullable=True,
        index=True,
    )

    path: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    rows: Mapped[int] = mapped_column(Integer, nullable=False)
    modified: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    origin_modified: Mapped[datetime | None] = mapped_column(
        DateTime,
        nullable=True,
    )
    origin_path: Mapped[str] = mapped_column(String, nullable=False)
    sha256: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
        index=True,
    )

    year: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        index=True,
    )
    month: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
        index=True,
    )
    state: Mapped[str | None] = mapped_column(
        String(2),
        nullable=True,
        index=True,
    )

    dataset: Mapped["CatalogDataset"] = relationship(
        "CatalogDataset",
        back_populates="files",
    )
    group: Mapped[Optional["DatasetGroup"]] = relationship(
        "DatasetGroup",
        back_populates="files",
    )
    columns: Mapped[list["ColumnDefinition"]] = relationship(
        "ColumnDefinition", secondary=file_columns, back_populates="files"
    )

    __table_args__ = (
        Index("ix_files_dataset_group", "dataset_id", "group_id"),
        Index("ix_files_temporal", "year", "month"),
        Index(
            "ix_files_lookup",
            "dataset_id",
            "group_id",
            "year",
            "month",
            "state",
        ),
        {"schema": "pysus"},
    )
