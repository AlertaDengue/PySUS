from datetime import datetime
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Sequence,
    String,
    Table,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class DatasetBase(DeclarativeBase):
    pass


file_columns = Table(
    "file_columns",
    DatasetBase.metadata,
    Column("file_id", Integer, ForeignKey("pysus.files.id"), primary_key=True),
    Column("column_id", Integer, primary_key=True),
    schema="pysus",
)


class Group(DatasetBase):
    __tablename__ = "dataset_groups"
    __table_args__ = (
        Index("ix_groups_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )

    id: Mapped[int] = mapped_column(
        Integer, Sequence("groups_id_seq", schema="pysus"), primary_key=True
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    dataset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    long_name: Mapped[str] = mapped_column(String, nullable=False)
    description: Mapped[str | None] = mapped_column(String, nullable=True)

    files: Mapped[list["File"]] = relationship(
        "File", back_populates="group", cascade="all, delete-orphan"
    )


class File(DatasetBase):
    __tablename__ = "files"
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

    id: Mapped[int] = mapped_column(
        Integer, Sequence("files_id_seq", schema="pysus"), primary_key=True
    )
    dataset_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    group_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("pysus.dataset_groups.id"),
        nullable=True,
        index=True,
    )
    path: Mapped[str] = mapped_column(String, nullable=False, unique=True)
    size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    rows: Mapped[int] = mapped_column(Integer, nullable=False)
    type: Mapped[str] = mapped_column(String, nullable=True)
    modified: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    origin_modified: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    origin_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    origin_path: Mapped[str] = mapped_column(String, nullable=False)
    sha256: Mapped[str | None] = mapped_column(
        String(64), nullable=True, index=True
    )
    year: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    month: Mapped[int | None] = mapped_column(
        Integer, nullable=True, index=True
    )
    state: Mapped[str | None] = mapped_column(
        String(2), nullable=True, index=True
    )

    group: Mapped[Optional["Group"]] = relationship(
        "Group", back_populates="files"
    )
