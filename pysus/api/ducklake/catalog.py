import enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

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
    __table_args__ = {"schema": "pysus"}


class Origin(enum.Enum):
    FTP = "ftp"
    API = "api"


class CatalogDataset(CatalogTable):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
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

    id = Column(Integer, primary_key=True)
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

    id = Column(Integer, primary_key=True)
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

    id = Column(Integer, primary_key=True)
    dataset_id = Column(
        Integer, ForeignKey("pysus.datasets.id"), nullable=False, index=True
    )
    group_id = Column(
        Integer,
        ForeignKey("pysus.dataset_groups.id"),
        nullable=True,
        index=True,
    )
    path = Column(String, nullable=False, unique=True)
    size = Column(Integer, nullable=False)
    rows = Column(Integer, nullable=False)
    modified = Column(DateTime, nullable=False)
    sha256 = Column(String(64), nullable=True, index=True)
    year = Column(Integer, nullable=True, index=True)
    month = Column(Integer, nullable=True, index=True)
    state = Column(String(2), nullable=True, index=True)

    dataset = relationship("CatalogDataset", back_populates="files")
    group = relationship("DatasetGroup", back_populates="files")
    columns = relationship(
        "ColumnDefinition", secondary=file_columns, back_populates="files"
    )

    __table_args__ = (
        Index("ix_files_dataset_group", "dataset_id", "group_id"),
        Index("ix_files_temporal", "year", "month"),
        {"schema": "pysus"},
    )
