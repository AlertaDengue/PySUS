import enum

from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Date,
    Boolean,
    Index,
    Enum,
)

Base = declarative_base()


class Catalog(Base):
    __abstract__ = True
    __table_args__ = {"schema": "pysus"}


class Dataset(Catalog):
    __tablename__ = "datasets"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True, index=True)
    metadata_id = Column(
        Integer,
        ForeignKey("pysus.dataset_metadata.id"),
        index=True,
    )

    dataset_metadata = relationship(
        "DatasetMetadata",
        back_populates="datasets",
    )

    groups = relationship(
        "DatasetGroup",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )

    columns = relationship(
        "ColumnDefinition",
        back_populates="dataset",
        cascade="all, delete-orphan",
    )


class ColumnDefinition(Catalog):
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
    position = Column(Integer, nullable=False, index=True)

    dataset = relationship("Dataset", back_populates="columns")

    __table_args__ = (
        Index("ix_columns_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )


class DatasetGroup(Catalog):
    __tablename__ = "dataset_groups"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    dataset_id = Column(
        Integer,
        ForeignKey("pysus.datasets.id"),
        nullable=False,
        index=True,
    )
    metadata_id = Column(
        Integer,
        ForeignKey("pysus.dataset_group_metadata.id"),
        index=True,
    )

    dataset = relationship(
        "Dataset",
        back_populates="groups",
    )

    group_metadata = relationship(
        "DatasetGroupMetadata",
        back_populates="groups",
    )

    files = relationship(
        "File",
        back_populates="group",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_groups_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )


class File(Catalog):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)

    group_id = Column(
        Integer,
        ForeignKey("pysus.dataset_groups.id"),
        nullable=False,
        index=True,
    )
    path = Column(String, nullable=False, unique=True)
    size = Column(Integer, nullable=False)
    rows = Column(Integer, nullable=False)

    modified = Column(Date, nullable=False)

    group = relationship(
        "DatasetGroup",
        back_populates="files",
    )


class DatasetMetadata(Catalog):
    class Origin(enum.Enum):
        FTP = "ftp"
        API = "api"

    __tablename__ = "dataset_metadata"

    id = Column(Integer, primary_key=True)
    long_name = Column(String, nullable=False)
    description = Column(String, nullable=True)
    source = Column(String, nullable=True)
    origin = Column(Enum(Origin), nullable=False)

    datasets = relationship(
        "Dataset",
        back_populates="dataset_metadata",
    )


class DatasetGroupMetadata(Catalog):
    __tablename__ = "dataset_group_metadata"

    id = Column(Integer, primary_key=True)
    long_name = Column(String, nullable=False)
    description = Column(String, nullable=True)

    groups = relationship(
        "DatasetGroup",
        back_populates="group_metadata",
    )
