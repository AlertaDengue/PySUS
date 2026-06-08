from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import Column, Integer, Sequence, String, Boolean, Index


class ColumnsBase(DeclarativeBase):
    pass


class ColumnDefinition(ColumnsBase):
    __tablename__ = "dataset_columns"

    id = Column(Integer, Sequence("columns_id_seq", schema="pysus"), primary_key=True)
    dataset_id = Column(Integer, nullable=False, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    description = Column(String, nullable=True)
    nullable = Column(Boolean, nullable=False, default=True)

    __table_args__ = (
        Index("ix_columns_dataset_name", "dataset_id", "name"),
        {"schema": "pysus"},
    )
