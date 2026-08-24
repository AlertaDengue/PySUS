"""Data transformation utilities for PySUS.

Provides unit detection, cross-dataset linking, aggregation, streaming,
column aliases, data masking, and numeric precision control.

Usage::

    from pysus.api.transform import (
        detect_units, link_datasets, aggregate_by_state,
        stream_parquet, rename_columns, mask_data, set_precision
    )
"""

from pysus.api.transform.aggregation import (
    aggregate_by_age_group,
    aggregate_by_period,
    aggregate_by_state,
)
from pysus.api.transform.aliases import get_aliases, rename_columns
from pysus.api.transform.linking import get_linking_keys, link_datasets
from pysus.api.transform.masking import mask_data, unmask_data
from pysus.api.transform.precision import optimize_memory, set_precision
from pysus.api.transform.streaming import stream_parquet
from pysus.api.transform.units import DetectedUnit, detect_units

__all__ = [
    "DetectedUnit",
    "aggregate_by_age_group",
    "aggregate_by_period",
    "aggregate_by_state",
    "detect_units",
    "get_aliases",
    "get_linking_keys",
    "link_datasets",
    "mask_data",
    "optimize_memory",
    "rename_columns",
    "set_precision",
    "stream_parquet",
    "unmask_data",
]
