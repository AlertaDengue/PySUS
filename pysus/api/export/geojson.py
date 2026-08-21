"""GeoJSON export for geographic columns.

Exports DataFrames with lat/lon columns to GeoJSON FeatureCollection.

Usage::

    from pysus.api.export.geojson import to_geojson

    to_geojson(df, "output.geojson", lat_col="LATITUDE", lon_col="LONGITUDE")
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def to_geojson(
    df: pd.DataFrame,
    path: str | Path,
    lat_col: str = "LATITUDE",
    lon_col: str = "LONGITUDE",
    geocode_col: str | None = None,
    properties: list[str] | None = None,
) -> Path:
    """Export DataFrame to GeoJSON with Point geometries.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame.
    path : str or Path
        Output file path.
    lat_col : str
        Latitude column name.
    lon_col : str
        Longitude column name.
    geocode_col : str, optional
        Geocode column for properties.
    properties : list, optional
        Additional columns to include as properties.

    Returns
    -------
    Path
        Path to created file.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)

    features = []

    for idx, row in df.iterrows():
        lat = row.get(lat_col)
        lon = row.get(lon_col)
        if pd.notna(lat) and pd.notna(lon):
            geometry = {
                "type": "Point",
                "coordinates": [
                    float(str(lon)),
                    float(str(lat)),
                ],
            }

            props: dict[str, str | None] = {}
            if geocode_col and geocode_col in row:
                props["geocode"] = str(row[geocode_col])

            if properties:
                for prop in properties:
                    if prop in row:
                        props[prop] = (
                            str(row[prop]) if pd.notna(row[prop]) else None
                        )

            features.append(
                {
                    "type": "Feature",
                    "id": (
                        int(idx) if isinstance(idx, (int, float)) else str(idx)
                    ),
                    "geometry": geometry,
                    "properties": props,
                }
            )

    geojson = {"type": "FeatureCollection", "features": features}

    path.write_text(
        json.dumps(geojson, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return path
