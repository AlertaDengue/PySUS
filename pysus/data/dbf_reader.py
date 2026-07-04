"""Byte-level DBF reader for DATASUS files.

Provides a fast path that reads DBF records at the byte level using
numpy's frombuffer with structured dtypes, avoiding the per-row
Python-object overhead of dbfread.

Suitable for large DATASUS files (SIA-PA, PNI, etc.) where dbfread's
row-by-row materialisation is the bottleneck.
"""

import struct
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd


@dataclass
class DBFField:
    name: str
    type_: str
    length: int
    offset: int  # byte offset within record (includes delete-flag byte)


@dataclass
class DBFSchema:
    num_records: int
    header_len: int
    record_len: int
    fields: list[DBFField] = field(default_factory=list)

    @property
    def field_names(self) -> list[str]:
        return [f.name for f in self.fields]

    def build_dtype(self) -> np.dtype:
        spec = [("_deleted", "S1")]
        for f in self.fields:
            spec.append((f.name, f"S{f.length}"))
        return np.dtype(spec)


def _parse_header(path: str | Path) -> DBFSchema:
    """Parse a DBF header and return a ``DBFSchema``."""
    with open(path, "rb") as fh:
        header = fh.read(32)
        if len(header) < 32 or header[0] not in (
            0x02,
            0x03,
            0x30,
            0x31,
            0x43,
            0x63,
            0x83,
            0x8B,
            0x8C,
            0xF5,
        ):
            raise ValueError(f"Not a valid dBASE file: {path}")

        num_records = struct.unpack("<I", header[4:8])[0]
        header_len = struct.unpack("<H", header[8:10])[0]
        record_len = struct.unpack("<H", header[10:12])[0]
        field_bytes = fh.read(header_len - 32)

    fields = []
    offset = 1
    pos = 0
    while pos < len(field_bytes) - 1:
        if field_bytes[pos] == 0x0D:
            break
        raw = field_bytes[pos : pos + 32]
        name = (
            raw[0:11]
            .split(b"\x00")[0]
            .decode("ascii", errors="replace")
            .strip()
        )
        type_ = chr(raw[11])
        length = raw[16]
        fields.append(DBFField(name, type_, length, offset))
        offset += length
        pos += 32

    return DBFSchema(
        num_records=num_records,
        header_len=header_len,
        record_len=record_len,
        fields=fields,
    )


_ENCODING = "latin-1"


def _decode(val: bytes) -> str:
    return val.decode(_ENCODING, errors="replace").replace("\x00", "").strip()


def read_dbf_schema(path: str | Path) -> DBFSchema:
    """Return the schema of a DBF file without reading records."""
    return _parse_header(path)


def read_dbf_fast(
    path: str | Path,
    columns: list[str] | None = None,
    encoding: str = "latin-1",
) -> pd.DataFrame:
    """Read an entire DBF file into a DataFrame using vectorised byte access.

    Parameters
    ----------
    path : str or Path
        Path to the DBF file.
    columns : list[str], optional
        Subset of columns to read.  If *None* all columns are returned.
    encoding : str
        Text encoding for character fields (default ``latin-1``, the encoding
        DATASUS uses).

    Returns
    -------
    pd.DataFrame
    """
    global _ENCODING
    _ENCODING = encoding

    path = Path(path)
    schema = _parse_header(path)
    n = schema.num_records
    if n == 0:
        return pd.DataFrame(columns=schema.field_names)

    cols_lower = None if columns is None else {c.lower() for c in columns}
    target = [
        f
        for f in schema.fields
        if cols_lower is None or f.name.lower() in cols_lower
    ]
    dtype = schema.build_dtype()

    with open(path, "rb") as fh:
        fh.seek(schema.header_len)
        raw = fh.read(n * schema.record_len)

    records: np.ndarray = np.frombuffer(raw, dtype=dtype, count=n)
    records = records[
        records["_deleted"] != b"*"
    ]  # skip deleted rows (matches dbfread)
    n = len(records)
    if n == 0:
        return pd.DataFrame(columns=[f.name for f in target])

    data = {}
    for fld in target:
        col: np.ndarray = records[fld.name]
        decoded = np.empty(n, dtype=object)
        for i in range(n):
            val = col[i]
            b = val if isinstance(val, bytes) else val.tobytes()
            decoded[i] = _decode(b)
        data[fld.name] = decoded

    return pd.DataFrame(data)


def read_dbf_filtered(
    path: str | Path,
    column: str,
    values: list[str],
    columns: list[str] | None = None,
    encoding: str = "latin-1",
    prefix_match: bool = True,
) -> pd.DataFrame:
    """Read only DBF records where *column* matches one of *values*.

    The scan is performed at the byte level -- unmatched rows are never
    decoded or materialised.

    Parameters
    ----------
    path : str or Path
    column : str
        Column name to filter on.
    values : list[str]
        Target values.  For ``prefix_match=True`` a 3-char value will
        match any longer value that starts with it.
    columns : list[str], optional
        Columns to return.  If *None*, all columns are returned.
    encoding : str
    prefix_match : bool
        If ``True``, a value of length < field length acts as a prefix.

    Returns
    -------
    pd.DataFrame
    """
    global _ENCODING
    _ENCODING = encoding

    path = Path(path)
    schema = _parse_header(path)
    n = schema.num_records
    if n == 0:
        cols = columns or schema.field_names
        return pd.DataFrame(columns=cols)

    filter_field = _find_field(schema, column)

    # Bare (unpadded) targets. The prefix-vs-exact decision is made per value
    # in _scan_column by comparing the value length to the field width, so a
    # value shorter than the field must NOT be pre-padded: pre-padding broke
    # exact match (the scanned chunk is stripped of trailing padding first).
    target_bytes: list[bytes] = [val.encode(encoding) for val in values]

    matches = _scan_column(
        path, schema, filter_field, target_bytes, prefix_match
    )

    if not matches:
        cols = columns or schema.field_names
        return pd.DataFrame(columns=cols)

    return _materialize_rows(path, schema, matches, columns)


def stream_dbf_fast(
    path: str | Path,
    chunk_size: int = 100_000,
    encoding: str = "latin-1",
) -> Iterator[pd.DataFrame]:
    """Stream records from a DBF file in chunks using vectorised byte access.

    Parameters
    ----------
    path : str or Path
    chunk_size : int
        Number of rows per chunk.
    encoding : str

    Yields
    ------
    pd.DataFrame
    """
    global _ENCODING
    _ENCODING = encoding

    path = Path(path)
    schema = _parse_header(path)
    n = schema.num_records
    rl = schema.record_len
    dtype = schema.build_dtype()

    with open(path, "rb") as fh:
        fh.seek(schema.header_len)
        raw = fh.read(n * rl)

    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        chunk_n = end - start
        chunk_raw = raw[start * rl : end * rl]

        records: np.ndarray = np.frombuffer(
            chunk_raw, dtype=dtype, count=chunk_n
        )
        records = records[records["_deleted"] != b"*"]  # skip deleted rows
        chunk_n = len(records)

        data = {}
        for fld in schema.fields:
            col: np.ndarray = records[fld.name]
            decoded = np.empty(chunk_n, dtype=object)
            for i in range(chunk_n):
                val = col[i]
                b = val if isinstance(val, bytes) else val.tobytes()
                decoded[i] = _decode(b)
            data[fld.name] = decoded

        yield pd.DataFrame(data)


def _find_field(schema: DBFSchema, name: str) -> DBFField:
    name_lower = name.lower()
    for f in schema.fields:
        if f.name.lower() == name_lower:
            return f
    raise KeyError(
        f"Column '{name}' not found. Available: {schema.field_names}"
    )


def _scan_column(
    path: Path,
    schema: DBFSchema,
    field: DBFField,
    target_bytes: list[bytes],
    prefix_match: bool,
) -> list[int]:
    """Scan *field* for any of *target_bytes*; return matching row indices."""
    matches: list[int] = []
    field_width = field.length
    field_offset_in_record = field.offset

    with open(path, "rb") as fh:
        for row_idx in range(schema.num_records):
            record_start = schema.header_len + row_idx * schema.record_len
            fh.seek(record_start)
            delete_flag = fh.read(1)
            if delete_flag in (b"\x00", b"*"):
                continue

            fh.seek(record_start + field_offset_in_record)
            chunk = fh.read(field_width)

            stripped = chunk.rstrip(
                b"\x00 "
            )  # DATASUS pads with spaces and/or NULs
            for target in target_bytes:
                if prefix_match and len(target) < field_width:
                    if stripped.startswith(target):
                        matches.append(row_idx)
                        break
                else:
                    if stripped == target:
                        matches.append(row_idx)
                        break

    return matches


def _materialize_rows(
    path: Path,
    schema: DBFSchema,
    row_indices: list[int],
    columns: list[str] | None,
) -> pd.DataFrame:
    """Read only the specified rows from the DBF and return a DataFrame."""
    cols_lower = None if columns is None else {c.lower() for c in columns}
    target_fields = [
        f
        for f in schema.fields
        if cols_lower is None or f.name.lower() in cols_lower
    ]

    data: dict[str, list[str]] = {f.name: [] for f in target_fields}

    with open(path, "rb") as fh:
        for row_idx in row_indices:
            offset = schema.header_len + row_idx * schema.record_len
            fh.seek(offset)
            record = fh.read(schema.record_len)
            if len(record) < schema.record_len:
                continue
            for fld in target_fields:
                raw = record[fld.offset : fld.offset + fld.length]
                data[fld.name].append(_decode(raw))

    return pd.DataFrame(data)
