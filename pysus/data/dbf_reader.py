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
from pysus.api.errors import DBFFormatError

_SUPPORTED_VERSIONS = frozenset(
    {0x02, 0x03, 0x30, 0x31, 0x43, 0x63, 0x83, 0x8B, 0x8C, 0xF5}
)
# Versions whose header uses the classic 32-byte field descriptors and whose
# record length is exactly the deletion flag plus the character field widths.
_STRUCT_FIELD_SUM_VERSIONS = frozenset(
    {0x02, 0x03, 0x43, 0x63, 0x83, 0x8B, 0x8C, 0xF5}
)
_DELETE_FLAGS = frozenset({b" ", b"*"})


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
    version: int = 0

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
    path = Path(path)
    file_size = path.stat().st_size
    with open(path, "rb") as fh:
        header = fh.read(32)
        if len(header) < 32 or header[0] not in _SUPPORTED_VERSIONS:
            raise DBFFormatError(f"Not a valid dBASE file: {path}")

        version = header[0]
        num_records = struct.unpack("<I", header[4:8])[0]
        header_len = struct.unpack("<H", header[8:10])[0]
        record_len = struct.unpack("<H", header[10:12])[0]

        if header_len < 33 or header_len > file_size:
            raise DBFFormatError(
                f"Invalid DBF header length {header_len} in {path} "
                f"(file size {file_size})."
            )
        if record_len < 1:
            raise DBFFormatError(
                f"Invalid DBF record length {record_len} in {path}."
            )

        field_bytes = fh.read(header_len - 32)
        if len(field_bytes) != header_len - 32:
            raise DBFFormatError(
                f"Truncated DBF header in {path}: expected "
                f"{header_len - 32} field-descriptor bytes, got "
                f"{len(field_bytes)}."
            )

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

    if version in _STRUCT_FIELD_SUM_VERSIONS:
        # DATASUS files occasionally carry one extra byte after the 0x0D
        # terminator (seen in old SIA/SIH/PNI files), so accept either the
        # standard terminator or terminator + one padding byte.
        terminator_pos = 32 * len(fields)
        extra = len(field_bytes) - terminator_pos
        if (
            terminator_pos >= len(field_bytes)
            or field_bytes[terminator_pos] != 0x0D
            or extra not in (1, 2)
        ):
            raise DBFFormatError(
                f"Malformed DBF field descriptor area in {path}."
            )

    return DBFSchema(
        num_records=num_records,
        header_len=header_len,
        record_len=record_len,
        fields=fields,
        version=version,
    )


_ENCODING = "latin-1"


def _decode(val: bytes) -> str:
    return val.decode(_ENCODING, errors="replace").replace("\x00", "").strip()


def _decode_column(column: np.ndarray) -> list[str]:
    """Decode a NumPy byte-string column without per-item function calls."""
    return [
        (value if isinstance(value, bytes) else value.tobytes())
        .decode(_ENCODING, errors="replace")
        .replace("\x00", "")
        .strip()
        for value in column
    ]


def read_dbf_schema(path: str | Path) -> DBFSchema:
    """Return the schema of a DBF file without reading records."""
    return _parse_header(path)


def _validate_record_area(path: Path, schema: DBFSchema) -> None:
    """Check that the file contains exactly the record area it declares."""
    size = path.stat().st_size
    expected = schema.header_len + schema.num_records * schema.record_len
    data_size = size

    if data_size > expected:
        with open(path, "rb") as fh:
            fh.seek(-1, 2)
            if fh.read(1) == b"\x1a":
                data_size -= 1

    if data_size < expected:
        raise DBFFormatError(
            f"Truncated DBF file {path}: expected at least {expected} bytes "
            f"for {schema.num_records} records, got {size}."
        )

    record_area = data_size - schema.header_len
    if record_area % schema.record_len != 0:
        raise DBFFormatError(
            f"Truncated or misaligned DBF record area in {path}: "
            f"{record_area} bytes is not a multiple of the "
            f"{schema.record_len}-byte record length."
        )


def _validate_delete_flags(
    path: Path, schema: DBFSchema, *, strict: bool = False
) -> None:
    """Check record deletion flags, sampling unless *strict* is requested."""
    n = schema.num_records
    if n == 0:
        return

    if strict:
        indices = list(range(n))
    else:
        indices_set = {0, n - 1}
        step = max(1, n // 32)
        indices_set.update(range(0, n, step))
        indices = sorted(indices_set)

    with open(path, "rb") as fh:
        for row_idx in indices:
            fh.seek(schema.header_len + row_idx * schema.record_len)
            flag = fh.read(1)
            if flag not in _DELETE_FLAGS:
                raise DBFFormatError(
                    f"Invalid DBF deletion flag {flag!r} at record "
                    f"{row_idx} in {path}."
                )


def validate_dbf(
    path: str | Path,
    schema: DBFSchema | None = None,
    *,
    strict: bool = False,
) -> None:
    """Raise :class:`DBFFormatError` if *path* is structurally invalid.

    Parameters
    ----------
    path : str or Path
        DBF file to validate.
    schema : DBFSchema, optional
        Already-parsed schema.  If omitted, the header is parsed here.
    strict : bool
        If ``True``, check every record's deletion flag.  By default only
        the first, last, and a sample of records are checked.
    """
    path = Path(path)
    if schema is None:
        schema = _parse_header(path)

    if schema.version in _STRUCT_FIELD_SUM_VERSIONS:
        expected_record_len = 1 + sum(f.length for f in schema.fields)
        if expected_record_len != schema.record_len:
            raise DBFFormatError(
                f"Invalid DBF record length in {path}: field lengths sum to "
                f"{expected_record_len - 1} plus the deletion flag, but the "
                f"header declares {schema.record_len}."
            )

    _validate_record_area(path, schema)
    _validate_delete_flags(path, schema, strict=strict)


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
    validate_dbf(path, schema)
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
        data[fld.name] = _decode_column(col)

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
    validate_dbf(path, schema)
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
    validate_dbf(path, schema)
    n = schema.num_records
    rl = schema.record_len
    dtype = schema.build_dtype()

    with open(path, "rb") as fh:
        for start in range(0, n, chunk_size):
            end = min(start + chunk_size, n)
            chunk_n = end - start
            fh.seek(schema.header_len + start * rl)
            chunk_raw = fh.read(chunk_n * rl)

            records: np.ndarray = np.frombuffer(
                chunk_raw, dtype=dtype, count=chunk_n
            )
            records = records[records["_deleted"] != b"*"]  # skip deleted rows
            chunk_n = len(records)

            data = {}
            for fld in schema.fields:
                col: np.ndarray = records[fld.name]
                data[fld.name] = _decode_column(col)

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
