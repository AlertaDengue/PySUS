import struct
from datetime import date
from pathlib import Path

import pandas as pd
import pytest
from pysus.data.dbf_reader import (
    _parse_header,
    read_dbf_fast,
    read_dbf_filtered,
    read_dbf_schema,
    stream_dbf_fast,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _create_dbf(path: Path, fields, records):
    today = date.today()
    num_records = len(records)
    field_desc_len = 32 * len(fields)
    header_len = 32 + field_desc_len + 1
    record_len = 1 + sum(f[2] for f in fields)

    buf = bytearray()
    buf.append(0x03)
    buf.append(today.year - 1900)
    buf.append(today.month)
    buf.append(today.day)
    buf.extend(struct.pack("<I", num_records))
    buf.extend(struct.pack("<H", header_len))
    buf.extend(struct.pack("<H", record_len))
    buf.extend(b"\x00" * 20)

    displacement = 1
    for name, ftype, length, decimal in fields:
        buf.extend(name.encode("ascii").ljust(11, b"\x00")[:11])
        buf.append(ord(ftype))
        buf.extend(struct.pack("<I", displacement))
        displacement += length
        buf.append(length)
        buf.append(decimal)
        buf.extend(b"\x00" * 14)

    buf.append(0x0D)

    for record in records:
        buf.append(0x20)
        for i, (_, _, length, _) in enumerate(fields):
            val = record[i] if i < len(record) else ""
            if val is None:
                val = ""
            s = str(val)[:length].ljust(length)
            buf.extend(s.encode("ascii"))

    path.write_bytes(bytes(buf))


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.fixture
def simple_dbf(tmp_dir):
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0), ("AGE", "N", 3, 0), ("CITY", "C", 15, 0)],
        [
            ("Alice", "30", "Sao Paulo"),
            ("Bob", "25", "Rio"),
            ("Carol", "40", "Brasilia"),
        ],
    )
    return dbf_path


@pytest.fixture
def wide_dbf(tmp_dir):
    dbf_path = tmp_dir / "wide.dbf"
    fields = [(f"COL{i}", "C", 8, 0) for i in range(20)]
    records = [tuple(f"val{i}_{j}" for j in range(20)) for i in range(100)]
    _create_dbf(dbf_path, fields, records)
    return dbf_path


@pytest.fixture
def empty_dbf(tmp_dir):
    dbf_path = tmp_dir / "empty.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [])
    return dbf_path


@pytest.fixture
def disease_dbf(tmp_dir):
    dbf_path = tmp_dir / "disease.dbf"
    _create_dbf(
        dbf_path,
        [("CID", "C", 4, 0), ("UF", "C", 2, 0), ("YEAR", "C", 4, 0)],
        [
            ("G400", "SP", "2023"),
            ("G401", "RJ", "2023"),
            ("E104", "MG", "2023"),
            ("G400", "SP", "2022"),
            ("A901", "BA", "2023"),
            ("E109", "PR", "2023"),
            ("G400", "RS", "2023"),
        ],
    )
    return dbf_path


# ---------------------------------------------------------------------------
# read_dbf_schema
# ---------------------------------------------------------------------------


def test_read_schema(simple_dbf):
    schema = read_dbf_schema(simple_dbf)
    assert schema.num_records == 3
    assert len(schema.fields) == 3
    assert schema.field_names == ["NAME", "AGE", "CITY"]
    assert schema.fields[0].length == 10
    assert schema.fields[1].length == 3


def test_read_schema_empty(empty_dbf):
    schema = read_dbf_schema(empty_dbf)
    assert schema.num_records == 0
    assert len(schema.fields) == 1


def test_parse_header_invalid_file(tmp_dir):
    invalid = tmp_dir / "not_dbf.bin"
    invalid.write_bytes(b"not a dbf file at all")
    with pytest.raises(ValueError, match="Not a valid dBASE"):
        _parse_header(str(invalid))


def test_dbfschema_build_dtype(simple_dbf):
    schema = read_dbf_schema(simple_dbf)
    dtype = schema.build_dtype()
    names = list(dtype.names)
    assert "_deleted" in names
    assert "NAME" in names
    assert "AGE" in names


# ---------------------------------------------------------------------------
# read_dbf_fast
# ---------------------------------------------------------------------------


def test_read_fast_basic(simple_dbf):
    df = read_dbf_fast(simple_dbf)
    assert len(df) == 3
    assert list(df.columns) == ["NAME", "AGE", "CITY"]
    assert df["NAME"].iloc[0] == "Alice"
    assert df["CITY"].iloc[1] == "Rio"


def test_read_fast_column_subset(simple_dbf):
    df = read_dbf_fast(simple_dbf, columns=["NAME", "CITY"])
    assert len(df) == 3
    assert list(df.columns) == ["NAME", "CITY"]


def test_read_fast_empty(empty_dbf):
    df = read_dbf_fast(empty_dbf)
    assert len(df) == 0
    assert list(df.columns) == ["NAME"]


def test_read_fast_encoding(tmp_dir):
    dbf_path = tmp_dir / "accent.dbf"
    _create_dbf(
        dbf_path,
        [("TEXT", "C", 10, 0)],
        [("hello",)],
    )
    df = read_dbf_fast(dbf_path, encoding="cp1252")
    assert len(df) == 1
    assert df["TEXT"].iloc[0] == "hello"


def test_read_fast_wide(wide_dbf):
    df = read_dbf_fast(wide_dbf)
    assert len(df) == 100
    assert len(df.columns) == 20


def test_read_fast_strips_nul_bytes(tmp_dir):
    dbf_path = tmp_dir / "nuls.dbf"
    today = date.today()
    buf = bytearray()
    buf.append(0x03)
    buf.append(today.year - 1900)
    buf.append(today.month)
    buf.append(today.day)
    buf.extend(struct.pack("<I", 1))
    buf.extend(struct.pack("<H", 65))
    record_len_val = 1 + 8
    buf.extend(struct.pack("<H", record_len_val))
    buf.extend(b"\x00" * 20)
    buf.extend(b"VAL".ljust(11, b"\x00"))
    buf.append(ord("C"))
    buf.extend(struct.pack("<I", 1))
    buf.append(8)
    buf.append(0)
    buf.extend(b"\x00" * 14)
    buf.append(0x0D)
    buf.append(0x20)
    buf.extend(b"ab\x00cd   ")
    dbf_path.write_bytes(bytes(buf))

    df = read_dbf_fast(dbf_path)
    assert df["VAL"].iloc[0] == "abcd"


# ---------------------------------------------------------------------------
# read_dbf_filtered
# ---------------------------------------------------------------------------


def test_read_filtered_exact_match(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf, column="CID", values=["E104"], prefix_match=False
    )
    assert len(df) == 1
    assert df["CID"].iloc[0] == "E104"


def test_read_filtered_prefix_match(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf, column="CID", values=["G40"], prefix_match=True
    )
    assert len(df) == 4
    assert all(c.startswith("G40") for c in df["CID"])


def test_read_filtered_prefix_distinguishes_exact(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf, column="CID", values=["E10"], prefix_match=True
    )
    assert len(df) == 2
    cids = set(df["CID"])
    assert "E104" in cids
    assert "E109" in cids


def test_read_filtered_multiple_values(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf, column="UF", values=["SP", "RJ"], prefix_match=False
    )
    assert len(df) == 3
    ufs = set(df["UF"])
    assert "SP" in ufs
    assert "RJ" in ufs


def test_read_filtered_no_match(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf, column="CID", values=["Z99"], prefix_match=False
    )
    assert len(df) == 0
    assert "CID" in df.columns


def test_read_filtered_column_subset(disease_dbf):
    df = read_dbf_filtered(
        disease_dbf,
        column="CID",
        values=["G40"],
        prefix_match=True,
        columns=["CID", "UF"],
    )
    assert len(df) == 4
    assert list(df.columns) == ["CID", "UF"]


def test_read_filtered_missing_column(disease_dbf):
    with pytest.raises(KeyError, match="not found"):
        read_dbf_filtered(disease_dbf, column="NONEXISTENT", values=["X"])


def test_read_filtered_empty_file(empty_dbf):
    df = read_dbf_filtered(empty_dbf, column="NAME", values=["X"])
    assert len(df) == 0


# ---------------------------------------------------------------------------
# stream_dbf_fast
# ---------------------------------------------------------------------------


def test_stream_fast_basic(simple_dbf):
    chunks = list(stream_dbf_fast(simple_dbf, chunk_size=2))
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1
    all_rows = pd.concat(chunks, ignore_index=True)
    assert len(all_rows) == 3


def test_stream_fast_chunk_size_equal_records(simple_dbf):
    chunks = list(stream_dbf_fast(simple_dbf, chunk_size=3))
    assert len(chunks) == 1
    assert len(chunks[0]) == 3


def test_stream_fast_chunk_size_larger_than_records(simple_dbf):
    chunks = list(stream_dbf_fast(simple_dbf, chunk_size=100))
    assert len(chunks) == 1
    assert len(chunks[0]) == 3


def test_stream_fast_empty(empty_dbf):
    chunks = list(stream_dbf_fast(empty_dbf))
    assert len(chunks) == 0


def test_stream_fast_encoding(tmp_dir):
    dbf_path = tmp_dir / "stream_enc.dbf"
    _create_dbf(
        dbf_path,
        [("VAL", "C", 5, 0)],
        [(f"r{i}",) for i in range(250)],
    )
    chunks = list(stream_dbf_fast(dbf_path, chunk_size=100, encoding="cp1252"))
    assert len(chunks) == 3
    assert len(chunks[0]) == 100
    assert len(chunks[1]) == 100
    assert len(chunks[2]) == 50


def test_stream_fast_wide(wide_dbf):
    chunks = list(stream_dbf_fast(wide_dbf, chunk_size=30))
    assert len(chunks) == 4
    total = sum(len(c) for c in chunks)
    assert total == 100


def test_stream_fast_output_matches_read_fast(simple_dbf):
    df_full = read_dbf_fast(simple_dbf)
    chunks = list(stream_dbf_fast(simple_dbf, chunk_size=2))
    df_streamed = pd.concat(chunks, ignore_index=True)
    pd.testing.assert_frame_equal(df_full, df_streamed)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_large_record_count(tmp_dir):
    fields = [("A", "C", 4, 0)]
    records = [(f"{i:04d}",) for i in range(2000)]
    dbf_path = tmp_dir / "large.dbf"
    _create_dbf(dbf_path, fields, records)

    df = read_dbf_fast(dbf_path)
    assert len(df) == 2000
    assert df["A"].iloc[0] == "0000"
    assert df["A"].iloc[1999] == "1999"


def test_field_name_lowercase_match():
    from pysus.data.dbf_reader import DBFField, DBFSchema, _find_field

    schema = DBFSchema(
        num_records=1,
        header_len=65,
        record_len=10,
        fields=[DBFField("MIXED", "C", 10, 1)],
    )
    f = _find_field(schema, "mixed")
    assert f.name == "MIXED"
    f2 = _find_field(schema, "MIXED")
    assert f2.name == "MIXED"


def test_numpy_object_array_types(simple_dbf):
    df = read_dbf_fast(simple_dbf)
    assert df["NAME"].dtype == object
    assert isinstance(df["NAME"].iloc[0], str)
