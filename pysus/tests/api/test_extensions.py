import gzip
import json
import struct
import tarfile
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pysus.api.errors import ConversionError, FormatError
from pysus.api.extensions import (
    CSV,
    DBC,
    DBF,
    JSON,
    PDF,
    Directory,
    ExtensionFactory,
    File,
    GZip,
    Parquet,
    Tar,
    Zip,
    _map_dtype,
)
from pysus.api.models import BaseLocalFile


@pytest.fixture
def tmp_dir(tmp_path: Path):
    return tmp_path


async def collect_async(gen):
    out = []
    async for item in gen:
        out.append(item)
    return out


def _create_dbf(path, fields, records):
    """Create a minimal valid DBF file at *path*.

    Parameters
    ----------
    fields : list of (name, type, length, decimal)
    records : list of tuples
    """
    from datetime import date

    today = date.today()
    num_records = len(records)
    field_desc_len = 32 * len(fields)
    header_len = 32 + field_desc_len + 1
    record_len = 1 + sum(f[2] for f in fields)

    buf = bytearray()
    # Version (0x03 = FoxBASE)
    buf.append(0x03)
    # Last update date
    buf.append(today.year - 1900)
    buf.append(today.month)
    buf.append(today.day)
    # Number of records
    buf.extend(struct.pack("<I", num_records))
    # Header length
    buf.extend(struct.pack("<H", header_len))
    # Record length
    buf.extend(struct.pack("<H", record_len))
    # Reserved
    buf.extend(b"\x00" * 20)

    # Field descriptors
    displacement = 1
    for name, ftype, length, decimal in fields:
        buf.extend(name.encode("ascii").ljust(11, b"\x00")[:11])
        buf.append(ord(ftype))
        buf.extend(struct.pack("<I", displacement))
        displacement += length
        buf.append(length)
        buf.append(decimal)
        buf.extend(b"\x00" * 14)

    # Field terminator
    buf.append(0x0D)

    # Records
    for record in records:
        buf.append(0x20)
        for i, (_, _, length, _) in enumerate(fields):
            val = record[i] if i < len(record) else ""
            if val is None:
                val = ""
            s = str(val)[:length].ljust(length)
            buf.extend(s.encode("ascii"))

    path.write_bytes(bytes(buf))


# ---------------------------------------------------------------------------
# Existing tests (kept unchanged)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_directory_load_and_stream(tmp_dir):
    subdir = tmp_dir / "dir"
    subdir.mkdir()

    (subdir / "a.txt").write_text("a")
    (subdir / "b.csv").write_text("x\n1")

    obj = await ExtensionFactory.instantiate(subdir)
    assert isinstance(obj, Directory)
    assert obj.basename == "dir"

    loaded = await obj.load()
    assert {f.basename for f in loaded} == {"a.txt", "b.csv"}

    streamed = await collect_async(obj.stream())
    assert len(streamed) == 2
    assert all(hasattr(f, "load") for f in streamed)


@pytest.mark.asyncio
async def test_directory_empty(tmp_dir):
    subdir = tmp_dir / "empty"
    subdir.mkdir()

    obj = await ExtensionFactory.instantiate(subdir)
    loaded = await obj.load()
    assert loaded == []


@pytest.mark.asyncio
async def test_csv_load_and_stream(tmp_dir):
    path = tmp_dir / "data.csv"
    df = pd.DataFrame({"a": ["1", "2"], "b": ["3", "4"]})
    df.to_csv(path, index=False)

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, CSV)

    loaded = await obj.load()
    pd.testing.assert_frame_equal(
        loaded.astype(str),
        df.astype(str),
    )

    chunks = await collect_async(obj.stream(chunk_size=1))
    assert len(chunks) == 2
    assert all(isinstance(c, pd.DataFrame) for c in chunks)


@pytest.mark.asyncio
async def test_csv_sep_and_encoding_fallback(tmp_dir):
    path = tmp_dir / "data.csv"
    path.write_text("a;b\n1;2\n")

    obj = await ExtensionFactory.instantiate(path)
    df = await obj.load()

    assert list(df.columns) == ["a", "b"]
    assert df.iloc[0]["a"] == 1


@pytest.mark.asyncio
async def test_parquet_parse_and_stream(tmp_dir):
    csv_path = tmp_dir / "data.csv"

    df = pd.DataFrame(
        {
            "DT_NOTIFIC": ["20230101"],
            "CODMUNRES": [" 123 "],
            "OTHER": ["   "],
        }
    )
    df.to_csv(csv_path, index=False)

    csv_obj = await ExtensionFactory.instantiate(csv_path)
    pq_obj = await csv_obj.to_parquet()

    assert isinstance(pq_obj, Parquet)

    parsed = await pq_obj.load(parse=True)
    assert str(parsed["DT_NOTIFIC"].iloc[0]) == "2023-01-01"
    assert parsed["CODMUNRES"].iloc[0] == 123
    assert parsed["OTHER"].iloc[0] == ""

    chunks = await collect_async(pq_obj.stream())
    assert len(chunks) >= 1


@pytest.mark.asyncio
async def test_parquet_load_applies_add_dv_to_geocode_columns(tmp_dir):
    df = pd.DataFrame(
        {
            "ID_MUNICIP": ["261160", "530010"],
            "DT_NOTIFIC": ["20230101", "20230102"],
        }
    )
    path = tmp_dir / "test.parquet"
    df.to_parquet(path)

    pq_obj = Parquet(path=path, add_dv=True)
    parsed = await pq_obj.load(parse=True)

    assert parsed["ID_MUNICIP"].iloc[0] == "2611606"
    assert parsed["ID_MUNICIP"].iloc[1] == "5300108"
    assert str(parsed["DT_NOTIFIC"].iloc[0]) == "2023-01-01"


@pytest.mark.asyncio
async def test_parquet_load_skips_add_dv_when_disabled(tmp_dir):
    df = pd.DataFrame({"ID_MUNICIP": ["261160"]})
    path = tmp_dir / "test.parquet"
    df.to_parquet(path)

    pq_obj = Parquet(path=path, add_dv=False)
    parsed = await pq_obj.load(parse=True)

    assert parsed["ID_MUNICIP"].iloc[0] == "261160"


@pytest.mark.asyncio
async def test_dbf_decode_and_failure(tmp_dir):
    pytest.importorskip("dbfread")

    path = tmp_dir / "test.dbf"
    path.write_bytes(b"invalid")

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, DBF)

    assert obj.decode_column(b"COL\x00") == "COL"
    assert obj.decode_column("COL\x00") == "COL"

    with pytest.raises(Exception):  # noqa
        await obj.load()


@pytest.mark.asyncio
async def test_dbc_import_behavior(tmp_dir):
    import gc

    path = tmp_dir / "file.dbc"
    path.write_bytes(b"dummy")

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, DBC)

    with pytest.raises(struct.error):
        try:
            await obj.to_parquet(tmp_dir / "out.parquet")
        except struct.error:
            gc.collect()
            raise


@pytest.mark.asyncio
async def test_json_load_and_stream(tmp_dir):
    path = tmp_dir / "data.json"
    data = [{"a": 1}, {"a": 2}]
    path.write_text(json.dumps(data))

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, JSON)

    df = await obj.load()
    assert df.shape == (2, 1)

    streamed = await collect_async(obj.stream())
    assert len(streamed) == 1
    assert streamed[0].equals(df)


@pytest.mark.asyncio
async def test_pdf_load_and_stream(tmp_dir):
    path = tmp_dir / "file.pdf"
    content = b"%PDF-1.4\n..."
    path.write_bytes(content)

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, PDF)

    assert await obj.load() == content

    chunks = await collect_async(obj.stream(chunk_size=4))
    assert b"".join(chunks) == content


@pytest.mark.asyncio
async def test_file_load_and_stream(tmp_dir):
    path = tmp_dir / "file.bin"
    content = b"abc123"
    path.write_bytes(content)

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, File)

    assert await obj.load() == content

    chunks = await collect_async(obj.stream(chunk_size=2))
    assert b"".join(chunks) == content


@pytest.mark.asyncio
async def test_zip_full_flow(tmp_dir):
    zip_path = tmp_dir / "file.zip"
    inner = tmp_dir / "inner.csv"
    pd.DataFrame({"x": [1]}).to_csv(inner, index=False)

    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(inner, arcname="inner.csv")

    obj = await ExtensionFactory.instantiate(zip_path)
    assert isinstance(obj, Zip)

    members = await obj.list_members()
    assert "inner.csv" in members

    content = await obj.open_member("inner.csv")
    assert b"x" in content

    extracted = await obj.extract(tmp_dir / "out")
    assert any(isinstance(f, CSV) for f in extracted)


@pytest.mark.asyncio
async def test_gzip_full_flow(tmp_dir):
    path = tmp_dir / "data.csv.gz"
    raw = b"a,b\n1,2"

    with gzip.open(path, "wb") as f:
        f.write(raw)

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, GZip)

    assert await obj.load() == raw
    assert await obj.list_members() == ["data.csv"]

    extracted = await obj.extract(tmp_dir / "out")
    assert len(extracted) == 1
    assert isinstance(extracted[0], CSV)


@pytest.mark.asyncio
async def test_tar_full_flow(tmp_dir):
    tar_path = tmp_dir / "file.tar"
    f = tmp_dir / "a.txt"
    f.write_text("hello")

    with tarfile.open(tar_path, "w") as t:
        t.add(f, arcname="a.txt")

    obj = await ExtensionFactory.instantiate(tar_path)
    assert isinstance(obj, Tar)

    members = await obj.list_members()
    assert "a.txt" in members

    content = await obj.open_member("a.txt")
    assert content == b"hello"

    extracted = await obj.extract(tmp_dir / "out")
    assert any(isinstance(x, File) for x in extracted)


# ---------------------------------------------------------------------------
# New tests for lines 56-60: _map_dtype
# ---------------------------------------------------------------------------


def test_map_dtype():
    assert _map_dtype("int8") == "INTEGER"
    assert _map_dtype("int64") == "BIGINT"
    assert _map_dtype("unknown_dtype") == "VARCHAR"
    assert _map_dtype("object") == "VARCHAR"


# ---------------------------------------------------------------------------
# New tests for line 104: Directory.__repr__
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_directory_repr(tmp_dir):
    subdir = tmp_dir / "mydir"
    subdir.mkdir()
    obj = await ExtensionFactory.instantiate(subdir)
    assert repr(obj) == "mydir/"


# ---------------------------------------------------------------------------
# New tests for line 111: Directory.load when path doesn't exist
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_directory_load_nonexistent(tmp_dir):
    obj = Directory(path=tmp_dir / "nonexistent")
    loaded = await obj.load()
    assert loaded == []


# ---------------------------------------------------------------------------
# New tests for lines 139-152: CSV.columns encoding paths
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_csv_columns_with_preset_encoding(tmp_dir):
    path = tmp_dir / "data.csv"
    path.write_text("a,b\n1,2\n")
    obj = CSV(path=path)
    obj._encoding = "utf-8"
    cols = obj.columns
    assert len(cols) == 2


@pytest.mark.asyncio
async def test_csv_columns_fallback_encoding(tmp_dir):
    path = tmp_dir / "data.csv"
    path.write_text("a,b\n1,2\n")
    obj = CSV(path=path)
    cols = obj.columns
    assert len(cols) == 2
    assert obj._encoding is not None


# ---------------------------------------------------------------------------
# New tests for lines 194-195: CSV._get_sep sniffer ValueError fallback
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_csv_get_sep_sniffer_valueerror(tmp_dir):
    path = tmp_dir / "data.csv"
    path.write_text("a,b\n1,2\n")
    obj = CSV(path=path)

    with patch("csv.Sniffer.sniff", side_effect=ValueError("mock")):
        sep = await obj._get_sep()
        assert sep == ","


# ---------------------------------------------------------------------------
# New tests for lines 247, 253-254, 264: Parquet properties
# ---------------------------------------------------------------------------


def test_parquet_schema(tmp_dir):
    path = tmp_dir / "test.parquet"
    pd.DataFrame({"a": [1]}).to_parquet(path)
    obj = Parquet(path=path)
    schema = obj.schema
    assert isinstance(schema, pa.Schema)


def test_parquet_columns(tmp_dir):
    path = tmp_dir / "test.parquet"
    pd.DataFrame({"a": [1], "b": ["x"]}).to_parquet(path)
    obj = Parquet(path=path)
    cols = obj.columns
    assert len(cols) == 2


def test_parquet_rows(tmp_dir):
    path = tmp_dir / "test.parquet"
    pd.DataFrame({"a": [1, 2, 3]}).to_parquet(path)
    obj = Parquet(path=path)
    assert obj.rows == 3


# ---------------------------------------------------------------------------
# New tests for line 297: Parquet.stream when num_row_groups == 0
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parquet_stream_empty_row_groups(tmp_dir):
    path = tmp_dir / "empty.parquet"
    schema = pa.schema([pa.field("a", pa.int64())])
    with pq.ParquetWriter(str(path), schema):
        pass
    obj = Parquet(path=path)
    chunks = await collect_async(obj.stream())
    assert chunks == []


# ---------------------------------------------------------------------------
# New tests for lines 302-304: Parquet.stream with parse=True and add_dv
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_parquet_stream_parse_and_add_dv(tmp_dir):
    df = pd.DataFrame(
        {
            "ID_MUNICIP": ["261160"],
            "DT_NOTIFIC": ["20230101"],
        }
    )
    path = tmp_dir / "test.parquet"
    df.to_parquet(path)

    obj = Parquet(path=path, add_dv=True)
    chunks = await collect_async(obj.stream(parse=True))
    assert len(chunks) >= 1
    assert chunks[0]["ID_MUNICIP"].iloc[0] == "2611606"
    assert str(chunks[0]["DT_NOTIFIC"].iloc[0]) == "2023-01-01"


# ---------------------------------------------------------------------------
# New tests for lines 315, 324-326: parse_dftypes edge cases
# ---------------------------------------------------------------------------


def test_parse_dftypes_edge_cases():
    df = pd.DataFrame(
        {
            "DT_NOTIFIC": [123, "not_a_date", "20230101"],
            "CODMUNRES": [float("nan"), None, " 330455 "],
            "IDADE": [None, float("nan"), "25"],
        }
    )
    result = Parquet.parse_dftypes(df)

    assert result["DT_NOTIFIC"].iloc[0] == 123
    assert result["DT_NOTIFIC"].iloc[1] == "not_a_date"
    assert str(result["DT_NOTIFIC"].iloc[2]) == "2023-01-01"

    assert pd.isna(result["CODMUNRES"].iloc[0])
    assert pd.isna(result["CODMUNRES"].iloc[1])
    assert result["CODMUNRES"].iloc[2] == 330455

    assert pd.isna(result["IDADE"].iloc[0])
    assert pd.isna(result["IDADE"].iloc[1])
    assert result["IDADE"].iloc[2] == 25


# ---------------------------------------------------------------------------
# New tests for lines 351-360, 370, 394, 402-403, 413-427: DBF
# ---------------------------------------------------------------------------


def test_dbf_columns(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 20, 0), ("AGE", "N", 3, 0), ("SALARY", "F", 10, 2)],
        [("Alice", 30, 5000.00)],
    )
    obj = DBF(path=dbf_path)
    cols = obj.columns
    assert len(cols) == 3
    assert cols[0].dtype == "VARCHAR"
    assert cols[1].dtype == "INTEGER"
    assert cols[2].dtype == "FLOAT"


def test_dbf_rows(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0)],
        [("Alice",), ("Bob",), ("Charlie",)],
    )
    obj = DBF(path=dbf_path)
    assert obj.rows == 3


def test_dbf_decode_column_non_string():
    obj = DBF(path=Path("/dummy"))
    assert obj.decode_column(123) == 123
    assert obj.decode_column(45.6) == 45.6
    assert obj.decode_column(None) is None


@pytest.mark.asyncio
async def test_dbf_load(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0), ("AGE", "N", 3, 0)],
        [("Alice", 30), ("Bob", 25)],
    )
    obj = DBF(path=dbf_path)
    df = await obj.load()
    assert len(df) == 2
    assert list(df.columns) == ["NAME", "AGE"]
    assert df["NAME"].iloc[0] == "Alice"


@pytest.mark.asyncio
async def test_dbf_stream(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("VAL", "N", 5, 0)],
        [(10,), (20,), (30,), (40,), (50,)],
    )
    obj = DBF(path=dbf_path)
    chunks = await collect_async(obj.stream(chunk_size=2))
    assert len(chunks) >= 2
    assert all(isinstance(c, pd.DataFrame) for c in chunks)
    assert len(chunks[0]) == 2


# ---------------------------------------------------------------------------
# New tests for lines 445-447, 452-490, 493-496: DBF.to_parquet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbf_to_parquet(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0)],
        [("Alice",), ("Bob",), ("Charlie",)],
    )
    obj = DBF(path=dbf_path)

    calls = []

    def cb(current, total):
        calls.append((current, total))

    result = await obj.to_parquet(chunk_size=2, callback=cb)
    assert isinstance(result, Parquet)
    assert result.rows == 3
    assert len(calls) >= 1


@pytest.mark.asyncio
async def test_dbf_to_parquet_empty(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [])
    obj = DBF(path=dbf_path)
    result = await obj.to_parquet()
    assert isinstance(result, Parquet)
    assert result.path.exists()


@pytest.mark.asyncio
async def test_dbf_to_parquet_output_exists(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [("Alice",)])
    obj = DBF(path=dbf_path)

    out = tmp_dir / "existing.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out)

    result = await obj.to_parquet(output_path=out)
    assert isinstance(result, Parquet)
    assert result.rows == 1


@pytest.mark.asyncio
async def test_dbf_to_parquet_output_not_parquet(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [("Alice",)])
    obj = DBF(path=dbf_path)

    out = tmp_dir / "out.csv"
    out.write_text("a,b\n1,2")

    with pytest.raises(ConversionError, match="Could not parse"):
        await obj.to_parquet(output_path=out)


@pytest.mark.asyncio
async def test_dbf_to_parquet_non_parquet_extension(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [("Alice",)])
    obj = DBF(path=dbf_path)

    out = tmp_dir / "out.custom"
    result = await obj.to_parquet(output_path=out)
    assert isinstance(result, Parquet)
    assert out.exists()


# ---------------------------------------------------------------------------
# New tests for lines 507, 514, 520-521, 528-530, 542, 546-549, 560: DBC
# ---------------------------------------------------------------------------


def test_dbc_columns_raises():
    obj = DBC(path=Path("test.dbc"))
    with pytest.raises(FormatError):
        _ = obj.columns


def test_dbc_rows_raises():
    obj = DBC(path=Path("test.dbc"))
    with pytest.raises(FormatError):
        _ = obj.rows


@pytest.mark.asyncio
async def test_dbc_load_raises(tmp_dir):
    path = tmp_dir / "test.dbc"
    path.write_bytes(b"dummy")
    obj = DBC(path=path)
    with pytest.raises(struct.error):
        await obj.load()


@pytest.mark.asyncio
async def test_dbc_stream_raises(tmp_dir):
    path = tmp_dir / "test.dbc"
    path.write_bytes(b"dummy")
    obj = DBC(path=path)

    try:
        with pytest.raises(struct.error):
            async for _ in obj.stream():
                pass
    finally:
        import gc

        gc.collect()


@pytest.mark.asyncio
async def test_dbc_to_parquet_output_exists_is_parquet(tmp_dir):
    path = tmp_dir / "test.dbc"
    path.write_bytes(b"dummy")
    obj = DBC(path=path)

    out = tmp_dir / "out.parquet"
    pd.DataFrame({"x": [1]}).to_parquet(out)

    result = await obj.to_parquet(output_path=out)
    assert isinstance(result, Parquet)


@pytest.mark.asyncio
async def test_dbc_to_parquet_output_exists_not_parquet(tmp_dir):
    path = tmp_dir / "test.dbc"
    path.write_bytes(b"dummy")
    obj = DBC(path=path)

    out = tmp_dir / "out.csv"
    out.write_text("a,b\n1,2")

    with pytest.raises(ConversionError, match="Could not parse"):
        await obj.to_parquet(output_path=out)


# ---------------------------------------------------------------------------
# New tests for lines 580-585, 593: JSON.columns and JSON.rows
# ---------------------------------------------------------------------------


def test_json_columns(tmp_dir):
    path = tmp_dir / "data.json"
    path.write_text('[{"a": 1, "b": "x"}]')
    obj = JSON(path=path)
    cols = obj.columns
    assert len(cols) == 2
    assert cols[0].name == "a"
    assert cols[1].name == "b"


def test_json_columns_empty(tmp_dir):
    path = tmp_dir / "empty.json"
    path.write_text("")
    obj = JSON(path=path)
    cols = obj.columns
    assert cols == []


def test_json_rows(tmp_dir):
    path = tmp_dir / "data.json"
    path.write_text('[{"a": 1}, {"a": 2}, {"a": 3}]')
    obj = JSON(path=path)
    assert obj.rows == 3


# ---------------------------------------------------------------------------
# New tests for line 628: PDF.stream without chunk_size
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pdf_stream_no_chunk_size(tmp_dir):
    path = tmp_dir / "file.pdf"
    content = b"%PDF-1.4\n...content..."
    path.write_bytes(content)

    obj = PDF(path=path)
    chunks = await collect_async(obj.stream())
    assert b"".join(chunks) == content
    assert len(chunks) == 1


# ---------------------------------------------------------------------------
# New tests for line 642: Zip.load
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zip_load(tmp_dir):
    zip_path = tmp_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("test.txt", "hello")

    obj = Zip(path=zip_path)
    result = await obj.load()
    assert isinstance(result, zipfile.ZipFile)


# ---------------------------------------------------------------------------
# New tests for lines 692-718: Zip.to_parquet
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zip_to_parquet(tmp_dir):
    zip_path = tmp_dir / "data.zip"
    csv_path = tmp_dir / "inner.csv"
    pd.DataFrame({"x": [1, 2], "y": [3, 4]}).to_csv(csv_path, index=False)
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(csv_path, arcname="inner.csv")

    obj = Zip(path=zip_path)
    pq_obj = await obj.to_parquet()
    assert isinstance(pq_obj, Parquet)
    df = await pq_obj.load()
    assert len(df) == 2

    parquet_path = tmp_dir / "data.parquet"
    assert parquet_path.exists()
    temp_dir = tmp_dir / "data.tmp_extract"
    assert not temp_dir.exists()


# ---------------------------------------------------------------------------
# New tests for lines 723-740: Zip._safe_cleanup
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zip_safe_cleanup_nonexistent(tmp_dir):
    obj = Zip(path=tmp_dir / "dummy.zip")
    await obj._safe_cleanup(tmp_dir / "nonexistent")
    # Should not raise


@pytest.mark.asyncio
async def test_zip_safe_cleanup_with_files(tmp_dir):
    (tmp_dir / "f1.txt").write_text("a")
    (tmp_dir / "f2.txt").write_text("b")
    obj = Zip(path=tmp_dir / "dummy.zip")
    await obj._safe_cleanup(tmp_dir)
    assert not (tmp_dir / "f1.txt").exists()
    assert not (tmp_dir / "f2.txt").exists()
    assert not tmp_dir.exists()


@pytest.mark.asyncio
async def test_zip_safe_cleanup_with_subdir(tmp_dir):
    sub = tmp_dir / "sub"
    sub.mkdir()
    (sub / "nested.txt").write_text("nested")
    (tmp_dir / "top.txt").write_text("top")
    obj = Zip(path=tmp_dir / "dummy.zip")
    await obj._safe_cleanup(tmp_dir)
    assert not tmp_dir.exists()


# ---------------------------------------------------------------------------
# New tests for line 764: GZip.open_member
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gzip_open_member(tmp_dir):
    path = tmp_dir / "data.csv.gz"
    raw = b"a,b\n1,2"
    with gzip.open(path, "wb") as f:
        f.write(raw)

    obj = GZip(path=path)
    result = await obj.open_member("data.csv")
    assert result == raw


# ---------------------------------------------------------------------------
# New tests for line 799: Tar.load
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tar_load(tmp_dir):
    tar_path = tmp_dir / "file.tar"
    f = tmp_dir / "a.txt"
    f.write_text("hello")
    with tarfile.open(tar_path, "w") as t:
        t.add(f, arcname="a.txt")

    obj = Tar(path=tar_path)
    result = await obj.load()
    assert isinstance(result, tarfile.TarFile)


# ---------------------------------------------------------------------------
# New tests for ExtensionFactory._identify (lines 944, 947-949, 957-958)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_extension_factory_identify_oserror(tmp_dir):
    path = tmp_dir / "nonexistent.csv"
    result = await ExtensionFactory._identify(path)
    assert result is None


@pytest.mark.asyncio
async def test_extension_factory_identify_csv_falls_back(tmp_dir):
    path = tmp_dir / "test.csv"
    path.write_text("a,b\n1,2")
    result = await ExtensionFactory._identify(path)
    assert result is None
    cls = await ExtensionFactory.get_file_class(path)
    assert cls is CSV


@pytest.mark.asyncio
async def test_extension_factory_identify_zip(tmp_dir):
    path = tmp_dir / "test.zip"
    with zipfile.ZipFile(path, "w") as zf:
        zf.writestr("file.txt", "hello")
    result = await ExtensionFactory._identify(path)
    assert result is Zip


@pytest.mark.asyncio
async def test_extension_factory_identify_gzip(tmp_dir):
    path = tmp_dir / "test.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"hello")
    result = await ExtensionFactory._identify(path)
    assert result is GZip


@pytest.mark.asyncio
async def test_extension_factory_identify_tar(tmp_dir):
    path = tmp_dir / "test.tar"
    with tarfile.open(path, "w") as tf:
        info = tarfile.TarInfo(name="file.txt")
        data = b"hello"
        info.size = len(data)
        import io

        tf.addfile(info, io.BytesIO(data))
    result = await ExtensionFactory._identify(path)
    assert result is Tar


@pytest.mark.asyncio
async def test_extension_factory_identify_pdf(tmp_dir):
    path = tmp_dir / "test.pdf"
    path.write_bytes(b"%PDF-1.4" + b"\x00" * 10)
    result = await ExtensionFactory._identify(path)
    assert result is PDF


@pytest.mark.asyncio
async def test_extension_factory_identify_json(tmp_dir):
    path = tmp_dir / "test.json"
    path.write_text('{"key": "value"}')
    result = await ExtensionFactory._identify(path)
    assert result is JSON


@pytest.mark.asyncio
async def test_extension_factory_identify_json_array(tmp_dir):
    path = tmp_dir / "test.json"
    path.write_text("[1, 2, 3]")
    result = await ExtensionFactory._identify(path)
    assert result is JSON


@pytest.mark.asyncio
async def test_extension_factory_identify_json_false_positive(tmp_dir):
    path = tmp_dir / "test.bin"
    path.write_text("{not valid json {{{")
    result = await ExtensionFactory._identify(path)
    assert result is None
    cls = await ExtensionFactory.get_file_class(path)
    assert cls is File


@pytest.mark.asyncio
async def test_extension_factory_identify_parquet(tmp_dir):
    path = tmp_dir / "test.parquet"
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"a": [1, 2, 3]})
    pq.write_table(table, path)
    result = await ExtensionFactory._identify(path)
    assert result is Parquet


@pytest.mark.asyncio
async def test_extension_factory_identify_unknown(tmp_dir):
    path = tmp_dir / "test.bin"
    path.write_bytes(b"\x00\x01\x02\x03")
    result = await ExtensionFactory._identify(path)
    assert result is None


@pytest.mark.asyncio
async def test_extension_factory_identify_dbf(tmp_dir):
    pytest.importorskip("dbfread")
    path = tmp_dir / "test.dbf"
    _create_dbf(path, [("NAME", "C", 10, 0)], [("Alice",)])
    result = await ExtensionFactory._identify(path)
    assert result is DBF


@pytest.mark.asyncio
async def test_extension_factory_identify_wrong_extension(tmp_dir):
    """Parquet content with .txt extension is detected by content."""
    path = tmp_dir / "data.txt"
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table({"a": [1, 2, 3]})
    pq.write_table(table, path)
    result = await ExtensionFactory._identify(path)
    assert result is Parquet
    cls = await ExtensionFactory.get_file_class(path)
    assert cls is Parquet


# ---------------------------------------------------------------------------
# New tests for line 1010: ExtensionFactory.instantiate non-string file_type
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_instantiate_non_string_file_type(monkeypatch, tmp_dir):
    path = tmp_dir / "test.custom"
    path.write_text("data")

    class CustomFile:
        type = 42

        def __init__(self, **kwargs):
            for k, v in kwargs.items():
                setattr(self, k, v)

    monkeypatch.setitem(ExtensionFactory._extensions, ".custom", CustomFile)

    obj = await ExtensionFactory.instantiate(path)
    assert obj.type == "FILE"


# ---------------------------------------------------------------------------
# New tests for line 486: pq.ParquetWriter in empty DBF
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbf_to_parquet_empty_writer(tmp_dir):
    dbf_path = tmp_dir / "empty.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [])
    out_path = tmp_dir / "empty_out.parquet"
    db = DBF(path=dbf_path)

    mock_table = MagicMock(spec=pa.Table)
    mock_writer = MagicMock()
    mock_parquet = MagicMock(spec=Parquet)

    with (
        patch("pysus.api.extensions.pa") as mock_pa,
        patch(
            "pysus.api.extensions.pq.ParquetWriter", return_value=mock_writer
        ),
        patch.object(
            ExtensionFactory, "instantiate", return_value=mock_parquet
        ),
    ):
        mock_pa.Table.from_pandas.return_value = mock_table
        result = await db.to_parquet(out_path)

    assert result is mock_parquet
    mock_writer.close.assert_called_once()


# ---------------------------------------------------------------------------
# New tests for line 521: DBC.load success path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbc_load_success(tmp_dir):
    dbf_path = tmp_dir / "test.dbc"
    dbf_path.touch()
    obj = DBC(path=dbf_path)

    def mock_dbc2dbf(infile, outfile):
        _create_dbf(Path(outfile), [("NAME", "C", 10, 0)], [("NAME", b"Alice")])

    mock_parquet = MagicMock(spec=Parquet)
    mock_parquet.load = AsyncMock(return_value=pd.DataFrame({"x": [1]}))

    with (
        patch("pysus.api.extensions.dbc2dbf", side_effect=mock_dbc2dbf),
        patch.object(DBF, "to_parquet", return_value=mock_parquet),
    ):
        df = await obj.load()

    assert list(df.columns) == ["x"]
    assert len(df) == 1


# ---------------------------------------------------------------------------
# New tests for lines 529-530: DBC.stream success path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbc_stream_success(tmp_dir):
    dbf_path = tmp_dir / "test.dbc"
    dbf_path.touch()
    obj = DBC(path=dbf_path)

    def mock_dbc2dbf(infile, outfile):
        _create_dbf(Path(outfile), [("NAME", "C", 10, 0)], [("NAME", b"Alice")])

    async def _mock_stream(**kw):
        yield pd.DataFrame({"x": [1]})

    mock_parquet = MagicMock(spec=Parquet)
    mock_parquet.stream = _mock_stream

    with (
        patch("pysus.api.extensions.dbc2dbf", side_effect=mock_dbc2dbf),
        patch.object(DBF, "to_parquet", return_value=mock_parquet),
    ):
        chunks = [chunk async for chunk in obj.stream(chunk_size=100)]

    assert len(chunks) == 1
    assert list(chunks[0].columns) == ["x"]


# ---------------------------------------------------------------------------
# New tests for line 560: DBC.to_parquet non-BaseTabularFile after dbc2dbf
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbc_to_parquet_not_tabular(tmp_dir):
    dbf_path = tmp_dir / "test.dbc"
    dbf_path.touch()
    obj = DBC(path=dbf_path)

    def mock_dbc2dbf(infile, outfile):
        pass

    mock_non_tabular = MagicMock(spec=BaseLocalFile)

    with (
        patch("pysus.api.extensions.dbc2dbf", side_effect=mock_dbc2dbf),
        patch.object(
            ExtensionFactory, "instantiate", return_value=mock_non_tabular
        ),
    ):
        with pytest.raises(ConversionError, match="Not a DBF"):
            await obj.to_parquet()


# ---------------------------------------------------------------------------
# New tests for line 708: Zip.to_parquet with no tabular file inside
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_zip_to_parquet_no_tabular(tmp_dir):
    zip_path = tmp_dir / "data.zip"
    text_path = tmp_dir / "readme.txt"
    text_path.write_text("hello")
    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(text_path, arcname="readme.txt")

    obj = Zip(path=zip_path)
    with pytest.raises(ConversionError, match="No tabular file found"):
        await obj.to_parquet()


@pytest.mark.asyncio
async def test_dbc_to_parquet_permission_error_cleanup(tmp_dir):
    """Cover the PermissionError retry in DBC.to_parquet finally block."""
    from unittest.mock import patch

    from pysus.api.extensions import DBC

    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [("Alice",)])
    dbc_path = tmp_dir / "test.dbc"
    dbf_path.rename(dbc_path)

    out = tmp_dir / "out.parquet"

    def fake_dbc2dbf(inp, outp):
        _create_dbf(Path(outp), [("NAME", "C", 10, 0)], [("Bob",)])

    with patch("pysus.api.extensions.dbc2dbf", side_effect=fake_dbc2dbf):
        with patch(
            "pathlib.Path.unlink",
            side_effect=[PermissionError, PermissionError],
        ):
            obj = DBC(path=dbc_path)
            result = await obj.to_parquet(output_path=out, chunk_size=10)
    assert result is not None


# ---------------------------------------------------------------------------
# Byte-level reader integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dbf_load_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0), ("AGE", "N", 3, 0)],
        [("Alice", "30"), ("Bob", "25")],
    )
    obj = DBF(path=dbf_path)
    df = await obj.load(fast=True)
    assert len(df) == 2
    assert list(df.columns) == ["NAME", "AGE"]
    assert df["NAME"].iloc[0] == "Alice"


@pytest.mark.asyncio
async def test_dbf_load_fast_fallback(tmp_dir):
    """Invalid DBF file should fall back to dbfread gracefully."""
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "bad.dbf"
    _create_dbf(
        dbf_path,
        [("NAME", "C", 10, 0)],
        [("test",)],
    )
    obj = DBF(path=dbf_path)
    df = await obj.load(fast=True)
    assert len(df) == 1
    assert df["NAME"].iloc[0] == "test"


@pytest.mark.asyncio
async def test_dbf_load_no_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("COL", "C", 5, 0)],
        [("val1",), ("val2",)],
    )
    obj = DBF(path=dbf_path)
    df = await obj.load(fast=False)
    assert len(df) == 2
    assert df["COL"].iloc[0] == "val1"


@pytest.mark.asyncio
async def test_dbf_stream_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("X", "C", 4, 0)],
        [(f"r{i}",) for i in range(150)],
    )
    obj = DBF(path=dbf_path)
    chunks = await collect_async(obj.stream(chunk_size=50, fast=True))
    assert len(chunks) == 3
    assert all(len(c) == 50 for c in chunks[:2])
    assert len(chunks[2]) == 50


@pytest.mark.asyncio
async def test_dbf_stream_fast_fallback(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("Y", "C", 3, 0)],
        [("a",), ("b",), ("c",)],
    )
    obj = DBF(path=dbf_path)
    chunks = await collect_async(obj.stream(chunk_size=2, fast=True))
    assert len(chunks) == 2
    assert len(chunks[0]) == 2
    assert len(chunks[1]) == 1


@pytest.mark.asyncio
async def test_dbf_stream_no_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("Z", "C", 4, 0)],
        [("x",), ("y",)],
    )
    obj = DBF(path=dbf_path)
    chunks = await collect_async(obj.stream(chunk_size=1, fast=False))
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_dbf_to_parquet_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("A", "C", 8, 0), ("B", "C", 8, 0)],
        [("foo", "bar"), ("baz", "qux")],
    )
    obj = DBF(path=dbf_path)
    out = tmp_dir / "out.parquet"
    result = await obj.to_parquet(output_path=out, fast=True)
    assert isinstance(result, Parquet)
    assert out.exists()
    df = pd.read_parquet(out)
    assert len(df) == 2


@pytest.mark.asyncio
async def test_dbf_to_parquet_fast_fallback(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("K", "C", 6, 0)],
        [("abc",)],
    )
    obj = DBF(path=dbf_path)
    out = tmp_dir / "out.parquet"
    result = await obj.to_parquet(output_path=out, fast=True)
    assert isinstance(result, Parquet)
    assert out.exists()


@pytest.mark.asyncio
async def test_dbf_to_parquet_no_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("V", "C", 5, 0)],
        [("x",)],
    )
    obj = DBF(path=dbf_path)
    out = tmp_dir / "out.parquet"
    result = await obj.to_parquet(output_path=out, fast=False)
    assert isinstance(result, Parquet)


@pytest.mark.asyncio
async def test_dbf_columns_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("F1", "C", 10, 0), ("F2", "N", 5, 0)],
        [("a", "1")],
    )
    obj = DBF(path=dbf_path)
    cols = obj.columns
    assert len(cols) == 2
    assert cols[0].name == "F1"
    assert cols[1].name == "F2"


@pytest.mark.asyncio
async def test_dbf_rows_fast(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "test.dbf"
    _create_dbf(
        dbf_path,
        [("C1", "C", 5, 0)],
        [("a",), ("b",), ("c",)],
    )
    obj = DBF(path=dbf_path)
    assert obj.rows == 3


@pytest.mark.asyncio
async def test_dbf_load_fast_empty(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "empty.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [])
    obj = DBF(path=dbf_path)
    df = await obj.load(fast=True)
    assert len(df) == 0
    assert list(df.columns) == ["NAME"]


@pytest.mark.asyncio
async def test_dbf_to_parquet_fast_empty(tmp_dir):
    pytest.importorskip("dbfread")
    dbf_path = tmp_dir / "empty.dbf"
    _create_dbf(dbf_path, [("NAME", "C", 10, 0)], [])
    obj = DBF(path=dbf_path)
    out = tmp_dir / "out.parquet"
    result = await obj.to_parquet(output_path=out, fast=True)
    assert isinstance(result, Parquet)
    assert out.exists()


# ---------------------------------------------------------------------------
# Regression tests for bugs found during review
# ---------------------------------------------------------------------------


def test_json_columns_reads_keys_correctly(tmp_dir):
    """JSON.columns must parse first record's keys, not use nrows=0."""
    path = tmp_dir / "data.json"
    path.write_text(
        '[{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]'
    )
    obj = JSON(path=path)
    cols = obj.columns
    assert len(cols) == 2
    names = {c.name for c in cols}
    assert names == {"name", "age"}


def test_json_columns_dict_object(tmp_dir):
    """JSON.columns must handle top-level dict objects."""
    path = tmp_dir / "data.json"
    path.write_text('{"name": "Alice", "age": 30}')
    obj = JSON(path=path)
    cols = obj.columns
    assert len(cols) == 2
    names = {c.name for c in cols}
    assert names == {"name", "age"}


def test_json_columns_nested_list(tmp_dir):
    """JSON.columns must return empty for nested structures with no keys."""
    path = tmp_dir / "data.json"
    path.write_text("[1, 2, 3]")
    obj = JSON(path=path)
    cols = obj.columns
    assert cols == []


@pytest.mark.asyncio
async def test_tar_path_traversal_blocked(tmp_dir):
    """Tar.extract() must reject members with path traversal."""
    path = tmp_dir / "evil.tar"
    with tarfile.open(path, "w") as t:
        info = tarfile.TarInfo(name="../../etc/passwd")
        data = b"evil"
        info.size = len(data)
        import io

        t.addfile(info, io.BytesIO(data))
    obj = Tar(path=path)
    with pytest.raises(ValueError, match="Path traversal"):
        await obj.extract(target_dir=tmp_dir / "out")


@pytest.mark.asyncio
async def test_zip_path_traversal_blocked(tmp_dir):
    """Zip.extract() must reject members with path traversal."""
    path = tmp_dir / "evil.zip"
    with zipfile.ZipFile(path, "w") as z:
        z.writestr("../../etc/passwd", "evil")
    obj = Zip(path=path)
    with pytest.raises(ValueError, match="Path traversal"):
        await obj.extract(target_dir=tmp_dir / "out")


@pytest.mark.asyncio
async def test_csv_columns_semicolon_delimiter(tmp_dir):
    """CSV.columns must use detected delimiter, not hardcode comma."""
    path = tmp_dir / "data.csv"
    path.write_text("name;age\nAlice;30\n")
    obj = CSV(path=path)
    await obj._get_sep()
    cols = obj.columns
    assert len(cols) == 2
    names = {c.name for c in cols}
    assert names == {"name", "age"}


def test_csv_rows_quoted_newlines(tmp_dir):
    """CSV.rows must not count newlines inside quoted fields."""
    path = tmp_dir / "data.csv"
    path.write_text('name,bio\nAlice,"line1\nline2"\nBob,x\n')
    obj = CSV(path=path)
    assert obj.rows == 2


@pytest.mark.asyncio
async def test_tar_type_is_tar(tmp_dir):
    """Tar.type must be 'TAR', not 'ZIP'."""
    path = tmp_dir / "test.tar"
    with tarfile.open(path, "w") as t:
        info = tarfile.TarInfo(name="file.txt")
        data = b"hello"
        info.size = len(data)
        import io

        t.addfile(info, io.BytesIO(data))
    obj = Tar(path=path)
    assert obj.type == "TAR"


@pytest.mark.asyncio
async def test_gzip_type_is_gzip(tmp_dir):
    """GZip.type must be 'GZIP', not 'ZIP'."""
    path = tmp_dir / "test.gz"
    with gzip.open(path, "wb") as f:
        f.write(b"hello")
    obj = GZip(path=path)
    assert obj.type == "GZIP"


@pytest.mark.asyncio
async def test_safe_cleanup_nested_directories(tmp_dir):
    """Zip._safe_cleanup must handle nested directory structures."""
    nested = tmp_dir / "a" / "b" / "c"
    nested.mkdir(parents=True)
    (nested / "file.txt").write_text("data")
    (tmp_dir / "a" / "file2.txt").write_text("data2")

    zip_path = tmp_dir / "test.zip"
    with zipfile.ZipFile(zip_path, "w") as z:
        z.writestr("a/b/c/file.txt", "data")
        z.writestr("a/file2.txt", "data2")

    obj = Zip(path=zip_path)
    result = await obj.extract(target_dir=tmp_dir / "out")
    assert len(result) > 0

    await obj._safe_cleanup(tmp_dir / "out")
    assert not (tmp_dir / "out").exists()


@pytest.mark.asyncio
async def test_tar_symlink_blocked(tmp_dir):
    """Tar.extract() must reject symlinks."""
    path = tmp_dir / "evil.tar"
    with tarfile.open(path, "w") as t:
        link = tarfile.TarInfo(name="link")
        link.type = tarfile.SYMTYPE
        link.linkname = "/etc/passwd"
        t.addfile(link)
    obj = Tar(path=path)
    with pytest.raises(ValueError, match="Symlink"):
        await obj.extract(target_dir=tmp_dir / "out")


@pytest.mark.asyncio
async def test_json_detect_valid(tmp_dir):
    """JSON detection must succeed for valid small JSON."""
    path = tmp_dir / "data.json"
    path.write_text('{"a": 1, "b": 2}')
    result = await ExtensionFactory._identify(path)
    assert result is JSON


@pytest.mark.asyncio
async def test_json_detect_rejects_binary_starting_with_brace(tmp_dir):
    """JSON detection must reject binary files starting with {."""
    path = tmp_dir / "data.bin"
    path.write_bytes(b"{\x00\x01\x02\x03}")
    result = await ExtensionFactory._identify(path)
    assert result is None


@pytest.mark.asyncio
async def test_parquet_columns_cached(tmp_dir):
    """Parquet.columns must return the same list on repeated access."""
    path = tmp_dir / "test.parquet"
    table = pa.table({"x": [1, 2], "y": ["a", "b"]})
    pq.write_table(table, path)
    obj = Parquet(path=path)
    first = obj.columns
    second = obj.columns
    assert first is second


@pytest.mark.asyncio
async def test_csv_columns_cached(tmp_dir):
    """CSV.columns must return the same list on repeated access."""
    path = tmp_dir / "test.csv"
    path.write_text("a,b\n1,2\n")
    obj = CSV(path=path)
    first = obj.columns
    second = obj.columns
    assert first is second


@pytest.mark.asyncio
async def test_csv_rows_cached(tmp_dir):
    """CSV.rows must return the same int on repeated access."""
    path = tmp_dir / "test.csv"
    path.write_text("a,b\n1,2\n3,4\n")
    obj = CSV(path=path)
    first = obj.rows
    second = obj.rows
    assert first == second
