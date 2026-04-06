import gzip
import json
import tarfile
import zipfile
from pathlib import Path

import pandas as pd
import pytest
from pysus.api.extensions import (
    CSV,
    DBC,
    DBF,
    FTP_IMPORT,
    JSON,
    PDF,
    Directory,
    ExtensionFactory,
    File,
    GZip,
    Parquet,
    Tar,
    Zip,
)


# -------------------------
# Fixtures & helpers
# -------------------------
@pytest.fixture
def tmp_dir(tmp_path: Path):
    return tmp_path


async def collect_async(gen):
    out = []
    async for item in gen:
        out.append(item)
    return out


# -------------------------
# Directory
# -------------------------
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


# -------------------------
# CSV
# -------------------------
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


# -------------------------
# Parquet
# -------------------------
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


# -------------------------
# DBF
# -------------------------
@pytest.mark.asyncio
async def test_dbf_decode_and_failure(tmp_dir):
    pytest.importorskip("dbfread")

    path = tmp_dir / "test.dbf"
    path.write_bytes(b"invalid")

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, DBF)

    assert obj.decode_column(b"COL\x00") == "COL"
    assert obj.decode_column("COL\x00") == "COL"

    with pytest.raises(Exception):
        await obj.load()


# -------------------------
# DBC
# -------------------------
@pytest.mark.asyncio
async def test_dbc_import_behavior(tmp_dir):
    path = tmp_dir / "file.dbc"
    path.write_bytes(b"dummy")

    obj = await ExtensionFactory.instantiate(path)
    assert isinstance(obj, DBC)

    if not FTP_IMPORT:
        with pytest.raises(ImportError):
            await obj.load()
        with pytest.raises(ImportError):
            await obj.to_parquet()
    else:
        with pytest.raises(Exception):
            await obj.to_parquet(tmp_dir / "out.parquet")


# -------------------------
# JSON
# -------------------------
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


# -------------------------
# PDF
# -------------------------
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


# -------------------------
# Generic File
# -------------------------
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


# -------------------------
# ZIP
# -------------------------
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


# -------------------------
# GZIP
# -------------------------
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


# -------------------------
# TAR
# -------------------------
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
