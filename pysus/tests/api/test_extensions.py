import pytest
import pandas as pd
import json
import gzip
import tarfile
import zipfile
from pysus.api.extensions import (
    ExtensionFactory,
    CSV,
    Parquet,
    JSON,
    Directory,
    File,
    Zip,
    GZip,
    Tar,
    PDF,
)


@pytest.fixture
def tmp_dir(tmp_path):
    return tmp_path


@pytest.mark.asyncio
async def test_directory_instantiation(tmp_dir):
    subdir = tmp_dir / "test_subdir"
    subdir.mkdir()
    (subdir / "file.txt").write_text("hello")

    obj = await ExtensionFactory.instantiate(subdir)
    assert isinstance(obj, Directory)
    assert obj.basename == "test_subdir"

    content = await obj.load()
    assert len(content) == 1
    assert content[0].basename == "file.txt"


@pytest.mark.asyncio
async def test_csv_functionality(tmp_dir):
    csv_path = tmp_dir / "data.csv"
    df_orig = pd.DataFrame({"a": ["1", "2"], "b": ["3", "4"]})
    df_orig.to_csv(csv_path, index=False)

    obj = await ExtensionFactory.instantiate(csv_path)
    assert isinstance(obj, CSV)

    df_loaded = await obj.load()
    assert df_loaded.shape == (2, 2)

    chunks = []
    async for chunk in obj.stream(chunk_size=1):
        chunks.append(chunk)
    assert len(chunks) == 2


@pytest.mark.asyncio
async def test_parquet_conversion(tmp_dir):
    csv_path = tmp_dir / "source.csv"
    pd.DataFrame({"col": [1, 2, 3]}).to_csv(csv_path, index=False)

    csv_obj = await ExtensionFactory.instantiate(csv_path)
    parquet_obj = await csv_obj.to_parquet()

    assert isinstance(parquet_obj, Parquet)
    assert parquet_obj.path.suffix == ".parquet"
    assert parquet_obj.path.exists()

    df = await parquet_obj.load()
    assert len(df) == 3


@pytest.mark.asyncio
async def test_json_functionality(tmp_dir):
    json_path = tmp_dir / "data.json"
    data = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
    json_path.write_text(json.dumps(data))

    obj = await ExtensionFactory.instantiate(json_path)
    assert isinstance(obj, JSON)

    df = await obj.load()
    assert df.iloc[0]["val"] == "a"


@pytest.mark.asyncio
async def test_pdf_functionality(tmp_dir):
    pdf_path = tmp_dir / "test.pdf"
    content = b"%PDF-1.4\n1 0 obj\n<< /Type /Catalog >>\nendobj"
    pdf_path.write_bytes(content)

    obj = await ExtensionFactory.instantiate(pdf_path)
    assert isinstance(obj, PDF)

    loaded_content = await obj.load()
    assert loaded_content.startswith(b"%PDF-")

    chunks = []
    async for chunk in obj.stream(chunk_size=10):
        chunks.append(chunk)
    assert len(chunks) > 0
    assert b"".join(chunks) == content


@pytest.mark.asyncio
async def test_generic_file(tmp_dir):
    file_path = tmp_dir / "random.bin"
    content = b"some binary data"
    file_path.write_bytes(content)

    obj = await ExtensionFactory.instantiate(file_path)
    assert isinstance(obj, File)

    loaded = await obj.load()
    assert loaded == content


@pytest.mark.asyncio
async def test_zip_extraction(tmp_dir):
    zip_path = tmp_dir / "test.zip"
    inner_file = tmp_dir / "inner.csv"
    pd.DataFrame({"x": [1]}).to_csv(inner_file, index=False)

    with zipfile.ZipFile(zip_path, "w") as z:
        z.write(inner_file, arcname="inner.csv")

    obj = await ExtensionFactory.instantiate(zip_path)
    assert isinstance(obj, Zip)

    members = await obj.list_members()
    assert "inner.csv" in members

    extracted = await obj.extract(target_dir=tmp_dir / "extracted")
    assert len(extracted) >= 1
    assert any(isinstance(f, CSV) for f in extracted)


@pytest.mark.asyncio
async def test_gzip_functionality(tmp_dir):
    gz_path = tmp_dir / "data.csv.gz"
    content = b"header,val\n1,2"
    with gzip.open(gz_path, "wb") as f:
        f.write(content)

    obj = await ExtensionFactory.instantiate(gz_path)
    assert isinstance(obj, GZip)

    data = await obj.load()
    assert data == content


@pytest.mark.asyncio
async def test_tar_functionality(tmp_dir):
    tar_path = tmp_dir / "test.tar"
    content_path = tmp_dir / "file.txt"
    content_path.write_text("tar content")

    with tarfile.open(tar_path, "w") as tar:
        tar.add(content_path, arcname="file.txt")

    obj = await ExtensionFactory.instantiate(tar_path)
    assert isinstance(obj, Tar)

    members = await obj.list_members()
    assert "file.txt" in members

    member_data = await obj.open_member("file.txt")
    assert member_data == b"tar content"
