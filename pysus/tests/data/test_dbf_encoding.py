"""Code page detection from the DBF header (LDID, byte 29). See #382."""

import struct
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pytest
from pysus.data.dbf_reader import (
    LDID_CODEPAGES,
    _make_decoder,
    _parse_header,
    read_dbf_fast,
    read_dbf_filtered,
    resolve_encoding,
    stream_dbf_fast,
)


def _write_dbf(path: Path, ldid: int, values: list[bytes], width: int = 20):
    """One C(*width*) field named TXT, one record per raw value."""
    header = bytearray(32)
    header[0] = 0x03
    struct.pack_into("<IHH", header, 4, len(values), 32 + 32 + 1, 1 + width)
    header[29] = ldid
    fld = bytearray(32)
    fld[:3] = b"TXT"
    fld[11] = ord("C")
    fld[16] = width
    body = b"".join(b" " + v.ljust(width)[:width] for v in values)
    path.write_bytes(bytes(header + fld) + b"\r" + body + b"\x1a")
    return path


def _read_all(path: Path, **kw) -> list[pd.DataFrame]:
    """Read *path* through the three public readers."""
    fast = read_dbf_fast(path, **kw)
    streamed = pd.concat(
        list(stream_dbf_fast(path, chunk_size=1, **kw)), ignore_index=True
    )
    filtered = read_dbf_filtered(path, "TXT", [""], prefix_match=True, **kw)
    return [fast, streamed, filtered]


def _reencode(value: str, codepage: str) -> bytes:
    """Inverse of the lossless decoder: undefined bytes map back to ord()."""
    out = bytearray()
    for ch in value:
        try:
            out += ch.encode(codepage)
        except UnicodeEncodeError:
            cp = ord(ch)
            out.append(cp - 0xF000 if 0xF000 <= cp <= 0xF0FF else cp)
    return bytes(out)


def test_header_exposes_ldid(tmp_path):
    p = _write_dbf(tmp_path / "a.dbf", 0x02, [b"X"])
    assert _parse_header(p).ldid == 0x02


@pytest.mark.parametrize(
    "ldid, codepage, text",
    [
        (0x02, "cp850", "INFORMAÇÃO"),  # SIH RD
        (0x57, "cp1252", "N° ª º ç"),  # SIA AD
        (0x58, "cp1252", "€ “aspas”"),  # CNES ST (older files)
        (0x03, "cp1252", "SÃO PAULO"),  # SIA PA
    ],
)
def test_declared_codepage_is_used(tmp_path, ldid, codepage, text):
    p = _write_dbf(tmp_path / "a.dbf", ldid, [text.encode(codepage)])
    for df in _read_all(p):
        assert df["TXT"].tolist() == [text]


def test_sih_cp850_reproduction_from_issue(tmp_path):
    raw = "INFORMAÇÃO".encode("cp850")
    p = _write_dbf(tmp_path / "a.dbf", 0x02, [raw])
    value = read_dbf_fast(p)["TXT"][0]
    assert value == "INFORMAÇÃO"
    assert "\x87" not in value and "\x80" not in value


@pytest.mark.parametrize("ldid", [0x00, 0x35, 0xFF])
def test_missing_or_unknown_ldid_falls_back_to_latin1(tmp_path, ldid):
    # 0x00: no code page declared (CNES ST, SINAN, SINASC)
    # 0x35: what every SIM file declares, not a known LDID
    raw = "Nº 6° TENTATIVA".encode("latin-1")
    p = _write_dbf(tmp_path / "a.dbf", ldid, [raw])
    for df in _read_all(p):
        assert df["TXT"].tolist() == ["Nº 6° TENTATIVA"]


def test_fallback_encoding_is_configurable(tmp_path):
    p = _write_dbf(tmp_path / "a.dbf", 0x35, ["ação".encode("cp850")])
    for df in _read_all(p, fallback_encoding="cp850"):
        assert df["TXT"].tolist() == ["ação"]


def test_explicit_encoding_overrides_ldid(tmp_path):
    p = _write_dbf(tmp_path / "a.dbf", 0x02, ["ação".encode("latin-1")])
    for df in _read_all(p, encoding="latin-1"):
        assert df["TXT"].tolist() == ["ação"]


def test_undefined_cp1252_bytes_are_kept(tmp_path):
    # ADRR2608 declares cp1252 but AP_CNSPCN holds an encoded identifier
    # made of bytes 0x7B-0x84, including 0x81 (undefined in cp1252).
    raw = bytes([0x7B, 0x80, 0x81, 0x82, 0x83, 0x84, 0x8D, 0x8F, 0x90, 0x9D])
    p = _write_dbf(tmp_path / "a.dbf", 0x57, [raw])
    for df in _read_all(p):
        value = df["TXT"][0]
        assert "�" not in value
        assert value == "{€\x81‚ƒ„\x8d\x8f\x90\x9d"
        assert _reencode(value, "cp1252") == raw


@pytest.mark.parametrize("codepage", sorted(set(LDID_CODEPAGES.values())))
def test_every_byte_survives_each_codepage(codepage):
    # No byte is ever replaced or dropped, whatever the declared code page.
    raw = bytes(range(0x100))
    value = _make_decoder(codepage)(raw)
    assert "�" not in value
    assert _reencode(value, codepage) == raw


def test_resolve_encoding():
    assert resolve_encoding(0x02) == "cp850"
    assert resolve_encoding(0x00) == "latin-1"
    assert resolve_encoding(0x35) == "latin-1"
    assert resolve_encoding(0x35, fallback="cp1252") == "cp1252"
    assert resolve_encoding(0x02, encoding="utf-8") == "utf-8"


def test_explicit_multibyte_encoding_still_works(tmp_path):
    p = _write_dbf(tmp_path / "a.dbf", 0x00, ["DEMÊNCIA".encode()])
    assert read_dbf_fast(p, encoding="utf-8")["TXT"][0] == "DEMÊNCIA"


def test_concurrent_reads_do_not_share_encoding(tmp_path):
    # The encoding used to live in a module-level global; two reads of files
    # with different code pages running in threads could overwrite it.
    cp850 = _write_dbf(
        tmp_path / "sih.dbf", 0x02, ["INFORMAÇÃO".encode("cp850")] * 2000
    )
    latin = _write_dbf(
        tmp_path / "cnes.dbf", 0x00, ["Nº ALVARÁ".encode("latin-1")] * 2000
    )

    def read(path):
        return {
            v
            for chunk in stream_dbf_fast(path, chunk_size=7)
            for v in chunk["TXT"]
        }

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [
            pool.submit(read, cp850 if i % 2 else latin) for i in range(16)
        ]
        results = [f.result() for f in futures]

    for i, values in enumerate(results):
        expected = "INFORMAÇÃO" if i % 2 else "Nº ALVARÁ"
        assert values == {expected}


@pytest.mark.parametrize(
    "ldid, codepage, text",
    [
        (0x02, "cp850", "INFORMAÇÃO"),  # SIH RD
        (0x57, "cp1252", "N° ª º ç"),  # SIA AD
        (0x35, "latin-1", "Nº ALVARÁ"),  # SIM, unknown LDID
    ],
)
@pytest.mark.asyncio
async def test_dbfread_fallback_uses_declared_codepage(
    tmp_path, ldid, codepage, text
):
    # The dbfread path (fast=False, or after the fast reader fails) must
    # decode with the same encoding as the fast reader.
    pytest.importorskip("dbfread")
    from pysus.api.extensions import DBF

    p = _write_dbf(tmp_path / "a.dbf", ldid, [text.encode(codepage)] * 3)
    obj = DBF(path=p)
    assert obj.encoding == resolve_encoding(ldid)

    loaded = await obj.load(fast=False)
    assert loaded["TXT"].tolist() == [text] * 3

    chunks = [c async for c in obj.stream(chunk_size=2, fast=False)]
    assert pd.concat(chunks)["TXT"].tolist() == [text] * 3

    out = await obj.to_parquet(tmp_path / "a.parquet", fast=False)
    assert (await out.load())["TXT"].tolist() == [text] * 3


def test_dbfread_fallback_decode_is_lossless(tmp_path):
    pytest.importorskip("dbfread")
    from pysus.api.extensions import DBF

    p = _write_dbf(tmp_path / "a.dbf", 0x57, [b"X"])
    value = DBF(path=p).decode_column(b"AB\x81CD")
    assert value == "AB\x81CD"
    assert _reencode(value, "cp1252") == b"AB\x81CD"
