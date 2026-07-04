"""
Golden parity tests for ``pysus.data.dbf_reader`` against ``dbfread``.

Follow-up to AlertaDengue/PySUS #288 / #296. ``dbfread`` was PySUS's reader for
years, so it is the de-facto reference for correctness: for the same file, the
fast path should produce the same rows and values.

Two groups:

* **Parity** — the fast path matches ``dbfread`` (rows + values), including the
  latin-1 round-trip and the ``E104`` vs ``E109`` prefix/exact CID guard.
* **Regression** — the four divergences fixed in this PR:
    1. ``read_dbf_fast`` / ``stream_dbf_fast`` now skip deleted records (they
       didn't, so they disagreed with ``read_dbf_filtered`` and ``dbfread``).
    2. exact match (``prefix_match=False``) now matches values shorter than the
       field width and values with trailing NUL padding.
    3. default encoding is ``latin-1`` (DATASUS), not ``cp1252``.
    4. ``columns=`` in ``read_dbf_fast`` is case-insensitive, like the filter
       column lookup.

Fixtures are written by a small in-test dBASE III writer, so no real DATASUS
files are needed.
"""

from __future__ import annotations

import struct
from pathlib import Path

import pandas as pd
from dbfread import DBF
from pysus.data import dbf_reader

# ───────────────────────── fixture writer ─────────────────────────


def make_dbf(path, fields, records, deleted=()):
    """Write a minimal dBASE III .dbf with character ('C') fields.

    fields  : list of (name, length)
    records : list of rows; str values are latin-1 encoded and space-padded,
              bytes values are written verbatim (truncated/space-padded)
    deleted : indices of records to flag as deleted ('*')
    """
    record_len = 1 + sum(length for _, length in fields)
    header_len = 32 + 32 * len(fields) + 1

    out = bytearray(32)
    out[0] = 0x03
    struct.pack_into("<I", out, 4, len(records))
    struct.pack_into("<H", out, 8, header_len)
    struct.pack_into("<H", out, 10, record_len)

    for name, length in fields:
        fd = bytearray(32)
        nm = name.encode("ascii")[:11]
        fd[0 : len(nm)] = nm
        fd[11] = ord("C")
        fd[16] = length & 0xFF
        out += fd
    out.append(0x0D)

    for ri, rec in enumerate(records):
        out.append(0x2A if ri in deleted else 0x20)
        for (_, length), val in zip(fields, rec):
            b = (
                val
                if isinstance(val, bytes)
                else str(val).encode("latin-1", "replace")
            )
            out += b[:length].ljust(length, b" ")
    out.append(0x1A)

    Path(path).write_bytes(bytes(out))
    return str(path)


def dbfread_rows(path, encoding="latin-1"):
    return list(DBF(str(path), encoding=encoding, char_decode_errors="replace"))


# ───────────────────────────── parity ─────────────────────────────


def test_parity_read_dbf_fast_vs_dbfread(tmp_path):
    """Row-for-row, value-for-value parity on a plain file."""
    fields = [("UF", 2), ("DIAG_PRINC", 4), ("MUNIC_RES", 6), ("VAL_TOT", 10)]
    records = [
        ["SP", "G200", "355030", "1234.56"],
        ["RJ", "I10", "330455", "0.01"],
        ["MG", "E104", "310620", ""],
        ["BA", "", "292740", "99"],
    ]
    p = make_dbf(tmp_path / "plain.dbf", fields, records)

    df = dbf_reader.read_dbf_fast(p)
    ref = dbfread_rows(p)

    assert list(df.columns) == [n for n, _ in fields]
    assert len(df) == len(ref)
    for i, rec in enumerate(ref):
        for name, _ in fields:
            assert df.iloc[i][name] == str(rec[name]).strip(), (i, name)


def test_parity_latin1_accents(tmp_path):
    """DATASUS text is latin-1; accented bytes round-trip (default path)."""
    p = make_dbf(
        tmp_path / "acc.dbf",
        [("NOME", 12)],
        [[b"JO\xe3O"], [b"CONCEI\xc7\xc3O"], [b"M\xe3E"]],
    )
    df = dbf_reader.read_dbf_fast(p)  # default encoding now latin-1
    ref = dbfread_rows(p, encoding="latin-1")
    assert (
        list(df["NOME"])
        == [r["NOME"] for r in ref]
        == ["JOãO", "CONCEIÇÃO", "MãE"]
    )


def test_parity_filtered_prefix_vs_dbfread(tmp_path):
    """read_dbf_filtered (prefix path) == dbfread + manual filter."""
    fields = [("SP_NAIH", 13), ("SP_VALATO", 10)]
    records = [["123", "10"], ["456", "20"], ["123", "30"], ["789", "40"]]
    p = make_dbf(tmp_path / "filt.dbf", fields, records)

    df = dbf_reader.read_dbf_filtered(p, "SP_NAIH", ["123"], prefix_match=True)
    ref = [r for r in dbfread_rows(p) if str(r["SP_NAIH"]).strip() == "123"]
    assert len(df) == len(ref) == 2
    assert list(df["SP_VALATO"]) == [str(r["SP_VALATO"]).strip() for r in ref]


def test_parity_stream_vs_dbfread(tmp_path):
    """Streaming chunks, concatenated, equal the dbfread rows."""
    fields = [("A", 3), ("B", 3)]
    records = [[f"{i:03d}", "xyz"] for i in range(257)]
    p = make_dbf(tmp_path / "stream.dbf", fields, records)

    chunks = list(dbf_reader.stream_dbf_fast(p, chunk_size=100))
    df = pd.concat(chunks, ignore_index=True)
    ref = dbfread_rows(p)
    assert len(df) == len(ref)
    assert list(df["A"]) == [str(r["A"]).strip() for r in ref]


def test_prefix_match_nul_padding(tmp_path):
    """Prefix matching survives DATASUS NUL padding."""
    p = make_dbf(
        tmp_path / "nulpref.dbf",
        [("CID", 6)],
        [[b"G20\x00\x00\x00"], [b"Z999  "]],
    )
    df = dbf_reader.read_dbf_filtered(p, "CID", ["G20"], prefix_match=True)
    assert len(df) == 1


def test_prefix_distinguishes_4char_exact(tmp_path):
    """DATASUS CID semantics: 'E104' (fills the field) must not match 'E109'."""
    p = make_dbf(
        tmp_path / "cid.dbf",
        [("DIAG_PRINC", 4)],
        [["E104"], ["E109"], ["G400"]],
    )
    df = dbf_reader.read_dbf_filtered(
        p, "DIAG_PRINC", ["E104"], prefix_match=True
    )
    assert list(df["DIAG_PRINC"]) == ["E104"]


# ───────────── regression tests for the fixes in this PR ─────────────


def test_read_dbf_fast_skips_deleted(tmp_path):
    """(fix 1) read_dbf_fast drops '*'-flagged records, like dbfread."""
    p = make_dbf(
        tmp_path / "del.dbf",
        [("X", 3)],
        [["AAA"], ["BBB"], ["CCC"]],
        deleted={1},
    )
    df = dbf_reader.read_dbf_fast(p)
    ref = dbfread_rows(p)  # AAA, CCC
    assert list(df["X"]) == [str(r["X"]).strip() for r in ref] == ["AAA", "CCC"]


def test_fast_and_filtered_agree_on_deleted(tmp_path):
    """(fix 1) full read and filtered read agree on the same file."""
    p = make_dbf(
        tmp_path / "del2.dbf",
        [("X", 3)],
        [["AAA"], ["BBB"], ["CCC"]],
        deleted={1},
    )
    full = set(dbf_reader.read_dbf_fast(p)["X"])
    filt = set(
        dbf_reader.read_dbf_filtered(
            p, "X", ["AAA", "BBB", "CCC"], prefix_match=False
        )["X"]
    )
    assert full == filt == {"AAA", "CCC"}


def test_stream_skips_deleted(tmp_path):
    """(fix 1) streaming also drops deleted records."""
    fields = [("X", 3)]
    records = [[f"{i:03d}"] for i in range(10)]
    p = make_dbf(tmp_path / "delstream.dbf", fields, records, deleted={2, 7})
    df = pd.concat(
        list(dbf_reader.stream_dbf_fast(p, chunk_size=4)), ignore_index=True
    )
    ref = dbfread_rows(p)
    assert list(df["X"]) == [str(r["X"]).strip() for r in ref]
    assert len(df) == 8


def test_exact_match_shorter_than_field(tmp_path):
    """(fix 2) exact match works for values shorter than the field width."""
    p = make_dbf(
        tmp_path / "short.dbf", [("SP_NAIH", 13)], [["123"], ["456"], ["123"]]
    )
    df = dbf_reader.read_dbf_filtered(p, "SP_NAIH", ["123"], prefix_match=False)
    assert len(df) == 2


def test_exact_match_with_nul_padding(tmp_path):
    """(fix 2) exact match survives trailing NUL padding."""
    p = make_dbf(
        tmp_path / "nulexact.dbf",
        [("CID", 6)],
        [[b"G20\x00\x00\x00"], [b"Z999  "]],
    )
    df = dbf_reader.read_dbf_filtered(p, "CID", ["G20"], prefix_match=False)
    assert len(df) == 1


def test_columns_subset_case_insensitive(tmp_path):
    """(fix 4) read_dbf_fast column subset matches names case-insensitively."""
    p = make_dbf(
        tmp_path / "case.dbf", [("DIAG", 4), ("SEXO", 1)], [["G200", "1"]]
    )
    df = dbf_reader.read_dbf_fast(p, columns=["diag"])
    assert list(df.columns) == ["DIAG"]
    assert list(df["DIAG"]) == ["G200"]
