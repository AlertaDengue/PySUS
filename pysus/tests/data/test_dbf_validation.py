"""Structural validation and truncation tests for the DBF readers."""

import struct
from datetime import date
from pathlib import Path

import pytest
from pysus.api.errors import DBFFormatError
from pysus.data.dbf_reader import (
    read_dbf_fast,
    read_dbf_filtered,
    read_dbf_schema,
    stream_dbf_fast,
    validate_dbf,
)


def _create_dbf(
    path: Path,
    fields,
    records,
    *,
    eof: bool = False,
    version: int = 0x03,
) -> Path:
    today = date.today()
    num_records = len(records)
    header_len = 32 + 32 * len(fields) + 1
    record_len = 1 + sum(f[2] for f in fields)

    buf = bytearray()
    buf.append(version)
    buf.extend(bytes((today.year - 1900, today.month, today.day)))
    buf.extend(struct.pack("<I", num_records))
    buf.extend(struct.pack("<H", header_len))
    buf.extend(struct.pack("<H", record_len))
    buf.extend(b"\x00" * 20)

    for name, ftype, length, decimal in fields:
        buf.extend(name.encode("ascii").ljust(11, b"\x00")[:11])
        buf.append(ord(ftype))
        buf.extend(b"\x00" * 4)
        buf.append(length)
        buf.append(decimal)
        buf.extend(b"\x00" * 14)

    buf.append(0x0D)
    for record in records:
        buf.append(0x20)
        for i, (_, _, length, _) in enumerate(fields):
            value = record[i] if i < len(record) else ""
            buf.extend(str(value)[:length].ljust(length).encode("ascii"))

    if eof:
        buf.append(0x1A)

    path.write_bytes(bytes(buf))
    return path


@pytest.fixture
def valid_dbf(tmp_path):
    return _create_dbf(
        tmp_path / "valid.dbf",
        [("NAME", "C", 10, 0)],
        [(f"row{i}",) for i in range(100)],
    )


def test_valid_without_eof(valid_dbf):
    schema = read_dbf_schema(valid_dbf)
    validate_dbf(valid_dbf, schema)
    assert len(read_dbf_fast(valid_dbf)) == 100


def test_valid_with_eof(tmp_path):
    path = _create_dbf(
        tmp_path / "eof.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",)],
        eof=True,
    )
    validate_dbf(path)
    assert len(read_dbf_fast(path)) == 1


def test_truncated_record_area(valid_dbf):
    data = valid_dbf.read_bytes()
    valid_dbf.write_bytes(data[:-5])

    with pytest.raises(DBFFormatError, match="Truncated"):
        read_dbf_fast(valid_dbf)


def test_truncated_header(tmp_path):
    path = _create_dbf(
        tmp_path / "truncated_header.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",)],
    )
    data = path.read_bytes()
    path.write_bytes(data[:40])

    with pytest.raises(DBFFormatError, match="header"):
        validate_dbf(path)


def test_misaligned_trailing_bytes(valid_dbf):
    with open(valid_dbf, "ab") as fh:
        fh.write(b"x")

    with pytest.raises(DBFFormatError, match="multiple"):
        validate_dbf(valid_dbf)


def test_missing_field_terminator(tmp_path):
    path = _create_dbf(
        tmp_path / "no_terminator.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",)],
    )
    data = bytearray(path.read_bytes())
    data[64] = 0x00
    path.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="field descriptor"):
        validate_dbf(path)


def test_unsupported_version(tmp_path):
    path = _create_dbf(
        tmp_path / "bad_version.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",)],
        version=0x99,
    )

    with pytest.raises(DBFFormatError, match="Not a valid dBASE"):
        validate_dbf(path)


def test_invalid_delete_flag(valid_dbf):
    data = bytearray(valid_dbf.read_bytes())
    schema = read_dbf_schema(valid_dbf)
    data[schema.header_len] = ord("?")
    valid_dbf.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="deletion flag"):
        validate_dbf(valid_dbf)


def test_strict_checks_every_delete_flag(tmp_path):
    path = _create_dbf(
        tmp_path / "bad_middle.dbf",
        [("NAME", "C", 10, 0)],
        [(f"row{i}",) for i in range(100)],
    )
    schema = read_dbf_schema(path)
    data = bytearray(path.read_bytes())
    data[schema.header_len + 50 * schema.record_len] = ord("?")
    path.write_bytes(bytes(data))

    # The sampled default check intentionally does not scan every row.
    validate_dbf(path)
    with pytest.raises(DBFFormatError, match="deletion flag"):
        validate_dbf(path, strict=True)


def test_truncated_stream_raises_before_first_chunk(valid_dbf):
    data = valid_dbf.read_bytes()
    valid_dbf.write_bytes(data[:-5])

    with pytest.raises(DBFFormatError):
        next(stream_dbf_fast(valid_dbf, chunk_size=10))


def test_truncated_filtered_read_raises(valid_dbf):
    data = valid_dbf.read_bytes()
    valid_dbf.write_bytes(data[:-5])

    with pytest.raises(DBFFormatError):
        read_dbf_filtered(valid_dbf, column="NAME", values=["row0"])


def test_field_lengths_must_match_record_len(tmp_path):
    path = _create_dbf(
        tmp_path / "bad_record_len.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",), ("row1",)],
    )
    schema = read_dbf_schema(path)
    data = path.read_bytes()
    header, records = data[: schema.header_len], data[schema.header_len :]

    new_record_len = schema.record_len + 1
    new_records = bytearray()
    for i in range(schema.num_records):
        start = i * schema.record_len
        new_records.extend(records[start : start + schema.record_len])
        new_records.append(0)

    new_data = bytearray(header + new_records)
    struct.pack_into("<H", new_data, 10, new_record_len)
    path.write_bytes(bytes(new_data))

    with pytest.raises(DBFFormatError, match="field lengths sum"):
        validate_dbf(path)


@pytest.mark.parametrize("padding", [b"\x0d", b"\x00"])
def test_header_terminator_with_one_padding_byte(tmp_path, padding):
    path = _create_dbf(
        tmp_path / "header_padding.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",), ("row1",)],
    )
    schema = read_dbf_schema(path)
    data = path.read_bytes()
    header, records = data[: schema.header_len], data[schema.header_len :]

    new_header = bytearray(header + padding)
    struct.pack_into("<H", new_header, 8, schema.header_len + len(padding))
    path.write_bytes(bytes(new_header) + records)

    validate_dbf(path)
    assert len(read_dbf_fast(path)) == 2


def test_truncation_at_every_record_byte_raises(valid_dbf):
    data = valid_dbf.read_bytes()
    schema = read_dbf_schema(valid_dbf)
    expected = schema.header_len + schema.num_records * schema.record_len
    for cut in range(schema.header_len, expected):
        valid_dbf.write_bytes(data[:cut])
        with pytest.raises(DBFFormatError):
            read_dbf_fast(valid_dbf)


def test_full_size_and_optional_eof_are_valid(valid_dbf):
    data = valid_dbf.read_bytes()
    schema = read_dbf_schema(valid_dbf)
    expected = schema.header_len + schema.num_records * schema.record_len

    valid_dbf.write_bytes(data[:expected])
    assert len(read_dbf_fast(valid_dbf)) == schema.num_records

    valid_dbf.write_bytes(data[:expected] + b"\x1a")
    assert len(read_dbf_fast(valid_dbf)) == schema.num_records


def test_garbage_after_eof_is_rejected(valid_dbf):
    data = valid_dbf.read_bytes()
    schema = read_dbf_schema(valid_dbf)
    expected = schema.header_len + schema.num_records * schema.record_len
    valid_dbf.write_bytes(data[:expected] + b"\x1a" + b"x")

    with pytest.raises(DBFFormatError, match="multiple"):
        validate_dbf(valid_dbf)


def test_extra_aligned_record_is_accepted(valid_dbf):
    schema = read_dbf_schema(valid_dbf)
    data = valid_dbf.read_bytes()
    valid_dbf.write_bytes(data + b" " + b"0" * (schema.record_len - 1))

    validate_dbf(valid_dbf)
    # The header still declares the original row count.
    assert len(read_dbf_fast(valid_dbf)) == schema.num_records


def test_num_records_too_high_is_truncated(valid_dbf):
    data = bytearray(valid_dbf.read_bytes())
    schema = read_dbf_schema(valid_dbf)
    struct.pack_into("<I", data, 4, schema.num_records + 1)
    valid_dbf.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="Truncated"):
        validate_dbf(valid_dbf)


def test_num_records_too_low_reads_declared_rows(valid_dbf):
    data = bytearray(valid_dbf.read_bytes())
    schema = read_dbf_schema(valid_dbf)
    struct.pack_into("<I", data, 4, schema.num_records - 1)
    valid_dbf.write_bytes(bytes(data))

    validate_dbf(valid_dbf)
    assert len(read_dbf_fast(valid_dbf)) == schema.num_records - 1


def test_deleted_records_are_valid_and_skipped(tmp_path):
    path = _create_dbf(
        tmp_path / "deleted.dbf",
        [("NAME", "C", 10, 0)],
        [("row0",), ("row1",), ("row2",)],
    )
    schema = read_dbf_schema(path)
    data = bytearray(path.read_bytes())
    data[schema.header_len + schema.record_len] = ord("*")
    path.write_bytes(bytes(data))

    validate_dbf(path)
    assert read_dbf_fast(path)["NAME"].tolist() == ["row0", "row2"]


def test_zero_record_file_is_valid(tmp_path):
    path = _create_dbf(
        tmp_path / "zero.dbf", [("NAME", "C", 10, 0)], [], eof=True
    )
    validate_dbf(path)
    assert read_dbf_fast(path).empty
    assert list(stream_dbf_fast(path)) == []


def test_zero_record_len_is_rejected(tmp_path):
    path = _create_dbf(tmp_path / "zero_rl.dbf", [("NAME", "C", 10, 0)], [])
    data = bytearray(path.read_bytes())
    struct.pack_into("<H", data, 10, 0)
    path.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="record length"):
        validate_dbf(path)


def test_header_len_too_small_is_rejected(tmp_path):
    path = _create_dbf(tmp_path / "small_hl.dbf", [("NAME", "C", 10, 0)], [])
    data = bytearray(path.read_bytes())
    struct.pack_into("<H", data, 8, 10)
    path.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="header length"):
        validate_dbf(path)


def test_header_len_beyond_file_is_rejected(tmp_path):
    path = _create_dbf(tmp_path / "big_hl.dbf", [("NAME", "C", 10, 0)], [])
    data = bytearray(path.read_bytes())
    struct.pack_into("<H", data, 8, len(data) + 100)
    path.write_bytes(bytes(data))

    with pytest.raises(DBFFormatError, match="header length"):
        validate_dbf(path)


def test_short_field_descriptor_read_is_rejected(tmp_path, monkeypatch):
    from types import SimpleNamespace

    path = tmp_path / "short_header.dbf"
    buf = bytearray(40)
    buf[0] = 0x03
    struct.pack_into("<IHH", buf, 4, 0, 65, 11)
    path.write_bytes(bytes(buf))

    real_stat = Path.stat

    def fake_stat(self):
        return SimpleNamespace(st_size=real_stat(self).st_size + 100)

    monkeypatch.setattr(Path, "stat", fake_stat)
    with pytest.raises(DBFFormatError, match="Truncated DBF header"):
        validate_dbf(path)
