from pydantic import TypeAdapter
from pysus.api.types import (
    CSV,
    DBC,
    DBF,
    DIR,
    FILE,
    JSON,
    PARQUET,
    PDF,
    ZIP,
    FileType,
    State,
)


class TestFileType:
    def test_file_types_are_valid(self):
        adapter = TypeAdapter(FileType)
        valid_types = [FILE, DIR, PARQUET, CSV, JSON, PDF, DBC, DBF, ZIP]
        for ft in valid_types:
            assert adapter.validate_python(ft) == ft

    def test_invalid_file_type_raises(self):
        adapter = TypeAdapter(FileType)
        try:
            adapter.validate_python("INVALID")
            assert False, "Should have raised"
        except Exception:
            pass


class TestState:
    def test_all_brazilian_states_present(self):
        adapter = TypeAdapter(State)
        expected_states = {
            "AC",
            "AL",
            "AP",
            "AM",
            "BA",
            "CE",
            "ES",
            "GO",
            "MA",
            "MT",
            "MS",
            "MG",
            "PA",
            "PB",
            "PR",
            "PE",
            "PI",
            "RJ",
            "RN",
            "RS",
            "RO",
            "RR",
            "SC",
            "SP",
            "SE",
            "TO",
            "DF",
        }
        for state in expected_states:
            adapter.validate_python(state)

    def test_invalid_state_raises(self):
        adapter = TypeAdapter(State)
        try:
            adapter.validate_python("XX")
            assert False, "Should have raised"
        except Exception:
            pass
