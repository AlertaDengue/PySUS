from pysus.api.types import FileType, State


class TestFileType:
    def test_file_types_are_valid(self):
        valid_types: list[FileType] = [
            "FILE",
            "DIR",
            "PARQUET",
            "CSV",
            "JSON",
            "PDF",
            "DBC",
            "DBF",
            "ZIP",
        ]
        for ft in valid_types:
            assert ft in FileType.__args__


class TestState:
    def test_all_brazilian_states_present(self):
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
        actual_states = set(State.__args__)  # type: ignore
        assert actual_states == expected_states
