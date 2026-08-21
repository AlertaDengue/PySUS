"""Tests for flatten_json_columns and to_english (Phase 1.2)."""

import pandas as pd


class TestFlattenJsonColumns:
    def test_empty_df_returns_copy(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame()
        result = flatten_json_columns(df)
        assert result.empty
        assert result is not df

    def test_no_json_columns_unchanged(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame({"name": ["Alice", "Bob"], "age": [30, 40]})
        result = flatten_json_columns(df)
        assert list(result.columns) == ["name", "age"]

    def test_flattens_json_column(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame(
            {"meta": ['{"city": "SP", "region": "Sudeste"}', '{"city": "RJ"}']}
        )
        result = flatten_json_columns(df, columns=["meta"])
        assert "city" in result.columns
        assert "region" in result.columns
        assert "meta" not in result.columns
        assert result["city"].iloc[0] == "SP"
        assert pd.isna(result["region"].iloc[1])

    def test_auto_detects_json_columns(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "info": ['{"x": 1}', '{"x": 2}'],
            }
        )
        result = flatten_json_columns(df)
        assert "x" in result.columns

    def test_mixed_json_and_non_json(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame(
            {
                "data": ['{"a": 1}', "not json", '{"a": 3}'],
            }
        )
        result = flatten_json_columns(df, columns=["data"])
        assert "a" in result.columns
        # non-JSON row should still exist
        assert len(result) == 3

    def test_preserves_other_columns(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame(
            {
                "id": [1, 2],
                "nested": ['{"k": "v"}', '{"k": "w"}'],
            }
        )
        result = flatten_json_columns(df, columns=["nested"])
        assert "id" in result.columns
        assert "k" in result.columns

    def test_specified_column_not_in_df(self):
        from pysus.api.flatten import flatten_json_columns

        df = pd.DataFrame({"a": [1, 2]})
        result = flatten_json_columns(df, columns=["nonexistent"])
        assert list(result.columns) == ["a"]


class TestToEnglish:
    def test_renames_known_columns(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame({"DT_NOTIFIC": ["2024-01-01"], "SG_UF": ["SP"]})
        result = to_english(df)
        assert "notification_date" in result.columns
        assert "state" in result.columns

    def test_unknown_columns_unchanged(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame({"CUSTOM_COL": [1, 2]})
        result = to_english(df)
        assert "CUSTOM_COL" in result.columns

    def test_stores_alias_mapping(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame({"DT_NOTIFIC": ["2024-01-01"]})
        result = to_english(df)
        assert "aliases" in result.attrs
        assert result.attrs["aliases"]["DT_NOTIFIC"] == "notification_date"

    def test_store_mapping_false(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame({"DT_NOTIFIC": ["2024-01-01"]})
        result = to_english(df, store_mapping=False)
        assert "aliases" not in result.attrs

    def test_empty_df(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame()
        result = to_english(df)
        assert result.empty

    def test_no_matching_columns(self):
        from pysus.api.mappings import to_english

        df = pd.DataFrame({"foo": [1], "bar": [2]})
        result = to_english(df)
        assert list(result.columns) == ["foo", "bar"]
        assert result.attrs.get("aliases") is None

    def test_top_level_export(self):
        import pysus

        assert hasattr(pysus, "flatten_json_columns")
        assert hasattr(pysus, "to_english")


class TestMappingCompleteness:
    def test_common_sinan_columns_mapped(self):
        from pysus.api.mappings import PT_TO_EN

        for col in [
            "DT_NOTIFIC",
            "DT_SIN_PRI",
            "DT_NASC",
            "CS_SEXO",
            "IDADE",
            "SG_UF",
            "ID_MUNICIP",
            "CLASSI_FIN",
            "EVOLUCAO",
            "DT_OBITO",
        ]:
            assert col in PT_TO_EN, f"{col} not in PT_TO_EN"

    def test_all_values_are_snake_case(self):
        from pysus.api.mappings import PT_TO_EN

        for pt, en in PT_TO_EN.items():
            assert " " not in en, f"Value for {pt} has spaces: {en}"
            assert en == en.lower(), f"Value for {pt} not lowercase: {en}"
