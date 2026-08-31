"""Tests for pysus.info(), pysus.set_cache(), and first-run message."""


class TestSetCache:
    def test_set_cache_returns_path(self, tmp_path):
        import pysus

        target = tmp_path / "custom_cache"
        result = pysus.set_cache(target)
        assert result == target.resolve()
        assert target.exists()

    def test_set_cache_creates_parent_dirs(self, tmp_path):
        import pysus

        target = tmp_path / "a" / "b" / "c"
        result = pysus.set_cache(target)
        assert result.exists()

    def test_set_cache_with_string(self, tmp_path):
        import pysus

        result = pysus.set_cache(str(tmp_path / "str_cache"))
        assert result.exists()

    def test_set_cache_updates_global(self, tmp_path):
        import pysus

        original = pysus.CACHEPATH
        try:
            pysus.set_cache(tmp_path / "new")
            assert pysus.CACHEPATH == (tmp_path / "new").resolve()
        finally:
            pysus.CACHEPATH = original


class TestInfo:
    def test_info_prints_table(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "Name" in output
        assert "Origin" in output
        assert "Description" in output
        assert "FTP" in output
        assert "Saude" in output

    def test_info_includes_expected_datasets(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "SINAN" in output
        assert "SINASC" in output
        assert "SIM" in output

    def test_info_shows_auth_column(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "no" in output
        assert "yes" in output

    def test_info_shows_total_count(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "Total:" in output

    def test_info_shows_cache_path(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert str(pysus.CACHEPATH) in output

    def test_info_shows_origin_call_hints(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "pysus.ftp.sinan(...)" in output
        assert "pysus.dadosgov.cnes(...)" in output
        assert "pysus.saude.arboviroses(...)" in output

    def test_info_map_exception_names_to_fetchers(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "pysus.ftp.ibge(...)" in output

    def test_info_omits_hint_when_no_fetcher(self, capsys):
        import pysus

        pysus.info()
        output = capsys.readouterr().out
        assert "saude.cnes(...)" not in output
