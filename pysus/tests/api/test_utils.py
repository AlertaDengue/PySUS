from pysus.api.utils import add_dv, is_geocode_column


def test_is_geocode_column_true():
    assert is_geocode_column("ID_MUNICIP") is True
    assert is_geocode_column("ID_MN_RESI") is True
    assert is_geocode_column("MUN_ACID") is True
    assert is_geocode_column("COD_MUN_HO") is True
    assert is_geocode_column("CO_MUN_EXP") is True
    assert is_geocode_column("ID_MUNI_AT") is True
    assert is_geocode_column("ID_MUNIC_A") is True
    assert is_geocode_column("ID_MUNI_RE") is True


def test_is_geocode_column_false():
    assert is_geocode_column("DT_NOTIFIC") is False
    assert is_geocode_column("SG_UF") is False
    assert is_geocode_column("NM_PACIENT") is False
    assert is_geocode_column("CS_SEXO") is False
    assert is_geocode_column("") is False


def test_add_dv_6digit():
    assert add_dv("261160") == "2611606"


def test_add_dv_7digit_already_has_dv():
    assert add_dv("2611606") == "2611606"


def test_add_dv_miscalculated():
    assert add_dv("2201911") == "2201919"


def test_add_dv_none():
    assert add_dv(None) is None


def test_add_dv_empty():
    assert add_dv("") == ""


def test_add_dv_non_digit():
    assert add_dv("abc") == "abc"


def test_add_dv_5digit_returns_as_is():
    assert add_dv("12345") == "12345"


def test_add_dv_8digit_returns_as_is():
    assert add_dv("12345678") == "12345678"
