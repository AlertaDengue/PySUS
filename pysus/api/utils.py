GEOCODE_PREFIXES = (
    "ID_MUNICIP",
    "ID_MN_RESI",
    "ID_MUNI_RE",
    "MUN_",
    "COD_MUN_",
    "CO_MUN_",
    "ID_MUNI_AT",
    "ID_MUNIC_",
)


def is_geocode_column(name: str) -> bool:
    """Check if a column name corresponds to an IBGE municipality code."""
    upper = name.upper()
    return any(upper.startswith(p) for p in GEOCODE_PREFIXES)


def add_dv(geocode: str) -> str:
    if not geocode or not str(geocode).isdigit():
        return geocode

    miscalculated = {
        "2201911": "2201919",
        "2201986": "2201988",
        "2202257": "2202251",
        "2611531": "2611533",
        "3117835": "3117836",
        "3152139": "3152131",
        "4305876": "4305871",
        "5203963": "5203962",
        "5203930": "5203939",
    }

    if len(str(geocode)) == 7:
        return miscalculated.get(str(geocode), geocode)

    if len(str(geocode)) == 6:
        weight = [1, 2, 1, 2, 1, 2]
        total = sum(
            sum(divmod(int(d) * w, 10))
            for d, w in zip(
                str(geocode),
                weight,
            )
        )
        dv = 0 if total % 10 == 0 else 10 - (total % 10)
        code = str(geocode) + str(dv)
        return miscalculated.get(code, code)

    return geocode
