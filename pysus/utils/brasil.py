__all__ = [
    "MUNICIPALITIES",
    "MUN_BY_GEOCODE",
    "UFs",
    "MONTHS",
    "get_city_name_by_geocode",
    "parse_UFs",
]

import json
from pathlib import Path
from typing import Union

from pysus.utils import to_list

with open(
    f"{Path(__file__).parent}/municipios.json", "r", encoding="utf-8-sig"
) as muns:
    MUNICIPALITIES = json.loads(muns.read())

MUN_BY_GEOCODE = {mun["geocodigo"]: mun["municipio"] for mun in MUNICIPALITIES}


UFs = {
    "BR": "Brasil",
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
    "DF": "Distrito Federal",
}

MONTHS = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}


def get_city_name_by_geocode(geocode: Union[str, int]):
    """
    Returns the Municipality name from its geocode (IBGE)
    :param geocode: 7 digits city code, according to IBGE format
    :return: City name
    """

    return MUN_BY_GEOCODE[int(geocode)]


def parse_UFs(UF: Union[list[str], str]) -> list:
    """
    Formats states abbreviations into correct format and retuns a list.
    Also checks if there is an incorrect UF in the list.
    E.g: ['SC', 'mt', 'ba'] -> ['SC', 'MT', 'BA']
    """
    ufs = [uf.upper() for uf in to_list(UF)]
    if not all(uf in list(UFs) for uf in ufs):
        raise ValueError(f"Unknown UF(s): {set(ufs).difference(list(UFs))}")
    return ufs
