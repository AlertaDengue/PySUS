import json
import string
from difflib import get_close_matches
from pathlib import Path
from typing import Dict

import pandas as pd
import unidecode

# from loguru import logger

APP_DIR = Path(__file__).resolve(strict=True).parent.parent
CID10 = {'dengue': 'A90', 'chikungunya': 'A92.0', 'zika': 'A928'}

with open(APP_DIR / 'dataset/geocode_by_cities.json', 'r') as f:
    geocode_by_cities = json.load(f)


def normalize(s):
    for p in string.punctuation:
        s = s.replace(p, '')

    return unidecode.unidecode(s.lower().strip())


def search_string(substr: str) -> Dict[str, int]:
    """
    Fetch geocode of the city name matching to the substring.

    Parameters
    ----------
        substr: Part of city name
    Returns
    -------
        dict: Dictionary with key and values
            with city name and IBGE codes of all municipalities in Brazil
    """
    normalized_list = [normalize(f) for f in list(geocode_by_cities.keys())]

    matching_cities = [
        get_close_matches(i, normalized_list, n=55)
        for i in normalize(substr).split('.')
    ]

    return {
        key: geocode_by_cities[key]
        for key in geocode_by_cities
        if normalize(key) in list(*matching_cities)
    }


def download(
    disease: str,
    eyw_start: int,
    eyw_end: int,
    city_name: str,
    format='csv',
) -> pd.DataFrame:
    """
    Download InfoDengue API data by municipality and disease
        in the epidemiological week.

    Parameters
    ----------
        disease: Names of the diseases available in the InfoDengue System:
            dengue|chikungunya|zika
        eyw_start: Epidemiological week start
        eyw_end: Epidemiological week end
        city_name: Name of the municipalities of Brazil
        format="csv": Default data visualization format for the endpoint
    Returns
    -------
        pd: Pandas dataframe
    """

    geocode = geocode_by_cities.get(city_name)

    if disease not in CID10.keys():
        raise Exception(
            f'The diseases available are: {[k for k in CID10.keys()]}'
        )
    elif len(str(eyw_start)) != 6 or len(str(eyw_end)) != 6:
        raise Exception(
            'The epidemiological week must contain 6 digits, '
            'started in the year 2010 until 2022. Example: 202248'
        )
    elif geocode is None:
        list_of_cities = search_string(city_name)
        print(f'You must choose one of these city names: {list_of_cities}')
    else:
        s_yw = str(eyw_start)
        e_yw = str(eyw_end)
        ew_start, ey_start = s_yw[-2:], s_yw[:4]
        ew_end, ey_end = e_yw[-2:], e_yw[:4]
        url = 'https://info.dengue.mat.br/api/alertcity'
        params = (
            '&disease='
            + f'{disease}'
            + '&geocode='
            + f'{geocode}'
            + '&format='
            + f'{format}'
            + '&ew_start='
            + f'{ew_start}'
            + '&ew_end='
            + f'{ew_end}'
            + '&ey_start='
            + f'{ey_start}'
            + '&ey_end='
            + f'{ey_end}'
        )

        url_resp = '?'.join([url, params])
        return pd.read_csv(url_resp, index_col='SE').T
