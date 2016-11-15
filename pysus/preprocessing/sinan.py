from dbfread import DBF
import pandas as pd
import requests
import geocoder
from functools import lru_cache
import os


def read_sinan_dbf(fname, encoding) -> pd.DataFrame:
    """
    Read SINAN dbf file returning a Pandas Dataframe with
    :param fname: dbf file name
    :param encoding: Encoding of the dbf
    :return: pandas dataframe
    """
    db = DBF(fname, encoding=encoding)
    df = pd.DataFrame(list(db))

    def convert_week(x):
        try:
            w = int(x) % 100
        except ValueError:
            w = pd.np.nan
        return w
    for cname in df.columns:
        df[cname].replace('', pd.np.nan, inplace=True)
        if cname.startswith(('NU', 'ID')):
            try:
                df[cname] = pd.to_numeric(df[cname])
            except ValueError as e:
                # certain IDs can be alphanumerical
                pass
        elif cname.startswith('SEM'):
            df[cname] = df[cname].map(convert_week)

    return df


@lru_cache(maxsize=None)
def get_geocodes(geoc):
    """
    Return city name and state two letter code from geocode
    :param geoc:
    :return:
    """
    url = "http://cidades.ibge.gov.br/services/jSonpMuns.php?busca=330&featureClass=P&style=full&maxRows=5&name_startsWith={}".format(
        geoc)
    resp = requests.get(url)
    for d in resp.json()['municipios']:
        if int(geoc) == int(d['c']):
            return [d['n'].encode('latin-1').decode('utf-8'), d['s']]

    else:
        raise KeyError('could not find geocode {} in '.format(geoc))


def _address_generator(df, default=''):
    for l in df.iterrows():
        l = dict(l[1])
        try:
            l['cidade'] = ','.join(get_geocodes(l['ID_MN_RESI']))
        except:
            print("Could not find geocode {} using default")
            l['cidade'] = default
        yield l['NU_NOTIFIC'], "{NM_LOGRADO}, {NU_NUMERO}, {NM_BAIRRO}, {cidade}, Brasil".format(**l)


def geocode(sinan_df, outfile, default_city):
    """
    Geocode cases based on addresses included.
    :param default_city: default city to use in case of bad Geocode found in file. It can be "city, state"
    :param sinan_df: Dataframe generated from sinan DBF
    :param outfile: File on Which
    """
    addrs = _address_generator(sinan_df, default_city)
    if os.path.exists(outfile):
        mode = 'a'
        coords = pd.read_csv(outfile)
        geocoded = coords.NU_NOTIFIC.tolist()
    else:
        mode = 'w'
        geocoded = []
    with open(outfile, mode) as of:
        if mode == 'w':
            of.write("NU_NOTIFIC,latitude,longitude\n")
        for nu, ad in addrs:
            # ad = ad.encode('latin-1').decode('utf-8')
            if nu in geocoded:
                continue
            location = geocoder.google(ad)
            if location is None:
                raise NameError("Google could not find {}".format(ad))
            if location.latlng == []:
                print("Search for {} returned {} as coordinates, trying reduced address:".format(ad, location.latlng))
                ad = ','.join(ad.split(',')[2:])
                print(ad)
                location = geocoder.google(ad)
            try:
                of.write("{},{},{}\n".format(nu, location.latlng[0], location.latlng[1]))
                print("Successfully geolocated {}".format(ad))
            except IndexError:
                print("Search for {} returned {} as coordinates, skipping".format(ad, location.latlng))
                of.write("{},nan,nan\n".format(nu))
