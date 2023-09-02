"""
Download of vacination data.

This module contains function to download from specific campains:

- COVID-19 in 2020-2021 Downloaded as described [here](http://opendatasus.saude.gov.br/dataset/b772ee55-07cd-44d8-958f-b12edd004e0b/resource/5916b3a4-81e7-4ad5-adb6-b884ff198dc1/download/manual_api_vacina_covid-19.pdf)
"""
import os
import json
import requests
import pandas as pd

from loguru import logger
from json import JSONDecodeError
from requests.auth import HTTPBasicAuth

from pysus.ftp import CACHEPATH


def download_covid(uf=None, only_header=False):
    """
    Download covid vaccination data for a give UF
    :param uf: 'RJ' | 'SP', etc.
    :param only_header: Used to see the header of the data before downloading.
    :return: dataframe iterator as returned by pandas `read_csv('Vaccine_temp_<uf>.csv.gz', chunksize=5000)`
    """
    user = 'imunizacao_public'
    pwd = 'qlto5t&7r_@+#Tlstigi'
    index = 'desc-imunizacao'
    url = f'https://imunizacao-es.saude.gov.br/_search?scroll=1m'
    if uf is None:
        query = {'query': {'match_all': {}}, 'size': 10000}
        UF = 'BR'
    else:
        UF = uf.upper()
        query = {
            'query': {'match': {'paciente_endereco_uf': UF}},
            'size': 10000,
        }

    logger.info(f'Searching for COVID data of {UF}')
    tempfile = os.path.join(CACHEPATH, f'Vaccine_temp_{UF}.csv.gz')
    if os.path.exists(tempfile):
        print(
            'loading from cache. Returning an iterator of Dataframes in chunks of 5000.'
        )
        return pd.read_csv(tempfile, chunksize=5000)

    auth = HTTPBasicAuth(user, pwd)
    data_gen = elasticsearch_fetch(url, auth, query)

    if only_header:
        df = pd.DataFrame(next(data_gen))
        logger.warning(
            f'Downloading data sample for visualization of {df.shape[0]} rows...'
        )
        return df

    h = 1
    for dt in data_gen:
        df = pd.DataFrame(dt)
        if h:
            df.to_csv(tempfile)
            h = 0
        else:
            df.to_csv(tempfile, mode='a', header=False)

    logger.info(f'{tempfile} stored at {CACHEPATH}.')
    df = pd.read_csv(tempfile, chunksize=5000)

    return df


def elasticsearch_fetch(uri, auth, json_body={}):
    headers = {
        'Content-Type': 'application/json',
    }

    scroll_id = ''
    total = 0
    while True:
        if scroll_id:
            uri = 'https://imunizacao-es.saude.gov.br/_search/scroll'
            json_body['scroll_id'] = scroll_id
            json_body['scroll'] = '1m'
            if 'query' in json_body:
                del json_body[
                    'query'
                ]  # for the continuation of the download, query parameter is not allowed
                del json_body['size']
        try:
            s = requests.Session()
            response = s.post(uri, auth=auth, headers=headers, json=json_body)
            text = response.text
            try:
                resp = json.loads(text)
            except JSONDecodeError:
                resp = text
        except Exception as error:
            print('\nelasticsearch_fetch() error:', error)
            raise error
        try:
            if resp['hits']['hits'] == []:
                break
        except KeyError as e:
            logger.error(e)
            print(resp)
        total += len(resp['hits']['hits'])
        print(f'Downloaded {total} records\r', end='')
        # print(resp)
        # print(uri)
        yield [h['_source'] for h in resp['hits']['hits']]
        if '_scroll_id' in resp:
            scroll_id = resp['_scroll_id']


if __name__ == '__main__':
    print(download_covid('ba', only_header=True))
