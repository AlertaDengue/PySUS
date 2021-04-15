import pandas as pd
from pysus.online_data import CACHEPATH
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import os
import time
from datetime import date

def download(uf, cache=True):
    """
    Download ESUS data by UF
    :param uf: rj, mg, etc
    :param cache: if results should be cached on disk
    :return:
    """
    uf = uf.lower()
    user = 'user-public-notificacoes'
    pwd = 'Za4qNXdyQNSa9YaA'
    today = date.today()
    dt = today.strftime("_%d_%m_%Y")
    base = f'desc-notificacoes-esusve-{uf}'  #desc-notificacoes-esusve-
    url = f'https://{user}:{pwd}@elasticsearch-saps.saude.gov.br'
    out = f'ESUS_{uf}_{dt}.parquet'

    cachefile = os.path.join(CACHEPATH, 'ESUS_' + out)
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
    else:
        df = fetch(base, uf, url)
        if cache:
            df.to_parquet(cachefile)

    return df



def fetch(base, uf, url):
    print(f"Reading ESUS data for {uf.upper()}")
    es = Elasticsearch([url], send_get_body_as='POST')
    body = {"query": {"match_all": {}}}
    results = elasticsearch.helpers.scan(es, query=body, index=base)
    df = pd.DataFrame.from_dict([document['_source'] for document in results])
    print(f"{df.shape[0]} records downloaded.")
    df.sintomas = df['sintomas'].str.replace(';', '', )  ## remove os  ;
    return df