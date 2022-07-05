import os
import time
from datetime import date

import dask.dataframe as dd
import elasticsearch.helpers
import pandas as pd
from elasticsearch import Elasticsearch

from pysus.online_data import CACHEPATH


def download(uf, cache=True, checkmemory=True):
    """
    Download ESUS data by UF
    :param uf: rj, mg, etc
    :param cache: if results should be cached on disk
    :return: DataFrame if data fits in memory, other an iterator of chunks of size 1000.
    """
    uf = uf.lower()
    user = "user-public-notificacoes"
    pwd = "Za4qNXdyQNSa9YaA"
    today = date.today()
    dt = today.strftime("_%d_%m_%Y")
    base = f"desc-esus-notifica-estado-{uf}"  # desc-notificacoes-esusve-
    url = f"https://{user}:{pwd}@elasticsearch-saps.saude.gov.br"
    out = f"ESUS_{uf}_{dt}.parquet"

    cachefile = os.path.join(CACHEPATH, out)
    tempfile = os.path.join(CACHEPATH, f"ESUS_temp_{uf.upper()}.csv.gz")
    if os.path.exists(cachefile):
        df = pd.read_parquet(cachefile)
    elif os.path.exists(tempfile):
        df = pd.read_csv(tempfile, chunksize=1000)
    else:
        fname = fetch(base, uf, url)
        size = os.stat(fname).st_size
        if size > 50e6 and checkmemory:
            print(f"Downloaded data is to large:{size / 1e6} MB compressed.")
            print(
                "Only loading the first 1000 rows. If your computer has enough memory, set 'checkmemory' to False"
            )
            print(f"The full data is in {fname}")
            df = pd.read_csv(fname, chunksize=1000)
        else:
            df = pd.read_csv(fname, low_memory=False)
            print(f"{df.shape[0]} records downloaded.")
            os.unlink(fname)
            if cache:
                df.to_parquet(cachefile)

    return df


def fetch(base, uf, url):
    UF = uf.upper()
    print(f"Reading ESUS data for {UF}")
    es = Elasticsearch([url], send_get_body_as="POST")
    body = {"query": {"match_all": {}}}
    results = elasticsearch.helpers.scan(es, query=body, index=base)
    # df = pd.DataFrame.from_dict([document['_source'] for document in results])

    chunker = chunky_fetch(results, 3000)
    h = 1
    tempfile = os.path.join(CACHEPATH, f"ESUS_temp_{UF}.csv.gz")
    for ch in chunker:
        df = pd.DataFrame.from_dict(ch)
        df.sintomas = df["sintomas"].str.replace(
            ";",
            "",
        )  ## remove os  ;
        if h:
            df.to_csv(tempfile)
            h = 0
        else:
            df.to_csv(tempfile, mode="a", header=False)
    # df = pd.read_csv('temp.csv.gz')

    return tempfile


def chunky_fetch(results, chunk_size=3000):
    "Fetches data in chunks to preserve memory"
    data = []
    i = 0
    for d in results:
        data.append(d["_source"])
        i += 1
        if i == chunk_size:
            yield data
            data = []
            i = 0
    else:
        return data
