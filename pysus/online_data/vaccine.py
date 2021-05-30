"""
Download of vacination data.

This module contains function to download from specific campains:

- COVID-19 in 2020-2021 Downloaded as described [here](http://opendatasus.saude.gov.br/dataset/b772ee55-07cd-44d8-958f-b12edd004e0b/resource/5916b3a4-81e7-4ad5-adb6-b884ff198dc1/download/manual_api_vacina_covid-19.pdf)
"""
import pandas as pd
from pysus.online_data import CACHEPATH
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import os
import time
from datetime import date



def download_covid(uf):
    UF = uf.upper()
    user = 'imunizacao_public'
    pwd = 'qlto5t&7r_@+#Tlstigi'
    index = "desc-imunizacao"
    url = f"https://{user}:{pwd}@imunizacao-es.saude.gov.br/_search"
    query={"query": {"match_all": {"state": UF}}}
    es = Elasticsearch([url], send_get_body_as='POST', headers=)
    return es


if __name__ == "__main__":
    pass
