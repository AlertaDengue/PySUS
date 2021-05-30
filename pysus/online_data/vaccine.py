"""
Download of vacination data.

This module contains function to download from specific campains:

- COVID-19 in 2020-2021 Downloaded as described [here](http://opendatasus.saude.gov.br/dataset/b772ee55-07cd-44d8-958f-b12edd004e0b/resource/5916b3a4-81e7-4ad5-adb6-b884ff198dc1/download/manual_api_vacina_covid-19.pdf)
"""
import pandas as pd
from pysus.online_data import CACHEPATH
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import requests
from requests.auth import HTTPBasicAuth
import os
import time
import json
from datetime import date



def download_covid(uf):
    UF = uf.upper()
    user = 'imunizacao_public'
    pwd = 'qlto5t&7r_@+#Tlstigi'
    index = "desc-imunizacao"
    url = f"https://imunizacao-es.saude.gov.br/_search"
    query={"query": {"match_all": {"paciente_endereco_uf": UF}}}
    # es = Elasticsearch([url], send_get_body_as='POST', headers=)
    auth = HTTPBasicAuth(user, pwd)
    es = elasticsearch_fetch(url, auth)#, json.dumps(query))
    return es


def elasticsearch_fetch(uri, auth, json_body='', verb='post'):
    headers = {
        'Content-Type': 'application/json',
    }

    try:
        # make HTTP verb parameter case-insensitive by converting to lower()
        if verb.lower() == "get":
            resp = requests.get(uri, auth=auth, headers=headers, data=json_body)
        elif verb.lower() == "post":
            resp = requests.post(uri, auth=auth, headers=headers, data=json_body)
        elif verb.lower() == "put":
            resp = requests.put(uri, auth=auth, headers=headers, data=json_body)

        # read the text object string
        try:
            resp_text = json.loads(resp.text)
        except:
            resp_text = resp.text

        # catch exceptions and print errors to terminal
    except Exception as error:
        print ('\nelasticsearch_curl() error:', error)
        resp_text = error

    # return the Python dict of the request
    print ("resp_text:", resp_text)
    return resp_text

if __name__ == "__main__":
    print (download_covid('ba'))
