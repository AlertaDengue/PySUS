import pandas as pd
from elasticsearch import Elasticsearch
import elasticsearch.helpers
import os
import time
from datetime import date

def le_esus(uf, form_out='hdf'):
    
    user = 'user-public-notificacoes'
    pwd = 'Za4qNXdyQNSa9YaA'
    index=user
    today = date.today()
    dt = today.strftime("_%d_%m_%Y")
    base = f'desc-notificacoes-esusve-{uf}'  #desc-notificacoes-esusve-
    url = f'https://{user}:{pwd}@elasticsearch-saps.saude.gov.br'
    out = f'~/dados/dados_{uf}{dt}.zip'
    # if os.path.exists(out):
    #     return # Não baixa dados que já foram baixados
    print("lendo dados para ",uf)
    es = Elasticsearch([url], send_get_body_as='POST')
    body={"query": {"match_all": {}}}
    results = elasticsearch.helpers.scan(es, query=body, index=base)
    df = pd.DataFrame.from_dict([document['_source'] for document in results])
    print("foram lidos registros",df.shape[0])
    #print("coluns:", df.columns)
    df.sintomas = df['sintomas'].str.replace(';','',) ## remove os  ;
    t0 = time.time()
    if form_out == 'hdf':
        df.to_hdf(out.split('.zip')[0]+'.h5', key=uf, mode='w', 
                  encoding='utf-8-sig', complib='zlib')
    elif form_out == 'parquet':
        df.to_parquet(out.split('.zip')[0]+'.parquet')
    else:
        df.to_csv(out, sep = ';', encoding='utf-8-sig', 
                  index = False,quoting=csv.QUOTE_NONNUMERIC,compression='zip',
                  chunksize=10000
                  )
    print (f'Tempo decorrido para salvar no formato {form_out}: {time.time()-t0} segundos.')

# rodando a funcao na lista de uf
locais = ["ro", "ac", "am", "rr", "pa", "ap", "to"]

for i in locais:
    le_esus(i, 'parquet')