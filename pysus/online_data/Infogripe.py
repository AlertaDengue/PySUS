"""
Downloads data made available by the Infogripe service
"""

import pandas as pd

BASEURL = r'https://gitlab.fiocruz.br/marcelo.gomes/infogripe/-/raw/master/Dados/InfoGripe/'
DATASETS = {
    'Alerta de situação': r'tabela_de_alerta.csv',
    'Casos por idade, sexo e virus': r'dados_semanais_faixa_etaria_sexo_virus.csv.gz',
    'Casos Totais e estimativas': r'serie_temporal_com_estimativas_recentes.csv.gz',
    'Valores esperados por localidades': 'valores_esperados_por_localidade.csv',
}


def list_datasets():
    return list(DATASETS.keys())


def download(dataset_name):
    url = BASEURL + DATASETS[dataset_name]
    df = pd.read_csv(url, delimiter=';', decimal=',')
    return df
