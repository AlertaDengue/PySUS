#-*- coding:utf-8 -*-
u"""
This module contains a set of functions to deal with geodatadecode
commonly encoded variables
Created on 15/10/2020
by gabrielmcf
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'
from pysus.online_data.SIM import download
from pysus.preprocessing.decoders import translate_variables_SIM
from pysus.preprocessing.SIM import group_and_count, redistribute
from geobr import read_municipality
import pandas as pd

def add_data_to_municipality(  
                        counts,
                        map_year=2019,
                        codmun_col='CODMUNRES',
                        title_cols=['SEXO','IDADE_ANOS'],
                        value_col='CONTAGEM'):

    """
    Adiciona dados de mortalidade aos seus respectivos municípios. Gera um GeoDataFrame do GeoPandas.
    :param counts: dataframe contendo os dados a serem agregados.
    :param map_year: ano do mapa a ser usado (biblioteca geobr).
    :param codmun_col: coluna com geocode do município
    :param title_cols: colunas que serão utilizadas para formar o título das colunas no GeoDataFrame.
    :param value_col: coluna com o valor a ser adicionado ao GeoDataFrame
    :return:
    """

    # Extrai código do estado dos municípios.
    # 2 primeiros dígitos do código são o estado
    states = counts[counts[codmun_col] != 'nan'][codmun_col].apply(lambda x: str(x)[:2]).unique()
    geo_df = read_municipality(code_muni=states[0],year=map_year)
    
    if(len(states) > 1):
        for state in states[1:]:
            geo_df = geo_df.append(read_municipality(code_muni=state,year=map_year))

    column_names = column_name_list(counts,title_cols)
    geo_df[column_names] = 0

    for i, mun in geo_df.iterrows():
        data = counts[(counts[codmun_col] == mun['code_muni']) & (counts[value_col] > 0.0)]
        for _, item in data.iterrows():
            geo_df.loc[i, column_name(item,title_cols)] = item[value_col]

    return geo_df.fillna(0)

def column_name_list(df,title_cols):
    unique_indices = pd.DataFrame(df.groupby(title_cols).indices.keys())
    titles = unique_indices.apply(column_name,axis=1)
    return titles.tolist()

def column_name(item,title_cols=None):
    if(title_cols):
        name = item[title_cols[0]]
        if(len(title_cols) > 1):
            for col in title_cols[1:]:
                name = f"{name}-{item[col]}"
    else:
        name = item[0]
        if(len(item) > 1):
            for col in item[1:]:
                name = f"{name}-{col}"
        
    return name