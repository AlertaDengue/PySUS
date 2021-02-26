#-*- coding:utf-8 -*-
u"""
This module contains a set of functions to process data on SIM
Created on 16/08/2020
by gabrielmcf
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'
import numpy as np
from itertools import product

def logical_and_from_dict(dataframe, dictionary):
    if dictionary == {}:
        return np.array([True] * len(dataframe), dtype=bool)
    return np.logical_and.reduce([dataframe[k] == v for k,v in dictionary.items()])

def relax_filter(dictionary,fields):
    for field in reversed(fields):
        if field in dictionary:
            del dictionary[field]
            break
    return dictionary

def group_and_count(dataframe,variables):
    """
    Agrupa e conta as variáveis passadas como parâmetro no dataframe. Cria uma nova
    coluna chamada CONTAGEM, com o tipo float64 (para possibilitar redistribuição pro rata posterior).
    :param dataframe: dataframe pandas
    :param variables: lista de string contendo o nome das colunas a serem agrupadas no dataframe.
    :return:
    """
    df = dataframe

    # No pandas 1.1.0 será possível usar o argumento dropna=False, e evitar de converter NaN em uma categoria no translate_variables_SIM
    counts = df.groupby(variables).size().reset_index(name='CONTAGEM')
    counts["CONTAGEM"] = counts["CONTAGEM"].astype('float64')

    return counts

def redistribute_missing(counts,variables):
    """
    Realiza redistribuição pro rata das contagens com algum dado faltante.
    O dataframe deve conter uma coluna float64 chamada CONTAGEM e as demais colunas devem ser
    do tipo category, tendo os dados faltantes em uma categoria chamada 'nan'.
    :param counts: dataframe pandas contendo coluna com soma chamada CONTAGEM
    :param variables: variáveis a serem consideradas para filtro de redistribuição pro rata
    :return:
    """
    # Adiciona array de trues para as colunas faltantes nas variáveis
    missing_columns_count = len(counts.columns) - len(variables) - 1

    # Removendo categorias faltantes vazias
    for var in variables:
        condition_dict = {
            var: 'nan',
            'CONTAGEM': 0.0
        }
        counts = counts[~logical_and_from_dict(counts,condition_dict)]

    ### Dataframes de dados faltantes

    variables_dict = [{x: 'nan'} for x in variables]

    variables_condition = [logical_and_from_dict(counts,x) for x in variables_dict]

    # Primeiro item da tupla é != nan, segundo é o == nan
    variables_tuples = [(np.logical_not(x),x) for x in variables_condition]
    variables_product = list(product(*variables_tuples))

    # Remove regra de todos != nan
    del variables_product[0]

    # Lista todos os dados faltantes por grupos de colunas faltantes
    list_missing_data = [counts[np.logical_and.reduce(x)] for x in variables_product]
    # Remove as colunas de dado faltante dos dataframes
    list_missing_data = [x.drop(columns=x.columns[x.isin(['nan']).any()].tolist()) for x in list_missing_data]

    # Lista colunas que não são filtros
    not_filter_columns = list(set(counts.columns.to_list()) - set(variables))

    # Remove dados faltantes
    counts = counts[~np.logical_or.reduce(variables_product[-1])]

    # Executa para cada conjunto de dados faltantes
    for missing_count in list_missing_data:
        # Executa para cada linha de dados faltantes
        for row in missing_count.itertuples(index=False):
            row_dict = dict(row._asdict())
            for key in not_filter_columns:
                row_dict.pop(key)
            condition = logical_and_from_dict(counts,row_dict)
            sum_data = counts[condition]["CONTAGEM"].sum()
            # Caso não haja proporção conhecida relaxa o filtro
            while sum_data == 0.0 and len(row_dict) > 0:
                row_dict = relax_filter(row_dict,variables)
                condition = logical_and_from_dict(counts,row_dict)
                sum_data = counts[condition]["CONTAGEM"].sum()
            counts.loc[condition,"CONTAGEM"] = counts[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

    return counts

def redistribute_cid_chapter(counts,chapter,filter_columns,chapter_column="CID10_CHAPTER"):
    df_chapter = counts[(counts[chapter_column] == chapter) & (counts['CONTAGEM'] > 0)]
    counts = counts[counts[chapter_column] != chapter]
    not_filter_columns = list(set(counts.columns.to_list()) - set(filter_columns))

    for row in df_chapter.itertuples(index=False):
        row_dict = dict(row._asdict())
        for key in not_filter_columns:
            row_dict.pop(key)
        condition = logical_and_from_dict(counts,row_dict)
        sum_data = counts[condition]['CONTAGEM'].sum()
        while sum_data == 0.0 and len(row_dict) > 0:
            row_dict = relax_filter(row_dict,filter_columns)
            condition = logical_and_from_dict(counts,row_dict)
            sum_data = counts[condition]["CONTAGEM"].sum()
        redistributed_values = counts[condition]['CONTAGEM'].apply(lambda x: row.CONTAGEM*x/sum_data + x).copy()
        counts.loc[condition,'CONTAGEM'] = redistributed_values

    return counts