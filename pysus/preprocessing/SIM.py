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

def redistribute(counts,variables):
    """
    Realiza redistribuição pro rata das contagens com algum dado faltante.
    O dataframe deve conter uma coluna float64 chamada CONTAGEM e as demais colunas devem ser
    do tipo category, tendo os dados faltantes em uma categoria chamada 'nan'.
    :param counts: dataframe pandas contendo coluna com soma chamada CONTAGEM
    :param variables: variáveis a serem consideradas para filtro de redistribuição pro rata
    :return:
    """
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

    missing_counts = [counts[np.logical_and.reduce(x)] for x in variables_product]
    # Remove colunas com nan, no pandas 1.1.0 será possível deixar esses valores como NaN de verdade
    missing_counts = [x.drop(columns=x.columns[x.isin(['nan']).any()].tolist()) for x in missing_counts]

    # # Remove dados faltantes
    counts = counts[~np.logical_or.reduce(variables_product[-1])]


    # Executa para cada conjunto de dados faltantes
    for missing_rate in missing_counts:
        # Executa para cada linha de dados faltantes
        for row in missing_rate.itertuples(index=False):
            row_dict = dict(row._asdict())
            del row_dict["CONTAGEM"]
            condition = logical_and_from_dict(counts,row_dict)
            sum_data = counts[condition]["CONTAGEM"].sum()
            # Caso não haja proporção conhecida relaxa o filtro
            while sum_data == 0.0:
                row_dict = relax_filter(row_dict,variables)
                condition = logical_and_from_dict(counts,row_dict)
                sum_data = counts[condition]["CONTAGEM"].sum()
            counts.loc[condition,"CONTAGEM"] = counts[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

    return counts