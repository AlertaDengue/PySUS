# -*- coding:utf-8 -*-
u"""
This module contains a set of functions to process data on SIM
Created on 16/08/2020
by gabrielmcf
license: GPL V3 or Later
"""

__docformat__ = "restructuredtext en"
from decimal import Decimal
from itertools import product

import numpy as np
import pandas as pd


def logical_and_from_dict(dataframe, dictionary):
    if dictionary == {}:
        return np.array([True] * len(dataframe), dtype=bool)
    return np.logical_and.reduce([dataframe[k] == v for k, v in dictionary.items()])


def relax_filter(dictionary, fields):
    for field in reversed(fields):
        if field in dictionary:
            del dictionary[field]
            break
    return dictionary


def group_and_count(
    dataframe, group_columns, count_column="COUNTS", decimal_counts=False
):
    """
    Agrupa e conta as variáveis passadas como parâmetro no dataframe. Cria uma nova
    coluna de contagem, com o tipo Decimal para possibilitar redistribuição pro rata posterior e maior precisão.
    :param dataframe: dataframe pandas
    :param group_columns: lista de string contendo o nome das colunas a serem agrupadas no dataframe.
    :param count_columns: nome da coluna de counts a ser criada.
    :return:
    """
    counts = dataframe.groupby(group_columns).size().reset_index(name=count_column)

    if decimal_counts:
        counts[count_column] = counts[count_column].apply(lambda x: Decimal(x))
    else:
        counts[count_column] = counts[count_column].astype("float64")

    return counts


def redistribute_missing(
    counts, filter_columns, count_column="COUNTS", nan_string="nan"
):
    """
    Realiza redistribuição pro rata das contagens do SIM com algum dado faltante.
    O dataframe deve conter uma coluna float64 chamada CONTAGEM e as demais colunas devem ser
    do tipo category, tendo os dados faltantes em uma categoria definida pelo parâmetro nan_string.
    :param counts: dataframe pandas contendo coluna com soma chamada CONTAGEM
    :param filter_columns: variáveis a serem consideradas para filtro de redistribuição pro rata
    :param count_columns: nome da coluna de counts.
    :param nan_string: string usada na categoria de dado faltante
    :return:
    """

    # Removendo categorias faltantes vazias
    for var in filter_columns:
        condition_dict = {var: nan_string, count_column: 0.0}
        counts = counts[~logical_and_from_dict(counts, condition_dict)]

    ### Dataframes de dados faltantes

    variables_dict = [{x: nan_string} for x in filter_columns]

    variables_condition = [logical_and_from_dict(counts, x) for x in variables_dict]

    # Primeiro item da tupla é != nan, segundo é o == nan
    variables_tuples = [(np.logical_not(x), x) for x in variables_condition]
    variables_product = list(product(*variables_tuples))

    # Remove regra de todos != nan
    del variables_product[0]

    # Lista todos os dados faltantes por grupos de colunas faltantes
    list_missing_data = [counts[np.logical_and.reduce(x)] for x in variables_product]
    # Remove as colunas de dado faltante dos dataframes
    list_missing_data = [
        x.drop(columns=x.columns[x.isin([nan_string]).any()].tolist())
        for x in list_missing_data
    ]
    # Remove os conjuntos vazios
    list_missing_data = list(filter(lambda x: not x.empty, list_missing_data))

    # Remove dados faltantes
    counts = counts[~np.logical_or.reduce(variables_product[-1])]

    # Executa para cada conjunto de dados faltantes
    for missing_count in list_missing_data:
        counts = redistribute_rows_pro_rata(counts, filter_columns, missing_count)

    return counts


def redistribute_cid_chapter(
    counts,
    filter_columns,
    chapter=18,
    chapter_column="CID10_CHAPTER",
    count_columns="COUNTS",
):
    """
    Realiza redistribuição pro rata das contagens do SIM de um capítulo do CID10 passado.
    Por padrão o capítulo XVIII, de causas mal definidas, é redistribuído.
    :param counts: dataframe pandas contendo coluna com contagem
    :param filter_columns: variáveis a serem consideradas para filtro na redistribuição pro rata
    :param chapter: capítulo do CID10 a ser redistribuído
    :param chapter_column: nome da coluna de capítulo
    :param count_columns: nome da coluna de counts
    :return:
    """
    df_chapter = counts[
        (counts[chapter_column] == chapter) & (counts[count_columns] > 0)
    ]
    counts = counts[counts[chapter_column] != chapter]

    return redistribute_rows_pro_rata(counts, filter_columns, df_chapter)


def redistribute_rows_pro_rata(
    counts, filter_columns, redistribute_list, count_columns="COUNTS"
):
    """
    Redistribui as contagens do dataframe conforme as colunas de filtro passadas.
    :param counts: dataframe pandas contendo coluna de contagem
    :param filter_columns: variáveis a serem consideradas para filtro na redistribuição pro rata
    :param redistribute_list: dataframe contendo as linhas que serão redistribuídas
    :param count_columns: nome da coluna de counts
    :return:
    """
    # Evita alerta na atribuição de múltiplos itens com máscara (.loc)
    pd.set_option("mode.chained_assignment", None)

    not_filter_columns = list(set(counts.columns.to_list()) - set(filter_columns))

    for row in redistribute_list.itertuples(index=False):
        row_dict = dict(row._asdict())
        [row_dict.pop(key) for key in not_filter_columns]
        condition = logical_and_from_dict(counts, row_dict)
        sum_data = counts[condition][count_columns].sum()
        # Caso não haja proporção conhecida relaxa o filtro
        while sum_data == 0.0 and len(row_dict) > 0:
            row_dict = relax_filter(row_dict, filter_columns)
            condition = logical_and_from_dict(counts, row_dict)
            sum_data = counts[condition][count_columns].sum()
        counts.loc[condition, count_columns] = counts[condition][count_columns].apply(
            lambda x: pro_rata_model(x, getattr(row, count_columns), sum_data)
        )

    # Volta alerta para warning
    pd.set_option("mode.chained_assignment", "warn")
    return counts


def pro_rata_model(current_value, redistribution_amount, group_sum):
    return redistribution_amount * current_value / group_sum + current_value
