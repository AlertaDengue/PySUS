# -*- coding:utf-8 -*-
"""
This module contains a set of functions to decode
commonly encoded variables
Created on 19/07/16
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = "restructuredtext en"
import re
from datetime import datetime, timedelta
from string import ascii_uppercase

import numpy as np
import pandas as pd

from pysus.online_data.SIM import (
    get_CID10_chapters_table,
    get_CID10_table,
    get_municipios,
)


@np.vectorize
def decodifica_idade_SINAN(idade, unidade: str = "Y"):
    """
    Em tabelas do SINAN frequentemente a idade é representada como um inteiro que precisa ser parseado
    para retornar a idade em uma unidade cronológica padrão.
    :param unidade: unidade da idade: 'Y': anos, 'M' meses, 'D': dias, 'H': horas
    :param idade: inteiro ou sequencia de inteiros codificados.
    :return:
    """
    fator = {"Y": 1.0, "M": 12.0, "D": 365.0, "H": 365 * 24.0}
    if idade >= 4000:  # idade em anos
        idade_anos = idade - 4000
    elif idade >= 3000 and idade < 4000:  # idade em meses
        idade_anos = (idade - 3000) / 12.0
    elif idade >= 2000 and idade < 3000:  # idade em dias
        idade_anos = (idade - 2000) / 365.0
    elif idade >= 1000 and idade < 2000:  # idade em horas
        idade_anos = (idade - 1000) / (365 * 24.0)
    else:
        idade_anos = np.nan
    idade_dec = idade_anos * fator[unidade]
    return idade_dec


def get_age_string(unidade):
    if unidade == "Y":
        return "ANOS"
    elif unidade == "M":
        return "MESES"
    elif unidade == "D":
        return "DIAS"
    elif unidade == "H":
        return "HORAS"
    elif unidade == "m":
        return "MINUTOS"
    else:
        return ""


@np.vectorize
def decodifica_idade_SIM(idade, unidade="D"):
    """
    Em tabelas do SIM a idade encontra-se codificada
    :param idade: valor original da tabela do SIM
    :param unidade: Unidade de saida desejada: 'Y': anos, 'M' meses, 'D': dias, 'H': horas, 'm': minutos. Valor default: 'D'
    :return:
    """
    fator = {"Y": 365.0, "M": 30.0, "D": 1.0, "H": 1 / 24.0, "m": 1 / 1440.0}
    try:
        if idade.startswith("0") and idade[1:] != "00":
            idade = timedelta(minutes=int(idade[1:]))
            idade = idade.seconds / 86400 + idade.days
        elif idade.startswith("1"):
            idade = timedelta(hours=int(idade[1:]))
            idade = idade.seconds / 86400 + idade.days
        elif idade.startswith("2"):
            idade = timedelta(days=int(idade[1:])).days
        elif idade.startswith("3"):
            idade = timedelta(days=int(idade[1:]) * 30).days
        elif idade.startswith("4"):
            idade = timedelta(days=int(idade[1:]) * 365).days
        elif idade.startswith("5"):
            idade = timedelta(days=int(idade[1:]) * 365).days + 100 * 365
        else:
            idade = np.nan
    except ValueError:
        idade = np.nan
    return idade / fator.get(unidade, 1)


@np.vectorize
def decodifica_data_SIM(data):
    try:
        new_data = datetime.strptime(data, "%d%m%Y")
    except ValueError:
        new_data = np.nan
    return new_data


@np.vectorize
def is_valid_geocode(geocodigo):
    """
    Returns True if the geocode is valid
    :param geocodigo:
    :return:
    """
    if len(str(geocodigo)) != 7:
        raise ValueError("Geocode must have 7 digtis")
    dig = int(str(geocodigo)[-1])
    if dig == calculate_digit(geocodigo):
        return True
    else:
        return False


def get_valid_geocodes():
    tab_mun = get_municipios()
    df = tab_mun[(tab_mun["SITUACAO"] != "IGNOR")]
    return df["MUNCODDV"].append(df["MUNCOD"]).astype("int64").values


def calculate_digit(geocode):
    """
    Calcula o digito verificador do geocódigo de município com 6 dígitos
    :param geocode: geocódigo com 6 dígitos
    :return: dígito verificador
    """
    peso = [1, 2, 1, 2, 1, 2, 0]
    soma = 0
    geocode = str(geocode)
    for i in range(6):
        valor = int(geocode[i]) * peso[i]
        soma += sum([int(d) for d in str(valor)]) if valor > 9 else valor
    dv = 0 if soma % 10 == 0 else (10 - (soma % 10))
    return dv


@np.vectorize
def add_dv(geocodigo):
    if len(str(geocodigo)) == 7:
        return geocodigo
    else:
        return int(str(geocodigo) + str(calculate_digit(geocodigo)))


def columns_as_category(series, nan_string=None):
    series = series.astype("category")
    series = series.cat.add_categories


def translate_variables_SIM(
    dataframe,
    age_unit="Y",
    age_classes=None,
    classify_args={},
    classify_cid10_chapters=False,
    geocode_dv=True,
    nan_string="nan",
    category_columns=True,
):
    variables_names = dataframe.columns.tolist()
    df = dataframe

    valid_mun = get_valid_geocodes()

    # IDADE
    if "IDADE" in variables_names:
        column_name = f"IDADE_{get_age_string(age_unit)}"
        df[column_name] = decodifica_idade_SIM(df["IDADE"], age_unit)
        if age_classes:
            df[column_name] = classify_age(df[column_name], **classify_args)
            df[column_name] = df[column_name].astype("category")
            df[column_name] = df[column_name].cat.add_categories([nan_string])
            df[column_name] = df[column_name].fillna(nan_string)

    # SEXO
    if "SEXO" in variables_names:
        df["SEXO"].replace(
            {"0": np.nan, "9": np.nan, "1": "Masculino", "2": "Feminino"}, inplace=True
        )
        df["SEXO"] = df["SEXO"].astype("category")
        df["SEXO"] = df["SEXO"].cat.add_categories([nan_string])
        df["SEXO"] = df["SEXO"].fillna(nan_string)

    # MUNRES
    if "MUNIRES" in variables_names:
        df = df.rename(columns={"MUNIRES": "CODMUNRES"})
        variables_names.append("CODMUNRES")

    # CODMUNRES
    if "CODMUNRES" in variables_names:
        if geocode_dv:
            df["CODMUNRES"] = df["CODMUNRES"].apply(add_dv)
        df["CODMUNRES"] = df["CODMUNRES"].astype("int64")
        df.loc[~df["CODMUNRES"].isin(valid_mun), "CODMUNRES"] = pd.NA
        df["CODMUNRES"] = df["CODMUNRES"].astype("category")
        df["CODMUNRES"] = df["CODMUNRES"].cat.add_categories([nan_string])
        df["CODMUNRES"] = df["CODMUNRES"].fillna(nan_string)

    # RACACOR
    if "RACACOR" in variables_names:
        df["RACACOR"].replace(
            {
                "0": np.nan,
                "1": "Branca",
                "2": "Preta",
                "3": "Amarela",
                "4": "Parda",
                "5": "Indígena",
                "6": np.nan,
                "7": np.nan,
                "8": np.nan,
                "9": np.nan,
                "": np.nan,
            },
            inplace=True,
        )
        df["RACACOR"] = df["RACACOR"].astype("category")
        df["RACACOR"] = df["RACACOR"].cat.add_categories([nan_string])
        df["RACACOR"] = df["RACACOR"].fillna(nan_string)

    # CAUSABAS IN CID10 CHAPTER
    if classify_cid10_chapters:
        code_index = get_CID10_code_index(get_CID10_chapters_table())
        df["CID10_CHAPTER"] = df["CAUSABAS"].str.slice(0, 3).map(code_index)
        df["CID10_CHAPTER"] = df["CID10_CHAPTER"].astype("category")

    return df


def classify_age(
    serie, start=0, end=90, freq=None, open_end=True, closed="left", interval=None
):
    """
    Classifica idade segundo parâmetros ou IntervalIndex
    :param serie: Serie pandas contendo idades
    :param start: início do primeiro grupo
    :param end: fim do último grupo
    :param freq: tamanho dos grupos. Por padrão considera cada valor um grupo.
    :param open_end: cria uma classe no final da lista de intervalos que contém todos acima daquele último valor. Default True
    :param closed: onde os intervalos devem ser fechados. Possíveis valores: {'left', 'right', 'both', 'neither'}. Default 'left'
    :param interval: IntervalIndex do pandas. Caso seja passado todos os outros parâmetros de intervalo são desconsiderados. Defaul None
    :return:
    """
    if interval:
        iv = interval
    else:
        iv = pd.interval_range(start=start, end=end, freq=freq, closed=closed)
    iv_array = iv.to_tuples().tolist()

    # Adiciona classe aberta no final da lista de intervalos.
    # Útil para criar agrupamentos como 0,1,2,...,89,90+
    if open_end:
        iv_array.append((iv_array[-1][1], +np.inf))
    intervals = pd.IntervalIndex.from_tuples(iv_array, closed=closed)
    return pd.cut(serie, intervals)


def get_CID10_code_index(datasus_chapters):
    code_index = {}
    for ch_array_index, chapter in datasus_chapters.iterrows():
        # Ex.: ['A00','B99']
        chapter_range = chapter["CAUSAS"].split("-")
        start_letter = chapter_range[0][0]
        end_letter = chapter_range[1][0]

        if start_letter == end_letter:
            number_range_start = int(chapter_range[0][1:3])
            number_range_finish = int(chapter_range[1][1:3])
            for code in range(number_range_start, number_range_finish + 1):
                code_index[f"{start_letter}{str(code).zfill(2)}"] = ch_array_index + 1
        else:
            string_range_start = chapter_range[0][0]
            string_range_end = chapter_range[1][0]
            full_string_range = re.compile(
                f"{string_range_start}.*{string_range_end}"
            ).search(ascii_uppercase)[0]

            for let_array_index, letter in enumerate(full_string_range):
                # First array letter
                if let_array_index == 0:
                    number_range_start = int(chapter_range[0][1:3])
                    number_range_end = 99
                elif let_array_index == len(full_string_range) - 1:  # Last array letter
                    number_range_start = 0
                    number_range_end = int(chapter_range[1][1:3])
                else:  # Middle letters
                    number_range_start = 0
                    number_range_end = 99
                for code_number in range(number_range_start, number_range_end + 1):
                    code_index[f"{letter}{str(code_number).zfill(2)}"] = (
                        ch_array_index + 1
                    )

    return code_index
