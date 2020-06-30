#-*- coding:utf-8 -*-
u"""
This module contains a set of functions to decode
commonly encoded variables
Created on 19/07/16
by fccoelho
license: GPL V3 or Later
"""

__docformat__ = 'restructuredtext en'
import numpy as np
import pandas as pd
from datetime import timedelta, datetime

@np.vectorize
def decodifica_idade_SINAN(idade, unidade='Y'):
    """
    Em tabelas do SINAN frequentemente a idade é representada como um inteiro que precisa ser parseado
    para retornar a idade em uma unidade cronológica padrão.
    :param unidade: unidade da idade: 'Y': anos, 'M' meses, 'D': dias, 'H': horas
    :param idade: inteiro ou sequencia de inteiros codificados.
    :return:
    """
    fator = {'Y': 1., 'M': 12., 'D': 365., 'H': 365*24.}
    if idade >= 4000: #idade em anos
        idade_anos = idade - 4000
    elif idade >= 3000 and idade < 4000: #idade em meses
        idade_anos = (idade-3000)/12.
    elif idade >= 2000 and idade < 3000: #idade em dias
        idade_anos = (idade-2000)/365.
    elif idade >= 1000 and idade < 2000: # idade em horas
        idade_anos = (idade-1000)/(365*24.)
    else:
        #print(idade)
        idade_anos = np.nan
        #raise ValueError("Idade inválida")
    idade_dec = idade_anos*fator[unidade]
    return idade_dec

@np.vectorize
def decodifica_idade_SIM(idade, unidade="D"):
    """
    Em tabelas do SIM a idade encontra-se codificada
    :param idade: valor original da tabela do SIM
    :param unidade: Unidade de saida desejada: 'Y': anos, 'M' meses, 'D': dias, 'H': horas. Valor default: 'D'
    :return:
    """
    fator = {'Y': 365., 'M': 30., 'D': 1., 'H': 1/24.}
    try:
        if idade.startswith('1'):
            idade = timedelta(hours=int(idade[1:])).days
        elif idade.startswith('2'):
            idade = timedelta(days=int(idade[1:])).days
        elif idade.startswith('3'):
            idade = timedelta(days=int(idade[1:]) * 30).days
        elif idade.startswith('4'):
            idade = timedelta(days=int(idade[1:]) * 365).days
        elif idade.startswith('5'):
            idade = timedelta(days=int(idade[1:]) * 365).days + 365 * 100
        else:
            idade = np.nan
    except ValueError:
        idade = np.nan
    return idade/fator.get(unidade, 1)

@np.vectorize
def decodifica_data_SIM(data):
    try:
        new_data = datetime.strptime(data, '%d%m%Y')
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
        raise ValueError('Geocode must have 7 digtis')
    dig = int(str(geocodigo)[-1])
    if dig == calculate_digit(geocodigo):
        return True
    else:
        return False


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


def add_dv(geocodigo):
    if len(str(geocodigo)) == 7:
        return geocodigo
    else:
        return int(str(geocodigo) + str(calculate_digit(geocodigo)))


def translate_variables_sim(dataframe, municipality_data = True):
    variables_names = dataframe.columns
    df = dataframe

    # # TIPOBITO
    # if("TIPOBITO" in variables_names):
    #     df = df["TIPOBITO"].replace({
    #             "0": np.nan,
    #             "9": np.nan,
    #             "1": "Fetal",
    #             "2": "Não Fetal"
    #         }
    #     )

    # # DTOBITO
    # if("DTOBITO" in variables_names):
    #     df["DTOBITO"] = decodifica_data_SIM(df["DTOBITO"])

    # SEXO
    if("SEXO" in variables_names):
        df["SEXO"].replace({
                "0": np.nan,
                "9": np.nan,
                "1": "Masculino",
                "2": "Feminino"
            },
            inplace=True
        )
        df["SEXO"] = df["SEXO"].astype('categorical')

    # CODMUNRES
    if("CODMUNRES" in variables_names):
        df["CODMUNRES"].astype('category')

    # # CODINST
    # if("CODINST" in variables_names):
    #     df = df["CODINST"].replace({
    #             "E": "Estadual",
    #             "R": "Regional",
    #             "M": "Municipal"
    #         }
    #     )