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
from pysus.online_data.SIM import get_municipios

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

def get_age_string(unidade):
    if unidade == 'Y':
        return 'ANOS'
    elif unidade == 'M':
        return 'MESES'
    elif unidade == 'D':
        return 'DIAS'
    elif unidade == 'H':
        return 'HORAS'
    elif unidade == 'm':
        return 'MINUTOS'
    else:
        return ''

@np.vectorize
def decodifica_idade_SIM(idade, unidade="D"):
    """
    Em tabelas do SIM a idade encontra-se codificada
    :param idade: valor original da tabela do SIM
    :param unidade: Unidade de saida desejada: 'Y': anos, 'M' meses, 'D': dias, 'H': horas, 'm': minutos. Valor default: 'D'
    :return:
    """
    fator = {'Y': 365., 'M': 30., 'D': 1., 'H': 1/24., 'm': 1/1440.}
    try:
        if idade.startswith('0') and idade[1:] != '00':
            idade = timedelta(minutes=int(idade[1:]))
            idade = idade.seconds/86400 + idade.days
        elif idade.startswith('1'):
            idade = timedelta(hours=int(idade[1:]))
            idade = idade.seconds/86400 + idade.days
        elif idade.startswith('2'):
            idade = timedelta(days=int(idade[1:])).days
        elif idade.startswith('3'):
            idade = timedelta(days=int(idade[1:]) * 30).days
        elif idade.startswith('4'):
            idade = timedelta(days=int(idade[1:]) * 365).days
        elif idade.startswith('5'):
            idade = timedelta(days=int(idade[1:]) * 365).days + 100 * 365
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

def get_valid_geocodes():
    tab_mun = get_municipios()
    df = tab_mun[(tab_mun["SITUACAO"] != "IGNOR")]
    return df["MUNCODDV"].append(df["MUNCOD"]).values

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


def translate_variables_SIM(dataframe,age_unity='Y',age_classes=None,classify_args={},municipality_data = True):
    variables_names = dataframe.columns
    df = dataframe
    
    valid_mun = get_valid_geocodes()

    # IDADE
    if("IDADE" in variables_names):
        column_name = "IDADE_{}".format(get_age_string(age_unity))
        df[column_name] = decodifica_idade_SIM(df["IDADE"],age_unity)
        if(age_classes):
            df[column_name] = classify_age(df[column_name],**classify_args)
            df[column_name] = df[column_name].astype('category')
            df[column_name] = df[column_name].cat.add_categories(['nan'])
            df[column_name] = df[column_name].fillna('nan')

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
        df["SEXO"] = df["SEXO"].astype('category')
        df["SEXO"] = df["SEXO"].cat.add_categories(['nan'])
        df["SEXO"] = df["SEXO"].fillna('nan')


    # CODMUNRES
    if("CODMUNRES" in variables_names):
        df["CODMUNRES"] = df["CODMUNRES"].astype('int64')
        df["CODMUNRES"] = add_dv(df["CODMUNRES"])
        df.loc[~df["CODMUNRES"].isin(valid_mun),"CODMUNRES"] = pd.NA
        df["CODMUNRES"] = df["CODMUNRES"].astype('category')
        df["CODMUNRES"] = df["CODMUNRES"].cat.add_categories(['nan'])
        df["CODMUNRES"] = df["CODMUNRES"].fillna('nan')


    return df


def classify_age(serie,start=0,end=90,freq=None,open_end=True,closed='left',interval=None):
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
    if(interval):
        iv = interval
    else:
        iv = pd.interval_range(start=start,end=end,freq=freq,closed=closed)
    iv_array = iv.to_tuples().tolist()

    # Adiciona classe aberta no final da lista de intervalos. 
    # Útil para criar agrupamentos como 0,1,2,...,89,90+
    if(open_end):
        iv_array.append((iv_array[-1][1],+np.inf))
    intervals = pd.IntervalIndex.from_tuples(iv_array,closed=closed)
    return pd.cut(serie,intervals)

def create_condition(dataframe,dictionary):
    if dictionary == {}:
        return np.array([True] * len(dataframe), dtype=bool)
    return np.logical_and.reduce([dataframe[k] == v for k,v in dictionary.items()])

def relax_filter(dictionary,fields):
    for field in fields:
        if field in dictionary:
            del dictionary[field]
            break
    return dictionary

def group_count_and_resample(dataframe,variables):
    df = dataframe

    # No pandas 1.1.0 será possível usar o argumento dropna=False, e evitar de converter NaN em uma categoria no translate_variables_SIM
    rates = df.groupby(variables).size().reset_index(name='CONTAGEM')
    rates["CONTAGEM"] = rates["CONTAGEM"].astype('float64')

    sum_original = rates["CONTAGEM"].sum()

    print("Removendo categorias faltantes vazias")
    # Remove município desconhecido com contagem 0
    rates = rates[~((rates['CODMUNRES'] == 'nan') & (rates['CONTAGEM'] == 0.0))]

    # Remove sexo desconhecido com contagem 0
    rates = rates[~((rates['SEXO'] == 'nan') & (rates['CONTAGEM'] == 0.0))]

    # Remove idade desconhecida com contagem 0
    rates = rates[~((rates['IDADE_ANOS'] == 'nan') & (rates['CONTAGEM'] == 0.0))]

    ### Dataframes de dados faltantes

    print("Criando dataframes de dados faltantes")
    # Faltando apenas município
    rates_no_munic = rates[(rates['CODMUNRES'] == 'nan') & ~(rates['SEXO'] == 'nan') & ~(rates['IDADE_ANOS'] == 'nan')].drop(columns=['CODMUNRES'])

    # Faltando apenas sexo
    rates_no_sex = rates[~(rates['CODMUNRES'] == 'nan') & (rates['SEXO'] == 'nan') & ~(rates['IDADE_ANOS'] == 'nan')].drop(columns=['SEXO'])

    # Faltando apenas idade
    rates_no_age = rates[~(rates['CODMUNRES'] == 'nan') & ~(rates['SEXO'] == 'nan') & (rates['IDADE_ANOS'] == 'nan')].drop(columns=['IDADE_ANOS'])

    # Faltando município e sexo
    rates_no_munic_sex = rates[(rates['CODMUNRES'] == 'nan') & (rates['SEXO'] == 'nan') & ~(rates['IDADE_ANOS'] == 'nan')].drop(columns=['CODMUNRES','SEXO'])

    # Faltando município e idade
    rates_no_munic_age = rates[(rates['CODMUNRES'] == 'nan') & ~(rates['SEXO'] == 'nan') & (rates['IDADE_ANOS'] == 'nan')].drop(columns=['CODMUNRES','IDADE_ANOS'])

    # Faltando sexo e idade
    rates_no_sex_age = rates[~(rates['CODMUNRES'] == 'nan') & (rates['SEXO'] == 'nan') & (rates['IDADE_ANOS'] == 'nan')].drop(columns=['SEXO','IDADE_ANOS'])

    # Faltando município, sexo e idade
    rates_no_munic_sex_age = rates[(rates['CODMUNRES'] == 'nan') & (rates['SEXO'] == 'nan') & (rates['IDADE_ANOS'] == 'nan')].drop(columns=['CODMUNRES','SEXO','IDADE_ANOS'])

    # Remove dados faltantes
    rates = rates[~((rates['CODMUNRES'] == 'nan') | (rates['SEXO'] == 'nan') | (rates['IDADE_ANOS'] == 'nan'))]

    print("Redistribuindo mortes com dados faltantes")

    missing_rates = [rates_no_age, rates_no_sex, rates_no_sex_age, rates_no_munic, rates_no_munic_age, rates_no_munic_sex, rates_no_munic_sex_age]

    # Executa para cada conjunto de dados faltantes
    for missing_rate in missing_rates:
        print("Dados conhecidos:",missing_rate.columns.tolist()[:-1])
        sum_missing = missing_rate["CONTAGEM"].sum()
        sum_rates = rates["CONTAGEM"].sum()
        # Executa para cada linha de dados faltantes
        for row in missing_rate.itertuples(index=False):
            row_dict = dict(row._asdict())
            del row_dict["CONTAGEM"]
            condition = create_condition(rates,row_dict)
            sum_data = rates[condition]["CONTAGEM"].sum()
            # Caso não haja proporção conhecida relaxa o filtro
            while sum_data == 0.0:
                row_dict = relax_filter(row_dict,variables)
                condition = create_condition(rates,row_dict)
                sum_data = rates[condition]["CONTAGEM"].sum()
                print("Linha sem proporção conhecida:",dict(row._asdict()))
                print("Filtro utilizado:",list(row_dict.keys()))
            rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)
        print('Dif. : {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_missing)))
        print('----------')
    print('Dif. final: {:f}'.format(rates["CONTAGEM"].sum() - sum_original))

    return rates