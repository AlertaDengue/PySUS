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

@np.vectorize
def decodifica_idade(idade, unidade='Y'):
    """
    Em tabelas do SINAN frequentemente a idade é representada como um inteiro que precisa ser parseado
    para retornar a idade em uma unidade cronológica padrão.
    :param unidade: unidade da idade: 'Y': anos, 'M' meses, 'D': dias, 'H': horas
    :param idade: inteiro ou sequencia de inteiros codificados.
    :return:
    """
    fator = {'Y': 1, 'M': 12, 'D': 365, 'H': 365*24}
    if idade >= 4000: #idade em anos
        idade_anos = idade - 4000
    elif idade >= 3000 and idade < 4000: #idade em meses
        idade_anos = (idade-3000)/12
    elif idade >= 2000 and idade < 3000: #idade em dias
        idade_anos = (idade-2000)/365
    elif idade >= 1000 and idade < 2000: # idade em horas
        idade_anos = (idade-1000)/(365*24)
    else:
        #print(idade)
        idade_anos = np.nan
        #raise ValueError("Idade inválida")
    idade_dec = idade_anos/fator[unidade]
    return idade_dec
