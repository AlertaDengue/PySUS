from pysus.online_data.SIM import download
from pysus.preprocessing.decoders import translate_variables_SIM, group_count_and_resample
from pysus.utilities import BR_STATES
import numpy as np
import pandas as pd



# A ordem das variáveis define a prioridade para distribuição.
# ['CODMUNRES','SEXO','IDADE_ANOS'] significa que IDADE_ANOS será removida primeiro na redistribuição e CODMUNRES por último.
variables = ['CODMUNRES','SEXO','IDADE_ANOS']

print("Baixando dados")
years = [2001,2002,2003,2004,2005,2006,2007,2008,2009,2010]
first = True
for state in BR_STATES:
    for year in years:
        print("Baixando {} {}".format(state,year))
        df = download(state, year)

        print("Traduzindo variáveis")
        df = translate_variables_SIM(df,age_classes=True)

        print("Filtrando")
        df = df[variables]

        print("Agrupando e contando")
        rates = group_count_and_resample(df,variables)

        print("Adicionando colunas de ano e estado")
        rates.insert(loc=0,column="UF",value=state)
        rates.insert(loc=0,column="ANO",value=year)

        print("Salvando no CSV")
        rates.to_csv("BR-{}-{}.csv".format(years[0],years[-1]),mode='a',index=False,header=first)
        if first:
            first = False

