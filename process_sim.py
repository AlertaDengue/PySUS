from pysus.online_data.SIM import download
from pysus.preprocessing.decoders import translate_variables_SIM, group_and_count, resample
from pysus.utilities import BR_STATES
import numpy as np
import pandas as pd



# A ordem das variáveis define a prioridade para distribuição.
# ['CODMUNRES','SEXO','IDADE_ANOS'] significa que IDADE_ANOS será removida primeiro na redistribuição e CODMUNRES por último.
variables = ['CODMUNRES','SEXO','IDADE_ANOS']
# folder = '/media/gabriel/Croquete/FTP_DATASUS/datasus/dissemin/publicos/SIM/CID10/DORES'

print("Baixando dados")
# years = [1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018]
# years = [1979,1980,1981,1982,1983,1984,1985,1986,1987,1988,1989,1990,1991,1992,1993,1994,1995]
# years = [1989,1990,1991,1992,1993,1994,1995]
years = [2006]
first = True
for state in ['SP']:
    for year in years:
        print("Baixando {} {}".format(state,year))
        df = download(state, year)

        print("Traduzindo variáveis")
        df = translate_variables_SIM(df,age_classes=True)

        print("Filtrando")
        df = df[variables]

        print("Agrupando e contando")
        counts = group_and_count(df,variables)

        counts = redistribute(counts,variables)
        counts.to_parquet('teste.parquet')
        print("Adicionando colunas de ano e estado")
        counts.insert(loc=0,column="UF",value=state)
        counts.insert(loc=0,column="ANO",value=year)

        print("Salvando no CSV")
        # counts.to_csv("SP-{}-{}.csv".format(years[0],years[-1]),mode='a',index=False,header=first)
        counts.to_csv("SP-2006.csv",index=False)
        if first:
            first = False

