def rate_per_class(df):
    non_zero_rates = df.groupby(df.columns.tolist()).size()
    print(non_zero_rates)


# from pysus.online_data.SIM import download
# from pysus.preprocessing.decoders import decodifica_idade_SIM
# from pysus.preprocessing.SIM import rate_per_class

# df = download(['RO'], [2001,2002])


# df["IDADE_ANOS"] = decodifica_idade_SIM(df.filter(['IDADE']),'Y')

# df = df.filter(['ANO','CODMUNRES','SEXO','IDADE_ANOS'])
# df = df.fillna(999)
# df = df.astype('int32')

# non_zero_rates = df.groupby(df.columns.tolist()).size()
