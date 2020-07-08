from pysus.online_data.SIM import download
from pysus.preprocessing.decoders import translate_variables_SIM

variables = ['ANO','UF','CODMUNRES','SEXO','IDADE_ANOS']

def create_query(row):
    return ''

print("Baixando dados")
df = download('SP', [2010])

print("Traduzindo variáveis")
df = translate_variables_SIM(df)

print("Filtrando")
df = df[variables]

print("Adicionando categorias faltantes")
df["ANO"] = df["ANO"].astype('category')
df["UF"] = df["UF"].astype('category')

df["IDADE_ANOS"] = df["IDADE_ANOS"].fillna(999)
df["IDADE_ANOS"] = df["IDADE_ANOS"].astype('uint16')

df["CODMUNRES"] = df["CODMUNRES"].cat.add_categories(['999'])
df["CODMUNRES"] = df["CODMUNRES"].fillna('999')

df["SEXO"] = df["SEXO"].cat.add_categories(['999'])
df["SEXO"] = df["SEXO"].fillna('999')

print("Agrupando e contando")
rates = df.groupby(df.columns.tolist()).size().reset_index(name='CONTAGEM')
rates["CONTAGEM"] = rates["CONTAGEM"].astype('float64')

sum_original = rates["CONTAGEM"].sum()

print("Removendo categorias faltantes vazias")
# Remove município desconhecido com contagem 0
rates = rates[~((rates['CODMUNRES'] == '999') & (rates['CONTAGEM'] == 0.0))]

# Remove sexo desconhecido com contagem 0
rates = rates[~((rates['SEXO'] == '999') & (rates['CONTAGEM'] == 0.0))]

# Remove idade desconhecida com contagem 0
rates = rates[~((rates['IDADE_ANOS'] == 999) & (rates['CONTAGEM'] == 0.0))]

### Dataframes de dados faltantes

print("Criando dataframes de dados faltantes")
# Faltando apenas município
rates_no_munic = rates[(rates['CODMUNRES'] == '999') & (rates['SEXO'] != '999') & (rates['IDADE_ANOS'] != 999)].drop(columns=['CODMUNRES'])

# Faltando apenas sexo
rates_no_sex = rates[(rates['CODMUNRES'] != '999') & (rates['SEXO'] == '999') & (rates['IDADE_ANOS'] != 999)].drop(columns=['SEXO'])

# Faltando apenas idade
rates_no_age = rates[(rates['CODMUNRES'] != '999') & (rates['SEXO'] != '999') & (rates['IDADE_ANOS'] == 999)].drop(columns=['IDADE_ANOS'])

# Faltando município e sexo
rates_no_munic_sex = rates[(rates['CODMUNRES'] == '999') & (rates['SEXO'] == '999') & (rates['IDADE_ANOS'] != 999)].drop(columns=['CODMUNRES','SEXO'])

# Faltando município e idade
rates_no_munic_age = rates[(rates['CODMUNRES'] == '999') & (rates['SEXO'] != '999') & (rates['IDADE_ANOS'] == 999)].drop(columns=['CODMUNRES','IDADE_ANOS'])

# Faltando sexo e idade
rates_no_sex_age = rates[(rates['CODMUNRES'] != '999') & (rates['SEXO'] == '999') & (rates['IDADE_ANOS'] == 999)].drop(columns=['SEXO','IDADE_ANOS'])

# Faltando município, sexo e idade
rates_no_munic_sex_age = rates[(rates['CODMUNRES'] == '999') & (rates['SEXO'] == '999') & (rates['IDADE_ANOS'] == 999)].drop(columns=['CODMUNRES','SEXO','IDADE_ANOS'])

# Remove dados faltantes
rates = rates[~((rates['CODMUNRES'] == '999') | (rates['SEXO'] == '999') | (rates['IDADE_ANOS'] == 999))]

print("Redistribuindo mortes com dados faltantes")

# missing_rates = [rates_no_munic, rates_no_sex, rates_no_age, rates_no_munic_age, rates_no_munic_sex, rates_no_sex_age, rates_no_munic_sex_age]

# for missing_rate in missing_rates:
#     missing_sum = missing_rate["CONTAGEM"].sum()


# Distribui dados com idade faltante
sum_no_age = rates_no_age["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_age.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["CODMUNRES"] == row.CODMUNRES) & (rates["SEXO"] == row.SEXO)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. idade faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_age)))

# Distribui dados com sexo faltante
sum_no_sex = rates_no_sex["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_sex.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["CODMUNRES"] == row.CODMUNRES) & (rates["IDADE_ANOS"] == row.IDADE_ANOS)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. sexo faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_sex)))

# Distribui dados com sexo e idade faltante
sum_no_sex_age = rates_no_sex_age["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_sex_age.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["CODMUNRES"] == row.CODMUNRES)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. sexo e idade faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_sex_age)))

# Distribui dados com município faltante
sum_no_munic = rates_no_munic["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_munic.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["SEXO"] == row.SEXO) & (rates["IDADE_ANOS"] == row.IDADE_ANOS)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. município faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_munic)))

# Distribui dados com município e idade faltante
sum_no_munic_age = rates_no_munic_age["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_munic_age.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["SEXO"] == row.SEXO)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. município e idade faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_munic_age)))

# Distribui dados com município e sexo faltante
sum_no_munic_sex = rates_no_munic_sex["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_munic_sex.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF) & (rates["IDADE_ANOS"] == row.IDADE_ANOS)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. município e sexo faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_munic_sex)))

# Distribui dados com município, sexo e idade faltante
sum_no_munic_sex_age = rates_no_munic_sex_age["CONTAGEM"].sum()
sum_rates = rates["CONTAGEM"].sum()
for row in rates_no_munic_sex_age.itertuples():
    condition = (rates["ANO"] == row.ANO) & (rates["UF"] == row.UF)
    sum_data = rates[condition]["CONTAGEM"].sum()
    rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)

print('Dif. município, sexo e idade faltante: {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_no_munic_sex_age)))

print('Dif. final: {:f}'.format(rates["CONTAGEM"].sum() - sum_original))

print("Gerando CSV")
rates.to_csv("br.csv",index=False)
