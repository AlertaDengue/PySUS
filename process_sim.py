from pysus.online_data.SIM import download
from pysus.preprocessing.decoders import translate_variables_SIM
import numpy as np

def create_condition(dataframe,dictionary):
    return np.logical_and.reduce([dataframe[k] == v for k,v in dictionary.items()])

variables = ['ANO','UF','CODMUNRES','SEXO','IDADE_ANOS']

print("Baixando dados")
df = download('SP', [2006])

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

missing_rates = [rates_no_age, rates_no_sex, rates_no_sex_age, rates_no_munic, rates_no_munic_age, rates_no_munic_sex, rates_no_munic_sex_age]

# Distribui dados com idade faltante
# sum_no_age = rates_no_age["CONTAGEM"].sum()
# sum_rates = rates["CONTAGEM"].sum()


for missing_rate in missing_rates:
    sum_missing = missing_rate["CONTAGEM"].sum()
    sum_rates = rates["CONTAGEM"].sum()
    for row in missing_rate.itertuples(index=False):
        row_dict = dict(row._asdict())
        del row_dict["CONTAGEM"]
        condition = create_condition(rates,row_dict)
        sum_data = rates[condition]["CONTAGEM"].sum()
        if sum_data == 0.0:
            if "CODMUNRES" in row_dict:
                del row_dict["CODMUNRES"]
            elif "SEXO" in row_dict:
                del row_dict["CODMUNRES"]
            elif "IDADE_ANOS" in row_dict:
                del row_dict["IDADE_ANOS"]
            condition = create_condition(rates,row_dict)
            sum_data = rates[condition]["CONTAGEM"].sum()
        rates.loc[condition,"CONTAGEM"] = rates[condition]["CONTAGEM"].apply(lambda x: row.CONTAGEM*x/sum_data + x)
    print("Filtro:",missing_rate.columns.tolist()[:-1])
    print('Dif. : {:f}'.format(rates["CONTAGEM"].sum() - (sum_rates + sum_missing)))

print('Dif. final: {:f}'.format(rates["CONTAGEM"].sum() - sum_original))
print("Gerando CSV")
rates.to_csv("sp-2006.csv",index=False)
