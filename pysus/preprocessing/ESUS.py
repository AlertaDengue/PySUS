import numpy as np
import pandas as pd

from pysus.online_data.ESUS import download


def cases_by_age_and_sex(UF, start="2020-03-01", end="2020-08-31"):
    """
    Fetches ESUS covid line list and aggregates by age and sex returning these counts between start and end dates.
    :param UF: State code
    :param start: Start date
    :param end: end date
    :return: dataframe
    """
    df = download(uf=UF)

    # Transformando as colunas em datetime type
    for cname in df:
        if cname.startswith("data"):
            df[cname] = pd.to_datetime(df[cname], errors="coerce")

    # Eliminando os valores nulos nas colunas com datas importantes
    old_size = len(df)
    df.dropna(
        subset=["dataNotificacao", "dataInicioSintomas", "dataTeste"], inplace=True
    )
    print(
        f"Removed {old_size - len(df)} rows with missing dates of symptoms, notification or testing"
    )

    # Desconsiderando os resultados negativos ou inconclusivos
    df = df.loc[~df.resultadoTeste.isin(["Negativo", "Inconclusivo ou Indeterminado"])]

    # Removendo sexo indeterminado
    df = df.loc[df.sexo.isin(["Masculino", "Feminino"])]

    # determinando a data dos primeiros sintomas como a data do index

    df["datesint"] = df["dataInicioSintomas"]
    df.set_index("datesint", inplace=True)
    df.sort_index(inplace=True, ascending=True)

    # vamos limitar a data inicial e a data final considerando apenas a primeira onda

    df = df.loc[start:end]

    ini = np.arange(0, 81, 5)
    fin = np.arange(5, 86, 5)
    fin[-1] = 120
    faixa_etaria = {f"[{i},{f})": (i, f) for i, f in zip(ini, fin)}

    labels = list(faixa_etaria.keys())
    df["faixa_etaria"] = [labels[i - 1] for i in np.digitize(df.idade, bins=ini)]

    agreg = df[["sexo", "faixa_etaria"]].groupby(["faixa_etaria", "sexo"]).size()
    agreg = agreg.reset_index()
    agreg.columns = ["faixa_etaria", "sexo", "n"]
    return agreg
