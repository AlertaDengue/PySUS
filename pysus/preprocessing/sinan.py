from dbfread import DBF
import pandas as pd


def read_sinan_dbf(fname, encoding) -> pd.DataFrame:
    """
    Read SINAN dbf file returning a Pandas Dataframe with
    :param fname: dbf file name
    :param encoding: Encoding of the dbf
    :return: pandas dataframe
    """
    db = DBF(fname, encoding=encoding)
    df = pd.DataFrame(list(db))
    for cname in df.columns:
        df[cname].replace('', pd.np.nan, inplace=True)
        if cname.startswith(('NU', 'ID')):
            try:
                df[cname] = pd.to_numeric(df[cname])
            except ValueError as e:
                # certain IDs can be alphanumerical
                pass
        elif cname.startswith('SEM'):
            df[cname] = df[cname].map(lambda x: int(x) % 100)

    return df



