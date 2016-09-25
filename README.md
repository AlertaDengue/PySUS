PySUS
=====

This package collects a set of utilities for handling with public databases published by Brazil's DATASUS
The documentation of how to use PySUS can be found [here](http://pysus.readthedocs.io/en/latest/)

Features
--------

- Decode encoded patient age to any time unit (years, months, etc)
- Convert `.dbc` files to DBF databases or read them into pandas dataframes. DBC files are basically DBFs compressed by a proprietary algorithm.
- Read SINAN dbf files returning DataFrames with properly typed columns

Instalation
-----------

`$ sudo pip install PySUS`

Examples
--------

```python
>>> from pysus.preprocessing.sinan import read_sinan_dbf

>>> df = read_sinan_dbf('mytest.dbf', encoding='latin-1')
>>> df.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 65535 entries, 0 to 65534
Data columns (total 10 columns):
DT_DIGITA     65469 non-null object
DT_NOTIFIC    65535 non-null object
DT_SIN_PRI    65535 non-null object
ID_AGRAVO     65535 non-null object
ID_BAIRRO     50675 non-null float64
ID_MUNICIP    65535 non-null int64
NM_BAIRRO     60599 non-null object
NU_ANO        65535 non-null int64
SEM_NOT       65535 non-null int64
SEM_PRI       65535 non-null int64
dtypes: float64(1), int64(4), object(5)
memory usage: 5.0+ MB

>>> df.DT_DIGITA[0]
datetime.date(2016, 4, 1)

```
