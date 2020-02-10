# PySUS
![travis](https://travis-ci.org/AlertaDengue/PySUS.svg?branch=master)

This package collects a set of utilities for handling with public databases published by Brazil's DATASUS
The documentation of how to use PySUS can be found [here](http://pysus.readthedocs.io/en/latest/)

## Features


- Decode encoded patient age to any time unit (years, months, etc)
- Convert `.dbc` files to DBF databases or read them into pandas dataframes. DBC files are basically DBFs compressed by a proprietary algorithm.
- Read SINAN dbf files returning DataFrames with properly typed columns
- Download SINASC data
- Download SIH data
- Download SIA data
- Download SIM data
- Download CIHA data
- Download SINAN data (only case investigation tables)

## Instalation

There are some dependencies which can't be installed through pip, namely `libffi`. Therefore on an ubuntu system:

```
sudo apt install libffi-dev
```
Then you can proceed to

`sudo pip install PySUS`

## Running from a Docker container
If you use windows, or for some other reason is not able to install PySUS on you OS, you can run it from a docker container.

First, clone the Pysus repository:

```bash
git clone https://github.com/fccoelho/PySUS.git
``` 
then from within the PySUS directory build the container

```bash
cd PySUS
docker build -t pysus .
```
You only have to do this once. On the first time it will take a few minutes.
Then you can launch jupyter from the container a just use PySUS:

```bash
docker run -p 8888:8888 pysus:latest
```
Point your browser to [http://127.0.0.1:8888](http://127.0.0.1:8888) and have fun.
Once you are done, you can stop the container with a simple `ctrl-c` from the terminal you started it or use the following command:
```bash
# to find the container ID
docker ps 
docker stop <CONTAINER ID>
```
### Mounting your working directory in the container
If you don't want you work to disappear when you stop the container, you must mount your working directory on the container. In the example below, I am mounting the `/home/fccoelho/Downloads/pysus` on the `/home/jovyan/work` directory inside the container. This means that everything that is saved inside the `work` directory will actually be saved in the `/home/fccoelho/Downloads/pysus`. Modify according to your needs.

```bash
docker run -e NB_USER=fccoelho -e NB_UID=1000 -v /home/fccoelho/Downloads/pysus:/home/jovyan/work -p 8888:8888 pysus:latest
```

For more options about interacting with your container check [jupyter-docker-stacks](https://jupyter-docker-stacks.readthedocs.io/en/latest/using/common.html) documentation.

## Examples

Reading SINAN files:

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

**Reading `.dbc` file:**

```python
>>> from pysus.utilities.readdbc import read_dbc

>>> df = read_dbc(filename, encoding='iso-8859-1')
>>> df.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1239 entries, 0 to 1238
Data columns (total 58 columns):
AP_MVM        1239 non-null object
AP_CONDIC     1239 non-null object
AP_GESTAO     1239 non-null object
AP_CODUNI     1239 non-null object
AP_AUTORIZ    1239 non-null object
AP_CMP        1239 non-null object
AP_PRIPAL     1239 non-null object
AP_VL_AP      1239 non-null float64
...
```
Downloading and reading SINASC data:

```python
In[1]: from pysus.online_data.sinasc import download
In[2]: df = download('SE', 2015)
In[3]: df.head()
Out[3]: 
   NUMERODN        CODINST ORIGEM    ...     TPROBSON PARIDADE KOTELCHUCK
0  19533794  MSE2805100001      1    ...           11        1          9
1  52927108  MSE2802700001      1    ...           11        1          9
2  54673238  MSE2804400001      1    ...           11        1          5
3  54673239  MSE2804400001      1    ...           10        1          3
4  54695292  MBA2916500001      1    ...           03        1          2
[5 rows x 64 columns]
```

Dowloading and reading SIM data:

```python
In[1]: from pysus.online_data.SIM import download
In[2]: df = download('ba', 2007)
In[3]: df.head()
Out[3]: 
   NUMERODO TIPOBITO   DTOBITO  ...   UFINFORM        CODINST CB_PRE
0  01499664        2  30072007  ...         29  RBA2914800001   C229
1  09798190        2  04072007  ...         29  RBA2914800001    R98
2  01499665        2  25082007  ...         29  RBA2914800001    I10
3  10595623        2  11092007  ...         29  RBA2914800001   G839
4  10599666        2  09082007  ...         29  EBA2927400001   I499
[5 rows x 56 columns]
```

Dowloading and reading CIHA data:

```python
In[1]: from pysus.online_data.CIHA import download
In[2]: df = download('mg', 2009, 7)
In[3]: df.head()
Out[3]: 
  ANO_CMPT MES_CMPT ESPEC        CGC_HOSP  ...  CAR_INT HOMONIMO     CNES FONTE
0     2009       07        16505851000126  ...                    2126796     1
1     2009       07        16505851000126  ...                    2126796     2
2     2009       07        16505851000126  ...                    2126796     6
3     2009       07        16505851000126  ...                    2126796     6
4     2009       07        16505851000126  ...                    2126796     1
[5 rows x 27 columns]
```
