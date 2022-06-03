# PySUS
[![DOI](https://zenodo.org/badge/63720586.svg)](https://zenodo.org/badge/latestdoi/63720586)

This package collects a set of utilities for handling with public databases published by Brazil's DATASUS
The documentation of how to use PySUS can be found [here](http://pysus.readthedocs.io/en/latest/)

If you use PySUS for a publication,  please use the bibtex below to cite it:
```bibtex
@software{flavio_codeco_coelho_2021_4883502,
  author       = {Flávio Codeço Coelho and
                  Bernardo Chrispim Baron and
                  Gabriel Machado de Castro Fonseca and
                  Pedro Reck and
                  Daniela Palumbo},
  title        = {AlertaDengue/PySUS: Vaccine},
  month        = may,
  year         = 2021,
  publisher    = {Zenodo},
  version      = {0.5.17},
  doi          = {10.5281/zenodo.4883502},
  url          = {https://doi.org/10.5281/zenodo.4883502}
}
```

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

## Changing cache directory

You can change the default directory where PySUS stores files downloaded from DataSUS public repository by setting an environment variable called `PYSUS_CACHEPATH` with the desired location. If the folder does not exist, it will be created on the package's first invocation.

In MacOS or an Unix-based system, run:

```bash
export PYSUS_CACHEPATH="/home/me/desired/path/.pysus"
```

You can also add this line at the end of your `~/.profile` or `~/.bashrc` files to make this setting persist.

In Windows, you can set a new environment variable by running:

```PowerShell
setx PYSUS_CACHEPATH "C:\Users\Me\desired\path\.pysus"
```

In Docker, just add an extra parameter `-e PYSUS_CACHEPATH="/home/me/desired/path/.pysus"` when starting the container:

```bash
docker run -p 8888:8888 -e PYSUS_CACHEPATH="/home/me/desired/path/.pysus" pysus:latest 
```

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

Dowloading and reading SIA data:

```python
In[1]: from pysus.online_data.SIA import download
In[2]: bi, ps = download('AC', 2020, 3, group=["BI", "PS"])
In[3]: bi.head()
Out[3]: 
    CODUNI  GESTAO CONDIC   UFMUN TPUPS  ... VL_APROV UFDIF MNDIF ETNIA NAT_JUR
0  2000733  120000     EP  120040    07  ...     24.2     0     0          1023
1  2001063  120000     EP  120040    36  ...      7.3     0     0          1023
2  2001063  120000     EP  120040    36  ...      7.3     0     0          1023
3  2001586  120000     EP  120040    05  ...     38.1     0     0          1147
4  2000083  120000     EP  120033    05  ...     64.8     0     0          1023
[5 rows x 36 columns]
In[4]: ps.head()
Out[4]:
  CNES_EXEC  GESTAO CONDIC   UFMUN  ... PERMANEN QTDATE QTDPCN NAT_JUR
0   2002094  120000     EP  120040  ...       30      1      1    1023
1   2002094  120000     EP  120040  ...               0      0    1023
2   2002094  120000     EP  120040  ...               0      0    1023
3   2002094  120000     EP  120040  ...               0      0    1023
4   2002094  120000     EP  120040  ...               0      0    1023
[5 rows x 45 columns]
```
