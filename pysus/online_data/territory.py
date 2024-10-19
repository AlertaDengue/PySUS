
from pathlib import Path
from typing import Union, Dict
import pandas as pd

from pysus.ftp import CACHEPATH
from pysus.ftp.databases.territory import Territory

ter = Territory().load()

def list_tables()-> Dict:
    files = ter.get_files()
    tabelas = [f for f in files if 'territor' in f.name]
    return tabelas

def list_maps():
    files = ter.get_files()
    mapas = [f for f in files if 'mapas' in f.name]
    return mapas

def download(fname: Union[str,list], data_path: str = CACHEPATH):
    files = ter.get_files()
    
    dfiles = ter.download(list(fname), data_path)


