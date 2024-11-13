
from pathlib import Path
from typing import Union, Dict, List
import pandas as pd

from pysus.ftp import CACHEPATH, File, Directory
from pysus.ftp.databases.territory import Territory

ter = Territory().load()

def list_tables()-> List[File]:
    d = Directory('/territorio/tabelas')
    tabelas = [f for f in d.content if 'territor' in f.name]
    return tabelas

def list_maps()-> List[File]:
    d = Directory('/territorio/mapas')
    mapas = [f for f in d.content if 'mapas' in f.name]
    return mapas

def download(fname: Union[str,list], data_path: str = CACHEPATH):
    files = Directory('/territorio/tabelas').content + Directory('/territorio/mapas').content
    for file in files:
        if fname in [str(file), file.name]:  # handles suffixed and no suffixed `fname`s
            return file.download()


