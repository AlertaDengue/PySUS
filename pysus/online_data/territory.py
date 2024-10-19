
from pathlib import Path
from typing import Union, Dict
import pandas as pd

from pysus.ftp import CACHEPATH
from pysus.ftp.databases.territory import Territory

ter = Territory().load()

def list_tables()-> Dict:
    ter.files
    return ter.get_files()

def download(fname: Union[str,list], data_path: str = CACHEPATH):
    files = ter.get_files()
    
    dfiles = ter.download(list(fname), data_path)


