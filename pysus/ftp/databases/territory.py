from typing import List, Optional, Union

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import to_list, zfill_year


class Territory(Database):
    paths = (Directory("/territorio/tabelas"), Directory("territorio/mapas"))

    def get_files(self):
        return [f for f in self.files if f.extension.upper() == ".ZIP"]
