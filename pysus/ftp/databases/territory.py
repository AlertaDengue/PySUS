from typing import Optional, List, Union

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list

class Territory(Database):
    paths = (
        Directory('/territorio/tabelas'),
        Directory('territorio/mapas')
    )

    def get_files(self):
        return [f for f in self.files if f.extension.upper() == '.ZIP']