from typing import List, Union
import humanize

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs


class SINASC(Database):
    name = "SINASC"
    paths = [
        Directory("/dissemin/publicos/SINASC/NOV/DNRES"),
        Directory("/dissemin/publicos/SINASC/ANT/DNRES"),
    ]
    metadata = {
        "long_name": "Sistema de Informações sobre Nascidos Vivos",
        "source": "http://sinasc.saude.gov.br/",
        "description": "",
    }
    groups = {
        "DN": "Declarações de Nascidos Vivos",
        "DNR": "Dados dos Nascidos Vivos por UF de residência",
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() == ".DBC":
            uf, year = self.format(file)

            if uf == "EX":  # DNEX2021.dbc
                state = None
            else:
                state = UFs[uf]

            description = {
                "name": str(file.basename),
                "uf": state,
                "year": year,
                "size": humanize.naturalsize(file.info["size"]),
                "last_update": file.info["modify"].strftime("%m-%d-%Y %I:%M%p"),
            }

            return description
        return {}

    def format(self, file: File) -> tuple:
        if file.name == "DNEX2021":
            pass

        year = zfill_year(file.name[-2:])
        charname = "".join([c for c in file.name if not c.isnumeric()])
        uf = charname[-2:]
        return uf, zfill_year(year)

    def get_files(
        self,
        ufs: Union[list[str], str],
        years: Union[list, str, int],
    ) -> List[File]:
        if "EX" in to_list(ufs):
            # DNEX2021
            if len(to_list(ufs)) == 1:
                return []

            to_list(ufs).remove("EX")

        ufs = parse_UFs(ufs)
        years = [str(y)[-2:].zfill(2) for y in to_list(years)]

        # Fist filter years to reduce the list size
        year_files = []
        for file in self.files:
            if str(file.info["modify"].year) in years:
                year_files.append(file)

        files = []
        for file in year_files:
            if "ANT/DNRES" in str(file.path):
                for uf in ufs:
                    if uf in file.name[3:]:
                        files.append(file)
            else:
                for uf in ufs:
                    if uf in file.name[2:]:
                        files.append(file)

        return files
