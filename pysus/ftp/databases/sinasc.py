from typing import List, Union, Optional

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs


class SINASC(Database):
    name = "SINASC"
    paths = (
        Directory("/dissemin/publicos/SINASC/NOV/DNRES"),
        Directory("/dissemin/publicos/SINASC/ANT/DNRES"),
    )
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
            group, _uf, year = self.format(file)

            try:
                uf = UFs[_uf]
            except KeyError:
                uf = _uf

            description = {
                "name": file.basename,
                "group": self.groups[group],
                "uf": uf,
                "year": year,
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }

            return description
        return {}

    def format(self, file: File) -> tuple:
        if file.name == "DNEX2021":
            pass

        year = zfill_year(file.name[-2:])
        charname = "".join([c for c in file.name if not c.isnumeric()])
        group, _uf = charname[:-2], charname[-2:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[List, str, int]] = None,
    ) -> List[File]:
        files = self.files

        groups = to_list(group)

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            if "EX" in to_list(uf):
                # DNEX2021
                if len(to_list(uf)) == 1:
                    return []

                to_list(uf).remove("EX")

            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(y)[-2:]) for y in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files
