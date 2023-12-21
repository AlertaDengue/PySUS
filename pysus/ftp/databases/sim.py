from typing import List, Union, Optional

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs


class SIM(Database):
    name = "SIM"
    paths = (
        Directory("/dissemin/publicos/SIM/CID10/DORES"),
        Directory("/dissemin/publicos/SIM/CID9/DORES"),
    )
    metadata = {
        "long_name": "Sistema de Informação sobre Mortalidade",
        "source": "http://sim.saude.gov.br",
        "description": "",
    }
    groups = {"CID10": "DO", "CID9": "DOR"}

    def describe(self, file: File) -> dict:
        group, _uf, year = self.format(file)
        _groups = {v: k for k, v in self.groups.items()}

        try:
            uf = UFs[_uf]
        except KeyError:
            uf = _uf

        description = {
            "name": str(file.basename),
            "uf": uf,
            "year": year,
            "group": _groups[group],
            "size": file.info["size"],
            "last_update": file.info["modify"],
        }

        return description

    def format(self, file: File) -> tuple:
        if "CID9" in str(file.path):
            group, _uf, year = file.name[:-4], file.name[-4:-2], file.name[-2:]
        else:
            group, _uf, year = file.name[:-6], file.name[-6:-4], file.name[-4:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[list[str], str],
        uf: Optional[Union[list[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = self.files

        groups = [self.groups[g.upper()] for g in to_list(group)]

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(y) for y in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files
