from itertools import product
from typing import List, Union
import humanize

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs


class SIM(Database):
    name = "SIM"
    paths = [
        Directory("/dissemin/publicos/SIM/CID10/DORES"),
        Directory("/dissemin/publicos/SIM/CID9/DORES"),
    ]
    metadata = {
        "long_name": "Sistema de Informação sobre Mortalidade",
        "source": "http://sim.saude.gov.br",
        "description": "",
    }
    groups = {"DO": "CID10", "DOR": "CID9"}

    def describe(self, file: File) -> dict:
        group, uf, year = self.format(file)

        description = dict(
            name=str(file.basename),
            uf=UFs[uf],
            year=year,
            group=self.groups[group],
            size=humanize.naturalsize(file.info["size"]),
            last_update=file.info["modify"].strftime("%m-%d-%Y %I:%M%p"),
        )

        return description

    def format(self, file: File) -> tuple:
        if "CID9" in str(file.path):
            group, uf, year = file.name[:-4], file.name[-4:-2], file.name[-2:]
        else:
            group, uf, year = file.name[:-6], file.name[-6:-4], file.name[-4:]

        return group, uf, zfill_year(year)

    def get_files(
        self,
        groups: Union[list[str], str],
        ufs: Union[list[str], str],
        years: Union[list, str, int],
    ) -> List[File]:
        groups = [g.upper() for g in to_list(groups)]
        ufs = parse_UFs(ufs)
        years = to_list(years)

        if not all(gr in list(self.groups.values()) for gr in groups):
            raise ValueError(
                "Unknown group(s): "
                f"{set(groups).difference(self.groups.values())}"
            )

        targets = []
        for group in groups:
            if group == "CID9":
                cid9_years = [str(y)[-2:].zfill(2) for y in years]
                targets.extend(
                    [
                        f"DOR{uf}{year}"
                        for uf, year in list(product(ufs, cid9_years))
                    ]
                )
            elif group == "CID10":
                cid10_years = [zfill_year(y) for y in years]
                targets.extend(
                    [
                        f"DO{uf}{year}"
                        for uf, year in list(product(ufs, cid10_years))
                    ]
                )

        files = []
        for file in self.files:
            if file.name in targets:
                files.append(file)

        return files

