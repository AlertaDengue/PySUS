from typing import List, Union
from itertools import product
import humanize

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs, MONTHS


class CNES(Database):
    name = "CNES"
    paths = Directory("/dissemin/publicos/CNES/200508_/Dados")
    metadata = {
        "long_name": "Cadastro Nacional de Estabelecimentos de Saúde",
        "source": "https://cnes.datasus.gov.br/",
        "description": (
            "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o sistema "
            "de informação oficial de cadastramento de informações de todos os "
            "estabelecimentos de saúde no país, independentemente de sua natureza "
            "jurídica ou de integrarem o Sistema Único de Saúde (SUS). Trata-se do "
            "cadastro oficial do Ministério da Saúde (MS) no tocante à realidade da "
            "capacidade instalada e mão-de-obra assistencial de saúde no Brasil em "
            "estabelecimentos de saúde públicos ou privados, com convênio SUS ou não."
        ),
    }
    groups = {
        "DC": "Dados Complementares",
        "EE": "Estabelecimento de Ensino",
        "EF": "Estabelecimento Filantrópico",
        "EP": "Equipes",
        "EQ": "Equipamentos",
        "GM": "Gestão e Metas",
        "HB": "Habilitação",
        "IN": "Incentivos",
        "LT": "Leitos",
        "PF": "Profissional",
        "RC": "Regra Contratual",
        "SR": "Serviço Especializado",
        "ST": "Estabelecimentos",
    }
    __loaded__ = []

    def load(
        self,
        groups: Union[str, List[str]] = None,
    ):
        """
        Loads specific paths to Database content, can receive CNES Groups as well.
        It will convert the files found within the paths into content.
        """
        if not self.__content__:
            self.paths.load()
            self.__content__.update(self.paths.content)

        if groups:
            groups = to_list(groups)

            if not all(
                group in self.groups for group in [gr.upper() for gr in groups]
            ):
                raise ValueError(
                    f"Unknown CNES group(s): {set(groups).difference(self.groups)}"
                )

            dirs = list(
                filter(lambda c: isinstance(c, Directory), self.__content__)
            )

            for directory in dirs:
                if directory.name in [gr.upper() for gr in groups]:
                    self.__content__.update(directory.content)

        return self

    def describe(self, file: File):
        if not isinstance(file, File):
            return file

        if file.extension.upper() in [".DBC", ".DBF"]:
            group, uf, year, month = self.format(file)

            description = {
                "name": str(file.basename),
                "group": self.groups[group],
                "uf": UFs[uf],
                "month": MONTHS[int(month)],
                "year": zfill_year(year),
                "size": humanize.naturalsize(file.info["size"]),
                "last_update": file.info["modify"].strftime(
                    "%m-%d-%Y %I:%M%p"
                ),
            }

            return description
        return file

    def format(self, file: File) -> tuple:
        group, uf = file.name[:2].upper(), file.name[2:4].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, uf, zfill_year(year), month

    def get_files(
        self,
        groups: Union[List[str], str],
        ufs: Union[List[str], str],
        years: Union[list, str, int],
        months: Union[list, str, int],
    ) -> List[File]:
        groups = [gr.upper() for gr in to_list(groups)]
        ufs = parse_UFs(ufs)
        years = [str(m)[-2:].zfill(2) for m in to_list(years)]
        months = [str(y)[-2:].zfill(2) for y in to_list(months)]

        if not all([gr in list(self.groups) for gr in groups]):
            raise ValueError(
                f"Unknown CNES Group(s): {set(groups).difference(list(self.groups))}"
            )

        for group in groups:
            if group not in self.__loaded__:
                self.load(groups=group)

        targets = ["".join(t) for t in product(groups, ufs, years, months)]

        return [f for f in self.files if f.name in targets]
