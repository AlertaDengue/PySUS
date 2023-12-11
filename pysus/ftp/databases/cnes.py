from typing import List, Union, Optional

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
    __loaded__ = set()

    def load(
        self,
        groups: Union[str, List[str]] = None,
    ):
        """
        Loads CNES Groups into content. Will convert the files and directories 
        found within FTP Directories into self.content
        """
        if not self.__content__:
            self.paths.load()
            self.__content__ |= self.paths.__content__

        if groups:
            groups = to_list(groups)

            if not all(
                group in self.groups for group in [gr.upper() for gr in groups]
            ):
                raise ValueError(
                    "Unknown CNES group(s): "
                    f"{set(groups).difference(self.groups)}"
                )

            for group in groups:
                group = group.upper()
                if group not in self.__loaded__:
                    directory = self.__content__[group]
                    directory.load()
                    self.__content__ |= directory.__content__
                    self.__loaded__.add(directory.name)
        return self

    def describe(self, file: File) -> dict:
        if not isinstance(file, File):
            return {}

        if file.name == "GMufAAmm":
            # Leftover
            return {}

        if file.extension.upper() in [".DBC", ".DBF"]:
            group, _uf, year, month = self.format(file)

            try:
                uf = UFs[_uf]
            except KeyError:
                uf = _uf

            description = {
                "name": str(file.basename),
                "group": self.groups[group],
                "uf": uf,
                "month": MONTHS[int(month)],
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }

            return description
        return {}

    def format(self, file: File) -> tuple:
        group, _uf = file.name[:2].upper(), file.name[2:4].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, _uf, zfill_year(year), month

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        if not group:
            raise ValueError("At least one CNES group is required")

        groups = [gr.upper() for gr in to_list(group)]

        self.load(groups)

        files = list(filter(lambda f: f.name[:2] in groups, self.files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: f.name[2:4] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [str(m)[-2:].zfill(2) for m in to_list(year)]
            files = list(filter(lambda f: f.name[-4:-2] in years, files))

        if month:
            months = [str(y)[-2:].zfill(2) for y in to_list(month)]
            files = list(filter(lambda f: f.name[-2:] in months, files))

        return files
