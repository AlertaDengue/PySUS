from typing import List, Literal, Optional, Union

from loguru import logger
from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import to_list, zfill_year


class IBGEDATASUS(Database):
    name = "IBGE-DataSUS"
    paths = (
        Directory("/dissemin/publicos/IBGE/POP"),
        Directory("/dissemin/publicos/IBGE/censo"),
        Directory("/dissemin/publicos/IBGE/POPTCU"),
        Directory("/dissemin/publicos/IBGE/projpop"),
        # Directory("/dissemin/publicos/IBGE/Auxiliar") # this has a different file name pattern
    )
    metadata = {
        "long_name": "Populaçao Residente, Censos, Contagens "
        "Populacionais e Projeçoes Intercensitarias",
        "source": "ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE",
        "description": (
            "São aqui apresentados informações sobre a população residente, "
            "estratificadas por município, faixas etárias e sexo, obtidas a "
            "partir dos Censos Demográficos, Contagens Populacionais "
            "e Projeções Intercensitárias."
        ),
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() in [".ZIP"]:
            year = file.name.split(".")[0][-2:]
            description = {
                "name": str(file.basename),
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }
            return description
        elif file.extension.upper() == ".DBF":
            year = file.name[-2:]
            description = {
                "name": str(file.basename),
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }
            return description
        return {}

    def format(self, file: File) -> tuple:
        return (file.name[-2:],)

    def get_files(
        self,
        source: Literal["POP", "censo", "POPTCU", "projpop"] = "POPTCU",
        year: Optional[Union[str, int, list]] = None,
        *args,
        **kwargs,
    ) -> List[File]:
        sources = ["POP", "censo", "POPTCU", "projpop"]
        source_dir = None

        for dir in self.paths:
            if source in sources and source in dir.path:
                source_dir = dir

        if not source_dir:
            raise ValueError(f"Unkown source {source}. Options: {sources}")

        files = source_dir.content

        if year:
            if isinstance(year, (str, int)):
                files = [
                    f
                    for f in files
                    if self.describe(f)["year"] == zfill_year(year)
                ]
            elif isinstance(year, list):
                files = [
                    f
                    for f in files
                    if str(self.describe(f)["year"])
                    in [str(zfill_year(y)) for y in year]
                ]

        return files
