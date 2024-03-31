from typing import Optional, List, Union

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list


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
            year = file.name.split('.')[0][-2:]
            description = {
                "name": str(file.basename),
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"]
            }
            return description
        elif file.extension.upper() == ".DBF":
            year = file.name[-2:]
            description = {
                "name": str(file.basename),
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"]
            }
            return description
        return {}

    def format(self, file: File) -> str:
        return file.name[-2:]

    def get_files(
            self,
            year: Optional[Union[str, int, list]] = None,
    ) -> List[File]:
        files = [f for f in self.files if f.extension.upper() in [".ZIP", ".DBF"] and self.describe(f)["year"] == year]
        # files = list(filter(
        #     lambda f: f.extension.upper() in [".ZIP"], self.files
        # ))

        if year or str(year) in ["0", "00"]:
            years = (
                [zfill_year(str(y)[-4:]) for y in to_list(year)]
            )
            files = list(filter(lambda f: zfill_year(self.format(f)) in years, files))

        return files
