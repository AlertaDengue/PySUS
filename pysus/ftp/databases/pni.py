from typing import List, Union, Optional, Literal

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs


class PNI(Database):
    name = "PNI"
    paths = (
        Directory("/dissemin/publicos/PNI/DADOS"),
    )
    metadata = {
        "long_name": "Sistema de Informações do Programa Nacional de Imunizações",
        "source": (
            "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/",
        ),
        "description": (
            "O SI-PNI é um sistema desenvolvido para possibilitar aos gestores "
            "envolvidos no Programa Nacional de Imunização, a avaliação dinâmica "
            "do risco quanto à ocorrência de surtos ou epidemias, a partir do "
            "registro dos imunobiológicos aplicados e do quantitativo populacional "
            "vacinado, agregados por faixa etária, período de tempo e área geográfica. "
            "Possibilita também o controle do estoque de imunobiológicos necessário "
            "aos administradores que têm a incumbência de programar sua aquisição e "
            "distribuição. Controla as indicações de aplicação de vacinas de "
            "imunobiológicos especiais e seus eventos adversos, dentro dos Centros "
            "de Referências em imunobiológicos especiais."
        ),
    }
    groups = {
        "CPNI": "Cobertura Vacinal",  # TODO: may be incorrect
        "DPNI": "Doses Aplicadas",  # TODO: may be incorrect
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() in [".DBC", ".DBF"]:
            group, _uf, year = self.format(file)

            try:
                uf = UFs[_uf]
            except KeyError:
                uf = _uf

            description = {
                "name": file.basename,
                "group": self.groups[group],
                "uf": uf,
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }

            return description
        return {}

    def format(self, file: File) -> tuple:

        if len(file.name) != 8:
            raise ValueError(f"Can't format {file.name}")

        n = file.name
        group, _uf, year = n[:4], n[4:6], n[-2:]
        return group, _uf, zfill_year(year)

    def get_files(
        self,
        group: Union[list, Literal["CNPI", "DPNI"]],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = list(filter(
            lambda f: f.extension.upper() in [".DBC", ".DBF"], self.files
        ))

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                "Unknown PNI Group(s): "
                f"{set(groups).difference(list(self.groups))}"
            )

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(m)[-2:]) for m in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        return files
