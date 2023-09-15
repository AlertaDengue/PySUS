from typing import List, Union
from itertools import product

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs, MONTHS


class SIA(Database):
    name = "SIA"
    paths = [
        Directory("/dissemin/publicos/SIASUS/199407_200712/Dados"),
        Directory("/dissemin/publicos/SIASUS/200801_/Dados"),
    ]
    metadata = {
        "long_name": "Sistema de Informações Ambulatoriais",
        "source": "http://sia.datasus.gov.br/principal/index.php",
        "description": (
            "O Sistema de Informação Ambulatorial (SIA) foi instituído pela Portaria "
            "GM/MS n.º 896 de 29 de junho de 1990. Originalmente, o SIA foi concebido "
            "a partir do projeto SICAPS (Sistema de Informação e Controle Ambulatorial "
            "da Previdência Social), em que os conceitos, os objetivos e as diretrizes "
            "criados para o desenvolvimento do SICAPS foram extremamente importantes e "
            "amplamente utilizados para o desenvolvimento do SIA, tais como: (i) o "
            "acompanhamento das programações físicas e orçamentárias; (ii) o "
            "acompanhamento das ações de saúde produzidas; (iii) a agilização do "
            "pagamento e controle orçamentário e financeiro; e (iv) a formação de "
            "banco de dados para contribuir com a construção do SUS."
        ),
    }
    groups = {
        "AB": "APAC de Cirurgia Bariátrica",
        "ABO": "APAC de Acompanhamento Pós Cirurgia Bariátrica",
        "ACF": "APAC de Confecção de Fístula",
        "AD": "APAC de Laudos Diversos",
        "AM": "APAC de Medicamentos",
        "AMP": "APAC de Acompanhamento Multiprofissional",
        "AN": "APAC de Nefrologia",
        "AQ": "APAC de Quimioterapia",
        "AR": "APAC de Radioterapia",
        "ATD": "APAC de Tratamento Dialítico",
        "BI": "Boletim de Produção Ambulatorial individualizado",
        "IMPBO": "",  # TODO
        "PA": "Produção Ambulatorial",
        "PAM": "",  # TODO
        "PAR": "", # TODO
        "PAS": "", # TODO
        "PS": "RAAS Psicossocial",
        "SAD": "RAAS de Atenção Domiciliar",
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() == ".DBC":
            group, _uf, year, month = self.format(file)

            description = {
                "name": str(file.basename),
                "group": self.groups[group],
                "uf": UFs[_uf],
                "month": MONTHS[int(month)],
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"],
            }

            return description
        return {}

    def format(self, file: File) -> tuple:
        if file.extension.upper() == ".DBC":
            year, month = file.name[-4:-2], file.name[-2:]
            group, _uf = file.name[:-6].upper(), file.name[-6:-4].upper()
            return group, _uf, zfill_year(year), month
        return ()

    def get_files(
        self,
        groups: Union[List[str], str],
        ufs: Union[List[str], str],
        months: Union[list, str, int],
        years: Union[list, str, int],
    ) -> List[File]:
        groups = [gr.upper() for gr in to_list(groups)]
        ufs = parse_UFs(ufs)
        months = [str(y)[-2:].zfill(2) for y in to_list(months)]
        years = [str(m)[-2:].zfill(2) for m in to_list(years)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                f"Unknown SIH Group(s): {set(groups).difference(list(self.groups))}"
            )

        # Fist filter files by group to reduce the files list length
        groups_files = []
        for file in self.files:
            if file.name[:-6] in groups:
                groups_files.append(file)

        targets = ["".join(t) for t in product(ufs, years, months)]

        files = []
        for file in groups_files:
            if file.name[-6:] in targets:
                files.append(file)

        return files
