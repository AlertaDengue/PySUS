from typing import List, Union, Optional

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs, MONTHS


class SIA(Database):
    name = "SIA"
    paths = (
        Directory("/dissemin/publicos/SIASUS/199407_200712/Dados"),
        Directory("/dissemin/publicos/SIASUS/200801_/Dados"),
    )
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
        "PAR": "",  # TODO
        "PAS": "",  # TODO
        "PS": "RAAS Psicossocial",
        "SAD": "RAAS de Atenção Domiciliar",
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() == ".DBC":
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
        if file.extension.upper() in [".DBC", ".DBF"]:
            digits = ''.join([d for d in file.name if d.isdigit()])
            chars, _ = file.name.split(digits)
            year, month = digits[:2], digits[2:]
            group, _uf = chars[:-2].upper(), chars[-2:].upper()
            return group, _uf, zfill_year(year), month
        return ()

    def get_files(
        self,
        group: Union[List[str], str],
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
    ) -> List[File]:
        files = list(filter(
            lambda f: f.extension.upper() in [".DBC", ".DBF"], self.files
        ))

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                "Unknown SIA Group(s): "
                f"{set(groups).difference(list(self.groups))}"
            )

        files = list(filter(lambda f: self.format(f)[0] in groups, files))

        if uf:
            ufs = parse_UFs(uf)
            files = list(filter(lambda f: self.format(f)[1] in ufs, files))

        if year or str(year) in ["0", "00"]:
            years = [zfill_year(str(m)[-2:]) for m in to_list(year)]
            files = list(filter(lambda f: self.format(f)[2] in years, files))

        if month:
            months = [str(y)[-2:].zfill(2) for y in to_list(month)]
            files = list(filter(lambda f: self.format(f)[3] in months, files))

        return files
