from typing import List, Union, Optional

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs, MONTHS


class CIHA(Database):
    name = "CIHA"
    paths = (Directory("/dissemin/publicos/CIHA/201101_/Dados"))
    metadata = {
        "long_name": "Comunicação de Internação Hospitalar e Ambulatorial",
        "source": "http://ciha.datasus.gov.br/CIHA/index.php",
        "description": (
            "A CIHA foi criada para ampliar o processo de planejamento, programação, "
            "controle, avaliação e regulação da assistência à saúde permitindo um "
            "conhecimento mais abrangente e profundo dos perfis nosológico e "
            "epidemiológico da população brasileira, da capacidade instalada e do "
            "potencial de produção de serviços do conjunto de estabelecimentos de saúde "
            "do País. O sistema permite o acompanhamento das ações e serviços de saúde "
            "custeados por: planos privados de assistência à saúde; planos públicos; "
            "pagamento particular por pessoa física; pagamento particular por pessoa "
            "jurídica; programas e projetos federais (PRONON, PRONAS, PROADI); recursos "
            "próprios das secretarias municipais e estaduais de saúde; DPVAT; gratuidade "
            "e, a partir da publicação da Portaria GM/MS nº 2.905/2022, consórcios públicos. "
            "As informações registradas na CIHA servem como base para o processo de "
            "Certificação de Entidades Beneficentes de Assistência Social em Saúde (CEBAS) "
            "e para monitoramento dos programas PRONAS e PRONON."
        ),
    }
    groups = {
        "CIHA": "Comunicação de Internação Hospitalar e Ambulatorial",
    }

    def describe(self, file: File):
        if not isinstance(file, File):
            return file

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
        return file

    def format(self, file: File) -> tuple:
        group, _uf = file.name[:4].upper(), file.name[4:6].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, _uf, zfill_year(year), month

    def get_files(
        self,
        uf: Optional[Union[List[str], str]] = None,
        year: Optional[Union[list, str, int]] = None,
        month: Optional[Union[list, str, int]] = None,
        group: Union[List[str], str] = "CIHA",
    ) -> List[File]:
        files = list(filter(
            lambda f: f.extension.upper() in [".DBC", ".DBF"], self.files
        ))

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                "Unknown CIHA Group(s): "
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
