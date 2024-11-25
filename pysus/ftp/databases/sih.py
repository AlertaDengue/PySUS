from typing import List, Optional, Union

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import MONTHS, UFs, parse_UFs, to_list, zfill_year


class SIH(Database):
    name = "SIH"
    paths = (
        Directory("/dissemin/publicos/SIHSUS/199201_200712/Dados"),
        Directory("/dissemin/publicos/SIHSUS/200801_/Dados"),
    )
    metadata = {
        "long_name": "Sistema de Informações Hospitalares",
        "source": (
            "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",  # noqa
            "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/",  # noqa
        ),
        "description": (
            "A finalidade do AIH (Sistema SIHSUS) é a de transcrever todos os "
            "atendimentos que provenientes de internações hospitalares que "
            "foram financiadas pelo SUS, e após o processamento, gerarem "
            "relatórios para os gestores que lhes possibilitem fazer os "
            "pagamentos dos estabelecimentos de saúde. Além disso, o nível "
            "Federal recebe mensalmente uma base de dados de todas as "
            "internações autorizadas (aprovadas ou não para pagamento) para "
            "que possam ser repassados às Secretarias de Saúde os valores de "
            "Produção de Média e Alta complexidade além dos valores de CNRAC, "
            "FAEC e de Hospitais Universitários – em suas variadas formas de "
            "contrato de gestão."
        ),
    }
    groups = {
        "RD": "AIH Reduzida",
        "RJ": "AIH Rejeitada",
        "ER": "AIH Rejeitada com erro",
        "SP": "Serviços Profissionais",
        "CH": "Cadastro Hospitalar",
        "CM": "",  # TODO
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() in [".DBC", ".DBF"]:
            group, _uf, year, month = self.format(file)

            try:
                uf = UFs[_uf]
            except KeyError:
                uf = _uf

            description = {
                "name": file.basename,
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
        files = list(
            filter(
                lambda f: f.extension.upper() in [".DBC", ".DBF"], self.files
            )
        )

        groups = [gr.upper() for gr in to_list(group)]

        if not all(gr in list(self.groups) for gr in groups):
            raise ValueError(
                f"Unknown SIH Group(s): "
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
