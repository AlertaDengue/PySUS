from typing import List, Union
from itertools import product
import humanize

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list, parse_UFs, UFs, MONTHS


class SIH(Database):
    name = "SIH"
    paths = [
        Directory("/dissemin/publicos/SIHSUS/199201_200712/Dados"),
        Directory("/dissemin/publicos/SIHSUS/200801_/Dados"),
    ]
    metadata = {
        "long_name": "Sistema de Informações Hospitalares",
        "source": (
            "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            "https://datasus.saude.gov.br/acesso-a-informacao/producao-hospitalar-sih-sus/",
        ),
        "description": (
            "A finalidade do AIH (Sistema SIHSUS) é a de transcrever todos os "
            "atendimentos que provenientes de internações hospitalares que "
            "foram financiadas pelo SUS, e após o processamento, gerarem "
            "relatórios para os gestores que lhes possibilitem fazer os pagamentos "
            "dos estabelecimentos de saúde. Além disso, o nível Federal recebe "
            "mensalmente uma base de dados de todas as internações autorizadas "
            "(aprovadas ou não para pagamento) para que possam ser repassados às "
            "Secretarias de Saúde os valores de Produção de Média e Alta complexidade "
            "além dos valores de CNRAC, FAEC e de Hospitais Universitários – em suas "
            "variadas formas de contrato de gestão."
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
        if file.extension.upper() == ".DBC":
            group, uf, year, month = self.format(file)

            description = dict(
                name=file.basename,
                group=self.groups[group],
                uf=UFs[uf],
                month=MONTHS[int(month)],
                year=zfill_year(year),
                size=humanize.naturalsize(file.info["size"]),
                last_update=file.info["modify"].strftime("%m-%d-%Y %I:%M%p"),
            )

            return description
        return {}

    def format(self, file: File) -> tuple:
        group, uf = file.name[:2].upper(), file.name[2:4].upper()
        year, month = file.name[-4:-2], file.name[-2:]
        return group, uf, zfill_year(year), month

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
            if file.name[:2] in groups:
                groups_files.append(file)

        targets = ["".join(t) for t in product(ufs, months, years)]

        files = []
        for file in groups_files:
            if file.name[2:] in targets:
                files.append(file)

        return files
