from typing import Any, Dict, List

from pysus.api.ftp.models import Dataset, Directory
from pysus.utils import zfill_year


class CIHA(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/CIHA/201101_/Dados"),
    ]

    group_definitions: Dict[str, str] = {
        "CIHA": "Comunicação de Internação Hospitalar e Ambulatorial",
    }

    @property
    def name(self) -> str:
        return "CIHA"

    @property
    def long_name(self) -> str:
        return "Comunicação de Internação Hospitalar e Ambulatorial"

    @property
    def description(self) -> str:
        return (
            "A CIHA foi criada para ampliar o processo de planejamento, "
            "programação, controle, avaliação e regulação da assistência à "
            "saúde permitindo um conhecimento mais abrangente e profundo dos "
            "perfis nosológico e epidemiológico da população brasileira."
        )

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            group_code = name[:4]
            state = name[4:6]
            year_short = name[6:8]
            month = name[8:10]

            group_info = None
            if group_code in self.group_definitions:
                group_info = {
                    "name": group_code,
                    "long_name": self.group_definitions[group_code],
                    "description": None,
                }

            return {
                "group": group_info,
                "state": state,
                "year": int(zfill_year(year_short)),
                "month": int(month),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None, "month": None}


class CNES(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/CNES/200508_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
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

    @property
    def name(self) -> str:
        return "CNES"

    @property
    def long_name(self) -> str:
        return "Cadastro Nacional de Estabelecimentos de Saúde"

    @property
    def description(self) -> str:
        return (
            "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o "
            "sistema de informação oficial de cadastramento de informações "
            "de todos os estabelecimentos de saúde no país."
        )

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            group_code = name[:2]
            state = name[2:4]
            year_short = name[4:6]
            month = name[6:8]

            group_info = None
            if group_code in self.group_definitions:
                group_info = {
                    "name": group_code,
                    "long_name": self.group_definitions[group_code],
                }

            return {
                "group": group_info,
                "state": state,
                "year": int(zfill_year(year_short)),
                "month": int(month),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None, "month": None}


class SINASC(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SINASC/NOV/DNRES"),
        Directory("/dissemin/publicos/SINASC/ANT/DNRES"),
    ]
    group_definitions: Dict[str, str] = {
        "DN": "Declarações de Nascidos Vivos",
        "DNR": "Nascidos Vivos por UF de residência",
    }

    @property
    def name(self) -> str:
        return "SINASC"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações sobre Nascidos Vivos"

    @property
    def description(self) -> str:
        return """
            O SINASC fornece subsídios para o diagnóstico de saúde e
            planejamento de políticas.
        """

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            year_short = name[-2:]
            charname = "".join([c for c in name if not c.isnumeric()])
            group_code, state = charname[:-2], charname[-2:]

            return {
                "group": {
                    "name": group_code,
                    "long_name": self.group_definitions.get(group_code),
                },
                "state": state,
                "year": int(zfill_year(year_short)),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None}


class SIM(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIM/CID10/DORES"),
        Directory("/dissemin/publicos/SIM/CID9/DORES"),
    ]
    group_definitions: Dict[str, str] = {
        "DO": "Mortalidade Geral (CID-10)",
        "DOR": "Mortalidade Geral (CID-9)",
    }

    @property
    def name(self) -> str:
        return "SIM"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação sobre Mortalidade"

    @property
    def description(self) -> str:
        return """
            O SIM coleta dados sobre óbitos no país para análise epidemiológica.
        """

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            if "CID9" in filename:
                group_code, state, year = name[:-4], name[-4:-2], name[-2:]
            else:
                group_code, state, year = name[:-6], name[-6:-4], name[-4:]

            return {
                "group": {
                    "name": group_code,
                    "long_name": self.group_definitions.get(group_code),
                },
                "state": state,
                "year": int(zfill_year(year)),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None}


class PNI(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/PNI/DADOS"),
    ]
    group_definitions: Dict[str, str] = {
        "CPNI": "Cobertura Vacinal",
        "DPNI": "Doses Aplicadas",
    }

    @property
    def name(self) -> str:
        return "PNI"

    @property
    def long_name(self) -> str:
        return "Programa Nacional de Imunizações"

    @property
    def description(self) -> str:
        return "O SI-PNI monitora a cobertura vacinal e doses aplicadas."

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            group_code, state, year_short = name[:4], name[4:6], name[-2:]

            return {
                "group": {
                    "name": group_code,
                    "long_name": self.group_definitions.get(group_code),
                },
                "state": state,
                "year": int(zfill_year(year_short)),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None}


class IBGEDATASUS(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/IBGE/POP"),
        Directory("/dissemin/publicos/IBGE/censo"),
        Directory("/dissemin/publicos/IBGE/POPTCU"),
        Directory("/dissemin/publicos/IBGE/projpop"),
    ]

    @property
    def name(self) -> str:
        return "IBGE"

    @property
    def long_name(self) -> str:
        return "População Residente e Projeções (IBGE)"

    @property
    def description(self) -> str:
        return "Informações sobre a população residente obtidas de Censos."

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            year_short = name[-2:]
            return {
                "group": {"name": "POP", "long_name": "População"},
                "year": int(zfill_year(year_short)),
            }
        except (IndexError, ValueError):
            return {"group": None, "year": None}


class SIA(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIASUS/199407_200712/Dados"),
        Directory("/dissemin/publicos/SIASUS/200801_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
        "PA": "Produção Ambulatorial",
        "BI": "Boletim de Produção Ambulatorial Individualizado",
        "AD": "APAC de Laudos Diversos",
        "AM": "APAC de Medicamentos",
        "AN": "APAC de Nefrologia",
        "AQ": "APAC de Quimioterapia",
        "AR": "APAC de Radioterapia",
    }

    @property
    def name(self) -> str:
        return "SIA"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações Ambulatoriais"

    @property
    def description(self) -> str:
        return "O SIA acompanha as ações de saúde produzidas."

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            digits = "".join([d for d in name if d.isdigit()])
            chars = name.split(digits)[0]

            group_code = chars[:-2]
            state = chars[-2:]
            year_short = digits[:2]
            month = digits[2:]

            group_info = None
            if group_code in self.group_definitions:
                group_info = {
                    "name": group_code,
                    "long_name": self.group_definitions[group_code],
                }

            return {
                "group": group_info,
                "state": state,
                "year": int(zfill_year(year_short)),
                "month": int(month),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None, "month": None}


class SIH(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SIHSUS/199201_200712/Dados"),
        Directory("/dissemin/publicos/SIHSUS/200801_/Dados"),
    ]
    group_definitions: Dict[str, str] = {
        "RD": "AIH Reduzida",
        "RJ": "AIH Rejeitada",
        "SP": "Serviços Profissionais",
        "ER": "AIH Rejeitada com Erro",
    }

    @property
    def name(self) -> str:
        return "SIH"

    @property
    def long_name(self) -> str:
        return "Sistema de Informações Hospitalares"

    @property
    def description(self) -> str:
        return """
            O SIH processa as internações hospitalares financiadas pelo SUS.
        """

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            group_code = name[:2]
            state = name[2:4]
            year_short = name[4:6]
            month = name[6:8]

            return {
                "group": {
                    "name": group_code,
                    "long_name": self.group_definitions.get(group_code),
                },
                "state": state,
                "year": int(zfill_year(year_short)),
                "month": int(month),
            }
        except (IndexError, ValueError):
            return {"group": None, "state": None, "year": None, "month": None}


class SINAN(Dataset):
    paths: List[Directory] = [
        Directory("/dissemin/publicos/SINAN/DADOS/FINAIS"),
        Directory("/dissemin/publicos/SINAN/DADOS/PRELIM"),
    ]

    group_definitions: Dict[str, str] = {
        "DENG": "Dengue",
        "ZIKA": "Zika Vírus",
        "CHIK": "Febre de Chikungunya",
        "HANS": "Hanseníase",
        "TUBE": "Tuberculose",
        "ANIM": "Acidente por Animais Peçonhentos",
    }

    @property
    def name(self) -> str:
        return "SINAN"

    @property
    def long_name(self) -> str:
        return "Sistema de Informação de Agravos de Notificação"

    @property
    def description(self) -> str:
        return "O SINAN é alimentado pela notificação de doenças compulsórias."

    def formatter(self, filename: str) -> Dict[str, Any]:
        try:
            name = filename.split(".")[0].upper()
            year_short = name[-2:]

            if name.startswith("SRC"):
                group_code = name[:3]
            else:
                group_code = name[:4]

            return {
                "group": {
                    "name": group_code,
                    "long_name": self.group_definitions.get(group_code),
                },
                "year": int(zfill_year(year_short)),
            }
        except (IndexError, ValueError):
            return {"group": None, "year": None}


AVAILABLE_DATABASES = [
    CIHA,
    CNES,
    IBGEDATASUS,
    PNI,
    SIA,
    SIH,
    SIM,
    SINAN,
    SINASC,
]
