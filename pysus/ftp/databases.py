from pysus.ftp import Database, File
import humanize
import datetime
from typing import Union, Any, List
from itertools import product
from pysus.utilities.brasil import STATES


def to_list(ite: Any) -> list:
    """Parse any builtin data type into a list"""
    return [ite] if type(ite) in [str, float, int] else list(ite)


def zfill_year(year: Union[str, int]) -> int:
    """
    Formats a len(2) year into len(4) with the correct year preffix
    E.g: 20 -> 2020; 99 -> 1999
    """
    current_year = str(datetime.datetime.now().year)[-2:]
    suffix = "19" if str(year) > current_year else "20"
    return int(suffix + str(year))


class SINAN(Database):
    name = "SINAN"
    paths = [
        "/dissemin/publicos/SINAN/DADOS/FINAIS",
        "/dissemin/publicos/SINAN/DADOS/PRELIM",
    ]
    metadata = dict(
        long_name="Doenças e Agravos de Notificação",
        source="https://portalsinan.saude.gov.br/",
        description=(
            "The Notifiable Diseases Information System - Sinan is primarily"
            "fed by the notification and investigation of cases of diseases "
            "and conditions listed in the national list of compulsorily notifiable "
            "diseases (Consolidation Ordinance No. 4, September 28, 2017, Annex)."
            "However, states and municipalities are allowed to include other "
            "important health problems in their region, such as difilobotriasis "
            "in the municipality of São Paulo. Its effective use enables the dynamic "
            "diagnosis of the occurrence of an event in the population, providing "
            "evidence for causal explanations of compulsorily notifiable diseases "
            "and indicating risks to which people are exposed. This contributes to "
            "identifying the epidemiological reality of a specific geographical area. "
            "Its systematic, decentralized use contributes to the democratization of "
            "information, allowing all healthcare professionals to access and make "
            "it available to the community. Therefore, it is a relevant tool to assist"
            " in health planning, define intervention priorities, and evaluate the "
            "impact of interventions."
        ),
    )

    diseases = dict(
        ACBI="Acidente de trabalho com material biológico",
        ACGR="Acidente de trabalho",
        ANIM="Acidente por Animais Peçonhentos",
        ANTR="Atendimento Antirrabico",
        BOTU="Botulismo",
        CANC="Cancêr relacionado ao trabalho",
        CHAG="Doença de Chagas Aguda",
        CHIK="Febre de Chikungunya",
        COLE="Cólera",
        COQU="Coqueluche",
        DENG="Dengue",
        DERM="Dermatoses ocupacionais",
        DIFT="Difteria",
        ESQU="Esquistossomose",
        EXAN="Doença exantemáticas",
        FMAC="Febre Maculosa",
        FTIF="Febre Tifóide",
        HANS="Hanseníase",
        HANT="Hantavirose",
        HEPA="Hepatites Virais",
        IEXO="Intoxicação Exógena",
        INFL="Influenza Pandêmica",
        LEIV="Leishmaniose Visceral",
        LEPT="Leptospirose",
        LERD="LER/Dort",
        LTAN="Leishmaniose Tegumentar Americana",
        MALA="Malária",
        MENI="Meningite",
        MENT="Transtornos mentais relacionados ao trabalho",
        NTRA="Notificação de Tracoma",
        PAIR="Perda auditiva por ruído relacionado ao trabalho",
        PEST="Peste",
        PFAN="Paralisia Flácida Aguda",
        PNEU="Pneumoconioses realacionadas ao trabalho",
        RAIV="Raiva",
        SDTA="Surto Doenças Transmitidas por Alimentos",
        SIFA="Sífilis Adquirida",
        SIFC="Sífilis Congênita",
        SIFG="Sífilis em Gestante",
        SRC="Síndrome da Rubéola Congênia",
        TETA="Tétano Acidental",
        TETN="Tétano Neonatal",
        TOXC="Toxoplasmose Congênita",
        TOXG="Toxoplasmose Gestacional",
        TRAC="Inquérito de Tracoma",
        TUBE="Tuberculose",
        VARC="Varicela",
        VIOL="Violência doméstica, sexual e/ou outras violências",
        ZIKA="Zika Vírus",
    )

    def describe(self, file: File) -> dict:
        dis_code, year = self.format(file)

        description = dict(
            name=file.name,
            disease=self.diseases[dis_code],
            year=year,
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )
        return description

    def get_files(
        self,
        dis_codes: Union[str, list],
        years: Union[str, int, list],
    ) -> List:

        codes = [c.upper() for c in to_list(dis_codes)]
        fyears = [str(y)[-2:].zfill(2) for y in to_list(years)]

        if not all(code in self.diseases for code in codes):
            raise ValueError(
                f"Unknown disease(s): {set(codes).difference(set(self.diseases))}"
            )

        targets = [f"{c}BR{y}.dbc" for c, y in list(product(codes, fyears))]

        files = list()
        for file in self.files:
            if file.name in targets:
                files.append(file)

        return files

    def format(self, file: File) -> tuple:
        fname = file.name.split(".dbc")[0]

        if fname.startswith("SRC"):
            dis_code = fname[:3]
        elif fname == "LEIBR22":
            dis_code = "LEIV"  # MISPELLED FILE NAME
        else:
            dis_code = fname[:4]

        year = zfill_year(fname[-2:])

        return dis_code, year


class SIM(Database):
    name = "SIM"
    paths = [
        "/dissemin/publicos/SIM/CID10/DORES",
        "/dissemin/publicos/SIM/CID9/DORES",
    ]
    metadata = dict(
        long_name="Sistema de Informação sobre Mortalidade",
        source="http://sim.saude.gov.br",
        description="",
    )

    def describe(self, file: File) -> dict:
        groups = dict(DO="CID10", DOR="CID9")

        group, uf, year = self.format(file)
        state = "Brasil" if uf == "BR" else STATES[uf]

        description = dict(
            name=file.name,
            state=state,
            year=year,
            group=groups[group],
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )

        return description

    def format(self, file: File) -> tuple:
        fname = file.name.upper().split(".DBC")[0]

        if "CID9" in str(file.path):
            group, uf, year = fname[:-4], fname[-4:-2], zfill_year(fname[-2:])
        else:
            group, uf, year = fname[:-6], fname[-6:-4], int(fname[-4:])

        return group, uf, year


class SINASC(Database):
    name = "SINASC"
    paths = [
        "/dissemin/publicos/SINASC/NOV/DNRES",
        "/dissemin/publicos/SINASC/ANT/DNRES",
    ]
    metadata = dict(
        long_name="Sistema de Informações sobre Nascidos Vivos",
        source="http://sinasc.saude.gov.br/",
        description="",
    )

    def describe(self, file: File) -> dict:
        uf, year = self.format(file)

        if uf == "BR":
            state = "Brasil"
        elif uf == "EX":  # DNEX2021.dbc
            state = None
        else:
            state = STATES[uf]

        description = dict(
            name=file.name,
            state=state,
            year=year,
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )

        return description

    def format(self, file: File) -> tuple:
        fname = file.name.upper().split(".DBC")[0]
        year = zfill_year(fname[-2:])
        charname = "".join([c for c in fname if not c.isnumeric()])
        uf = charname[-2:]
        return uf, year
