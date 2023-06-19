from pysus.ftp import Database, File
import humanize
import datetime
from typing import Union, Any, List
from itertools import product
from pysus.utilities.brasil import UFs


def to_list(ite: Any) -> list:
    """Parse any builtin data type into a list"""
    return [ite] if type(ite) in [str, float, int] else list(ite)


def zfill_year(year: Union[str, int]) -> int:
    """
    Formats a len(2) year into len(4) with the correct year preffix
    E.g: 20 -> 2020; 99 -> 1999
    """
    year = str(year)[-2:]
    current_year = str(datetime.datetime.now().year)[-2:]
    suffix = "19" if str(year) > current_year else "20"
    return int(suffix + str(year))


def parse_UFs(UF: Union[list[str], str]) -> list:
    """
    Formats states abbreviations into correct format and retuns a list.
    Also checks if there is an incorrect UF in the list.
    E.g: ['SC', 'mt', 'ba'] -> ['SC', 'MT', 'BA']
    """
    ufs = [uf.upper() for uf in to_list(UF)]
    if not all([uf in list(UFs) for uf in ufs]):
        raise ValueError(f"Unknown UF(s): {set(ufs).difference(list(UFs))}")
    return ufs


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
            name=file.basename,
            disease=self.diseases[dis_code],
            year=zfill_year(year),
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )
        return description

    def format(self, file: File) -> tuple:
        year = file.name[-2:]

        if file.name.startswith("SRC"):
            dis_code = file.name[:3]
        elif file.name == "LEIBR22":
            dis_code = "LEIV"  # MISPELLED FILE NAME
        else:
            dis_code = file.name[:4]

        return dis_code, year

    def get_files(
        self,
        dis_codes: Union[str, list],
        years: Union[str, int, list],
    ) -> List[File]:
        codes = [c.upper() for c in to_list(dis_codes)]
        fyears = [str(y)[-2:].zfill(2) for y in to_list(years)]

        if not all(code in self.diseases for code in codes):
            raise ValueError(
                f"Unknown disease(s): {set(codes).difference(set(self.diseases))}"
            )

        targets = [f"{c}BR{y}" for c, y in list(product(codes, fyears))]

        files = list()
        for file in self.files:
            if file.name in targets:
                files.append(file)

        return files


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
    groups = dict(DO="CID10", DOR="CID9")

    def describe(self, file: File) -> dict:
        group, uf, year = self.format(file)
        state = UFs[uf]

        description = dict(
            name=file.basename,
            state=state,
            year=year,
            group=self.groups[group],
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )

        return description

    def format(self, file: File) -> tuple:
        if "CID9" in str(file.path):
            group, uf, year = file.name[:-4], file.name[-4:-2], file.name[-2:]
        else:
            group, uf, year = file.name[:-6], file.name[-6:-4], file.name[-4:]

        return group, uf, year

    def get_files(
        self,
        groups: Union[list[str], str],
        ufs: Union[list[str], str],
        years: Union[list, str, int],
    ) -> List[File]:
        groups = [g.upper() for g in to_list(groups)]
        ufs = parse_UFs(ufs)
        years = to_list(years)

        if not all([gr in list(self.groups.values()) for gr in groups]):
            raise ValueError(
                f"Unknown group(s): {set(groups).difference(self.groups.values())}"
            )

        targets = []
        for group in groups:
            if group == "CID9":
                cid9_years = [str(y)[-2:].zfill(2) for y in years]
                targets.extend(
                    [
                        f"DOR{uf}{year}"
                        for uf, year in list(product(ufs, cid9_years))
                    ]
                )
            elif group == "CID10":
                cid10_years = [zfill_year(y) for y in years]
                targets.extend(
                    [
                        f"DO{uf}{year}"
                        for uf, year in list(product(ufs, cid10_years))
                    ]
                )

        files = []
        for file in self.files:
            if file.name in targets:
                files.append(file)

        return files


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

        if uf == "EX":  # DNEX2021.dbc
            state = None
        else:
            state = UFs[uf]

        description = dict(
            name=file.basename,
            state=state,
            year=year,
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%d/%m/%Y %H:%M"),
        )

        return description

    def format(self, file: File) -> tuple:
        year = zfill_year(file.name[-2:])
        charname = "".join([c for c in file.name if not c.isnumeric()])
        uf = charname[-2:]
        return uf, year


# How to test all functionalities in one line:
# a = SINAN()
# [a.describe(y[0]) for y in [a.get_files(*x) for x in [a.format(f) for f in a.files]]]
