from pysus.ftp import Database, File
import humanize
import datetime
from typing import Union
from pysus.utilities.brasil import STATES


def zfill_year(year: Union[str, int]) -> int:
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
        if file.name == "LEIBR22.dbc":
            file.name = "LEIVBR22.dbc"

        description = dict(
            name=file.name,
            disease=self.diseases[
                file.name[:4]
                if not file.name.startswith("SRC")
                else file.name[:3]
            ],
            year=self._get_year(file),
            size=humanize.naturalsize(file.size),
            last_update=file.date.strftime("%m/%d/%Y %H:%M"),
        )
        return description

    def _get_year(self, file: File) -> int:
        year = file.name.lower().split(".dbc")[0][-2:]
        return zfill_year(year)


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
            name=file.name, state=state, year=year, group=groups[group]
        )

        return description

    def format(self, file: File) -> tuple:
        fname = file.name.upper().split(".DBC")[0]

        if "CID9" in str(file.path):
            group = fname[:-4]
            uf = fname[-4:-2]
            year = zfill_year(fname[-2:])
        else:
            group = fname[:-6]
            uf = fname[-6:-4]
            year = fname[-4:]

        return group, uf, year
