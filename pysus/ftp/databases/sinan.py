from typing import Optional, List, Union

from pysus.ftp import Database, Directory, File
from pysus.ftp.utils import zfill_year, to_list


class SINAN(Database):
    name = "SINAN"
    paths = (
        Directory("/dissemin/publicos/SINAN/DADOS/FINAIS"),
        Directory("/dissemin/publicos/SINAN/DADOS/PRELIM"),
    )
    metadata = {
        "long_name": "Doenças e Agravos de Notificação",
        "source": "https://portalsinan.saude.gov.br/",
        "description": (
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
    }

    diseases = {
        "ACBI": "Acidente de trabalho com material biológico",
        "ACGR": "Acidente de trabalho",
        "ANIM": "Acidente por Animais Peçonhentos",
        "ANTR": "Atendimento Antirrabico",
        "BOTU": "Botulismo",
        "CANC": "Cancêr relacionado ao trabalho",
        "CHAG": "Doença de Chagas Aguda",
        "CHIK": "Febre de Chikungunya",
        "COLE": "Cólera",
        "COQU": "Coqueluche",
        "DENG": "Dengue",
        "DERM": "Dermatoses ocupacionais",
        "DIFT": "Difteria",
        "ESQU": "Esquistossomose",
        "EXAN": "Doença exantemáticas",
        "FMAC": "Febre Maculosa",
        "FTIF": "Febre Tifóide",
        "HANS": "Hanseníase",
        "HANT": "Hantavirose",
        "HEPA": "Hepatites Virais",
        "IEXO": "Intoxicação Exógena",
        "INFL": "Influenza Pandêmica",
        "LEIV": "Leishmaniose Visceral",
        "LEPT": "Leptospirose",
        "LERD": "LER/Dort",
        "LTAN": "Leishmaniose Tegumentar Americana",
        "MALA": "Malária",
        "MENI": "Meningite",
        "MENT": "Transtornos mentais relacionados ao trabalho",
        "NTRA": "Notificação de Tracoma",
        "PAIR": "Perda auditiva por ruído relacionado ao trabalho",
        "PEST": "Peste",
        "PFAN": "Paralisia Flácida Aguda",
        "PNEU": "Pneumoconioses realacionadas ao trabalho",
        "RAIV": "Raiva",
        "SDTA": "Surto Doenças Transmitidas por Alimentos",
        "SIFA": "Sífilis Adquirida",
        "SIFC": "Sífilis Congênita",
        "SIFG": "Sífilis em Gestante",
        "SRC": "Síndrome da Rubéola Congênia",
        "TETA": "Tétano Acidental",
        "TETN": "Tétano Neonatal",
        "TOXC": "Toxoplasmose Congênita",
        "TOXG": "Toxoplasmose Gestacional",
        "TRAC": "Inquérito de Tracoma",
        "TUBE": "Tuberculose",
        "VARC": "Varicela",
        "VIOL": "Violência doméstica, sexual e/ou outras violências",
        "ZIKA": "Zika Vírus",
    }

    def describe(self, file: File) -> dict:
        if file.extension.upper() == ".DBC":
            dis_code, year = self.format(file)

            description = {
                "name": str(file.basename),
                "disease": self.diseases[dis_code],
                "year": zfill_year(year),
                "size": file.info["size"],
                "last_update": file.info["modify"]
            }
            return description
        return {}

    def format(self, file: File) -> tuple:
        year = file.name[-2:]

        if file.name.startswith("SRC"):
            dis_code = file.name[:3]
        elif file.name == "LEIBR22":
            dis_code = "LEIV"  # MISPELLED FILE NAME
        elif file.name == "LERBR19":
            dis_code = "LERD"  # ANOTHER ONE
        else:
            dis_code = file.name[:4]

        return dis_code, zfill_year(year)

    def get_files(
        self,
        dis_code: Optional[Union[str, list]] = None,
        year: Optional[Union[str, int, list]] = None,
    ) -> List[File]:
        files = list(filter(
            lambda f: f.extension.upper() in [".DBC", ".DBF"], self.files
        ))

        if dis_code:
            codes = [c.upper() for c in to_list(dis_code)]

            if codes and not all(code in self.diseases for code in codes):
                raise ValueError(
                    "Unknown disease(s): "
                    f"{set(codes).difference(set(self.diseases))}"
                )

            files = list(filter(lambda f: self.format(f)[0] in codes, files))

        if year or str(year) in ["0", "00"]:
            years = (
                [zfill_year(str(y)[-2:]) for y in to_list(year)]
            )
            files = list(filter(lambda f: self.format(f)[1] in years, files))

        return files
