from ftplib import FTP
from typing import Union
from pysus.online_data import FTP_Downloader, FTP_Inspect,CACHEPATH


agravos = {
    "Animais Peçonhentos": "ANIM",
    "Botulismo": "BOTU",
    "Cancer": "CANC",
    "Chagas": "CHAG",
    "Chikungunya": "CHIK",
    "Colera": "COLE",
    "Coqueluche": "COQU",
    "Contact Communicable Disease": "ACBI",
    "Acidentes de Trabalho": "ACGR",
    "Dengue": "DENG",
    "Difteria": "DIFT",
    "Esquistossomose": "ESQU",
    "Febre Amarela": "FAMA",
    "Febre Maculosa": "FMAC",
    "Febre Tifoide": "FTIF",
    "Hanseniase": "HANS",
    "Hantavirose": "HANT",
    "Hepatites Virais": "HEPA",
    "Intoxicação Exógena": "IEXO",
    "Leishmaniose Visceral": "LEIV",
    "Leptospirose": "LEPT",
    "Leishmaniose Tegumentar": "LTAN",
    "Malaria": "MALA",
    "Meningite": "MENI",
    "Peste": "PEST",
    "Poliomielite": "PFAN",
    "Raiva Humana": "RAIV",
    "Sífilis Adquirida": "SIFA",
    "Sífilis Congênita": "SIFC",
    "Sífilis em Gestante": "SIFG",
    "Tétano Acidental": "TETA",
    "Tétano Neonatal": "TETN",
    "Tuberculose": "TUBE",
    "Violência Domestica": "VIOL",
    "Zika": "ZIKA",
}


class Disease:
    name: str
    diseases: dict = agravos

    def __init__(self, name: str) -> None:
        self.name = self.__diseasecheck__(name)

    def __diseasecheck__(self, name: str) -> str:
        return (
            name
            if name in self.diseases.keys()
            else ValueError(f"{name} not found.")
        )

    def __repr__(self) -> str:
        return f"SINAN Disease ({self.name})"

    def __str__(self) -> str:
        return self.name

    @property
    def code(self) -> str:
        return self.diseases[self.name]

    def get_years(self, stage: str = "all") -> list:
        """
        Returns the available years to download, if no stage
        is assigned, it will return years from both finals and
        preliminaries datasets.
        stage (str): 'finais' | 'prelim' | 'all'
        """

        def extract_years(paths):
            return [
                str(path).split("/")[-1].split(".dbc")[0][-2:]
                for path in paths
            ]

        p = self._ftp_list_datasets_paths
        prelim_years = extract_years(p(self.name, "prelim"))
        finais_years = extract_years(p(self.name, "finais"))

        if stage == "prelim":
            return sorted(prelim_years)
        elif stage == "finais":
            return sorted(finais_years)
        return sorted(prelim_years + finais_years)

    def get_ftp_paths(self, years: list) -> list:
        """
        Returns the FTP path available for years to download.
        years (list): a list with years to download, if year
                      is not available, it won't be included
                      in the result
        """
        p = self._ftp_list_datasets_paths
        prelim_paths = p(self.name, "prelim")
        finais_paths = p(self.name, "finais")
        all_paths = prelim_paths + finais_paths
        ds_paths = list()

        def mask(_year):
            return str(_year)[-2:].zfill(2)

        for year in years:
            [ds_paths.append(path) for path in all_paths if mask(year) in path]

        return ds_paths

    def _ftp_list_datasets_paths(self, disease: str, stage: str) -> list:
        """
        stage: 'f'|'finais' or 'p'|'prelim'
        """
        datasets_path = "/dissemin/publicos/SINAN/DADOS/"

        if stage.startswith("f"):
            datasets_path += "FINAIS"
        elif stage.startswith("p"):
            datasets_path += "PRELIM"
        else:
            raise ValueError(f"{stage}")

        code = self.diseases[disease]

        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.cwd(datasets_path)
        available_dbcs = ftp.nlst(f"{code}BR*.dbc")

        return [f"{ftp.pwd()}/{dbc}" for dbc in available_dbcs]


def list_diseases() -> list:
    """List available diseases on SINAN"""
    return list(agravos.keys())


def get_available_years(disease: str) -> list:
    """
    Fetch available years for data related to specific disease
    :param disease: Disease name. See `SINAN.list_diseases` for valid names
    :return: A list of DBC files from a specific disease found in the FTP Server.
    """
    return FTP_Inspect('SINAN').list_available_years(SINAN_disease=disease)


def download(disease, years: Union[str, list, int], data_path: str=CACHEPATH) -> list:
    """
    Downloads SINAN data directly from Datasus ftp server.
    :param disease: Disease according to `agravos`.
    :param years: 4 digit integer, can be a list of years.
    :param data_path: The directory where the chunks will be downloaded to.
    :return: list of downloaded parquet directories.
    """
    return FTP_Downloader('SINAN').download(
        SINAN_disease=disease,
        years=years,
        local_dir=data_path
    )
