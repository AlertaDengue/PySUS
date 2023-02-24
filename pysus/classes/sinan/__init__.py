from diseases import DISEASE_CODE
from ftplib import FTP
from typing import List, Union
from pathlib import Path


class SINAN:
    diseases = list(DISEASE_CODE.keys())
    __disease = None
    __disease_years = None
    __disease_paths = None
    __data_path = None

    def __init__(self, disease = None) -> None:
        if disease:
            self.__disease = Disease(disease)
            self.__disease_years = self.__disease.get_years('all')
            self.__disease_paths = self.__disease.get_ftp_paths(self.__disease_years)

    def available_years(self, disease: str = None, state: str = 'all') -> list:
        if not disease:
            return self.__disease.get_years(state)
        return Disease(disease).get_years(state)

    def download(self, data_path: str, disease: str = None, years: List[Union[(int, str)]] = None):
        Path(data_path).mkdir(parents=True, exist_ok=True)
        self.__data_path = data_path
        
        if self.__disease:
            _disease = self.__disease
        else:
            _disease = Disease(disease)

        if not years:
            _years = _disease.get_years()
        else:
            _years = years

        _paths = _disease.get_ftp_paths(_years)
        ...

class Disease:
    name: str
    __prelim_paths: list
    __finais_paths: list

    def __init__(self, name: str) -> None:
        self.name = self.__diseasecheck__(name)
        self.__prelim_paths = _ftp_list_datasets_paths(self.name, 'prelim')
        self.__finais_paths = _ftp_list_datasets_paths(self.name, 'finais')

    def __diseasecheck__(self, name: str) -> str:
        return name if name in DISEASE_CODE.keys() else ValueError(f'{name} not available.')

    def __repr__(self) -> str:
        return f'SINAN Disease ({self.name})'

    def __str__(self) -> str:
        return self.name

    @property
    def code(self) -> str:
        return DISEASE_CODE[self.name]

    def get_years(self, state: str = 'all') -> list:
        """ 
        Returns the available years to download, if no state
        is passed, it will return years from both finals and
        preliminaries datasets.
        state (str): 'finais' | 'prelim' | 'all'
        """

        extract_years = lambda paths: [
            str(path).split('/')[-1].split('.dbc')[0][-2:] for path in paths
        ]

        prelim_years = extract_years(self.__prelim_paths)
        finais_years = extract_years(self.__finais_paths)

        if state == 'prelim':
            return sorted(prelim_years)
        elif state == 'finais':
            return sorted(finais_years)
        return sorted(prelim_years + finais_years)

    def get_ftp_paths(self, years: list) -> list:
        """ 
        Returns the FTP path available for years to download.
        years (list): a list with years to download, if year
                      is not available, it won't be included 
                      in the result
        """
        all_paths = self.__prelim_paths + self.__finais_paths
        ds_paths = list()
        mask = lambda _year: str(_year)[-2:].zfill(2)
        for year in years:
            [ds_paths.append(path) for path in all_paths if mask(year) in path]

        return ds_paths
        

def _ftp_dataset_connect(state: str) -> FTP:
    """ 
    state: 'f'|'finais' or 'p'|'prelim'
    """
    datasets_path = '/dissemin/publicos/SINAN/DADOS/'

    if state.startswith('f'):
        datasets_path += 'FINAIS'
    elif state.startswith('p'):
        datasets_path += 'PRELIM'
    else:
        raise ValueError(f'{state}')

    ftp = FTP("ftp.datasus.gov.br")
    ftp.login()
    ftp.cwd(datasets_path)
    return ftp


def _ftp_list_datasets_paths(disease: str, state: str) -> list:
    conn = _ftp_dataset_connect(state)
    code = DISEASE_CODE[disease]
    available_dbcs = conn.nlst(f'{code}BR*.dbc')
    return [f'{conn.pwd()}/{dbc}' for dbc in available_dbcs]

