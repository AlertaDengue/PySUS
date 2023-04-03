from collections import ChainMap
from functools import lru_cache
from multiprocessing import Process
from abc import ABC, abstractmethod
import logging
import ftplib
import time
import re

class ContentNotLoaded(Exception):
    def __init__(self, message):
        self.message = message
        
    def __str__(self):
        return f"ContentNotLoaded: {self.message}"


class FTPDataSUS:
    """Common class to connect to DataSUS FTP server"""

    def __init__(self):
        self.host = 'ftp.datasus.gov.br'
        self.FTP: ftplib.FTP = None

    def __enter__(self):
        self.connect()
        return self.FTP
    
    def __exit__(self, *args):
        self.close()

    def connect(self):
        self.FTP = ftplib.FTP(self.host)
        self.FTP.connect()
        self.FTP.login()

    def reconnect(self):
        """Verifies if the connection is up, reconnects if needed"""
        try:
            self.FTP.nlst()
        except (BrokenPipeError, ConnectionResetError, ftplib.error_reply):
            self.connect()
            logging.debug(f'Reconnecting to {self.host}')

    def close(self):
        if self.FTP is not None:
            self.FTP.quit()
            self.FTP = None


class FTPDataSUSClient:
    """
    The client will keep the connection alive as a Process
    running in the background, according to its timeout. It
    will start the process after connecting to the FTP and
    run a cmd at every 15 seconds, until the timeout is
    reached. Closing the Client will close the process, or
    it will close itself when the timeout time expires.
    """

    def __init__(self, keepalive_timeout=3600):
        self.server = FTPDataSUS()
        self.keepalive_proc: Process = None
        self.timeout = keepalive_timeout
        self.conn_time = None

    def connect(self):
        proc = self.keepalive_proc
        if not proc:
            self._spawn_proc()
        elif not proc.is_alive():
            self._spawn_proc()

    def close(self):
        self.server.close()
        self.keepalive_proc.terminate()
        self.keepalive_proc = None
        self.conn_time = None

    def _spawn_proc(self):
        self.conn_time = time.time()
        self.keepalive_proc = Process(
            name='FTPDataSUS',
            target=self._keep_alive_loop, 
        )
        self.server.connect()
        self.keepalive_proc.start()

    def _keep_alive_loop(self):
        while time.time() - self.conn_time < self.timeout:
            self.server.reconnect()
            time.sleep(15)
        logging.debug(f'Connection timed out')
        self.close()


class FTPDatabaseBase(ABC):
    name: str
    ftp_conn = FTPDataSUS()
    FTP_PATHS = {
    'SINAN': [
        '/dissemin/publicos/SINAN/DADOS/FINAIS',
        '/dissemin/publicos/SINAN/DADOS/PRELIM',
    ],
    'SIM': [
        '/dissemin/publicos/SIM/CID10/DORES',
        '/dissemin/publicos/SIM/CID9/DORES',
    ],
    'SINASC': [
        '/dissemin/publicos/SINASC/NOV/DNRES',
        '/dissemin/publicos/SINASC/ANT/DNRES',
    ],
    'SIH': [
        '/dissemin/publicos/SIHSUS/199201_200712/Dados',
        '/dissemin/publicos/SIHSUS/200801_/Dados',
    ],
    'SIA': [
        '/dissemin/publicos/SIASUS/199407_200712/Dados',
        '/dissemin/publicos/SIASUS/200801_/Dados',
    ],
    'PNI': ['/dissemin/publicos/PNI/DADOS'],
    'CNES': ['dissemin/publicos/CNES/200508_/Dados'],
    'CIHA': ['/dissemin/publicos/CIHA/201101_/Dados'],
    }

    @abstractmethod
    def __init__(self) -> None:
        ...

    @property
    def files_paths(self) -> list[str]:
        ...

    @abstractmethod
    def get_files(self, pattern: re.Pattern) -> list[str]:
        ...

    @abstractmethod
    def files_regex(self, **kwargs) -> re.Pattern:
        ...

    @lru_cache
    def load_ftp_content(self, databases: tuple) -> dict:
        """ databases: tuple(["SINAN", "SIM", "SINASC", "SIH", "SIA", "PNI", "CNES", "CIHA"]) """
        content = dict()
        trim_path = lambda path: str(path).split('/')[-1]
        with self.ftp_conn as ftp:
            for db in databases:
                for path in self.FTP_PATHS[db]:
                    if db == 'CNES':
                        content[path] = dict(ChainMap(*[
                            {
                                trim_path(group):list(map(
                                    trim_path,
                                    ftp.nlst(group)))
                            } 
                            for group in ftp.nlst(path)
                        ]))
                    else:
                        content[path] = list(map(trim_path, ftp.nlst(path)))
        return content


class SINAN(FTPDatabaseBase):
    name: str
    ftp_paths: dict
    ftp_content: dict = None
    diseases: dict = {
        'Animais Peçonhentos': 'ANIM',
        'Botulismo': 'BOTU',
        'Cancer': 'CANC',
        'Chagas': 'CHAG',
        'Chikungunya': 'CHIK',
        'Colera': 'COLE',
        'Coqueluche': 'COQU',
        'Contact Communicable Disease': 'ACBI',
        'Acidentes de Trabalho': 'ACGR',
        'Dengue': 'DENG',
        'Difteria': 'DIFT',
        'Esquistossomose': 'ESQU',
        'Febre Amarela': 'FAMA',
        'Febre Maculosa': 'FMAC',
        'Febre Tifoide': 'FTIF',
        'Hanseniase': 'HANS',
        'Hantavirose': 'HANT',
        'Hepatites Virais': 'HEPA',
        'Intoxicação Exógena': 'IEXO',
        'Leishmaniose Visceral': 'LEIV',
        'Leptospirose': 'LEPT',
        'Leishmaniose Tegumentar': 'LTAN',
        'Malaria': 'MALA',
        'Meningite': 'MENI',
        'Peste': 'PEST',
        'Poliomielite': 'PFAN',
        'Raiva Humana': 'RAIV',
        'Sífilis Adquirida': 'SIFA',
        'Sífilis Congênita': 'SIFC',
        'Sífilis em Gestante': 'SIFG',
        'Tétano Acidental': 'TETA',
        'Tétano Neonatal': 'TETN',
        'Tuberculose': 'TUBE',
        'Violência Domestica': 'VIOL',
        'Zika': 'ZIKA',
    }

    def __init__(self) -> None:
        self.name = 'SINAN'
        paths = super().FTP_PATHS[self.name]
        self.ftp_paths = dict(finais=paths[0], prelim=paths[1])

    def code(self, disease: str) -> str:
        return self.diseases[disease]

    def get_files(self, diseases: list[str], years: list[int]) -> dict:
        if not self.ftp_content:
            raise ContentNotLoaded(
                'Content can be loaded using `load_ftp_content()`'
            )
        pat = self.files_regex(diseases, years)
        matches = lambda files: list(map(pat, files))
        files = dict(
            prelim = self.ftp_content['prelim']
        )


    def get_years(self, disease: str) -> dict:
        if not self.ftp_content:
            raise ContentNotLoaded(
                'Content can be loaded using `load_ftp_content()`'
            )
        extract_years = lambda files: set([
                str(file).lower().split('.dbc')[0][-2:]
                for file in files if str(file).startswith(self.code(disease))
            ])

        return dict(
            prelim=sorted(extract_years(self.ftp_content['prelim'])),
            finais=sorted(extract_years(self.ftp_content['finais']))
        )

    def files_regex(self, diseases: list[str] = None, years: list[int] = None) -> re.Pattern:
        re_diseases = '|'.join([self.diseases[dis] for dis in diseases])
        re_years = '|'.join([str(y)[-2:].zfill(2) for y in years])
        return re.compile(f'{re_diseases}BR{re_years}.dbc', re.I)

    def load_ftp_content(self) -> None:
        finais, prelim = self.ftp_paths.values()
        content = super().load_ftp_content((self.name,))
        self.ftp_content = dict(finais=content[finais], prelim=content[prelim])

class SIM(FTPDatabaseBase):
    ...

class SINASC(FTPDatabaseBase):
    ...

class SIH(FTPDatabaseBase):
    ...

class SIA(FTPDatabaseBase):
    ...

class PNI(FTPDatabaseBase):
    ...
    
class CNES(FTPDatabaseBase):
    ...

class CIHA(FTPDatabaseBase):
    ...

