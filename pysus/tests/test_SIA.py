import unittest
from ftplib import FTP
import pandas as pd
from pysus.online_data.SIA import download, check_file_split

class SIATestCase(unittest.TestCase):
    def test_check_split_filenames(self):
        ftp = FTP("ftp.datasus.gov.br")
        ftp.login()
        ftp.cwd("/dissemin/publicos/SIASUS/200801_/Dados")
        names = check_file_split('PASP2012.dbc', ftp)
        assert len(names) == 3
        assert 'PASP2012b.dbc' in names

    @unittest.skip  # Takes a long time to complete
    def test_download_large_PA(self):
        res = download('SP', 2020, 12, group=['PA'])
        if isinstance(res, pd.DataFrame):
            assert not res.empty
        else:
            pass


if __name__ == '__main__':
    unittest.main()
