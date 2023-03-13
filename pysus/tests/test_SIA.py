import unittest
from ftplib import FTP
import pandas as pd
from pysus.online_data.SIA import download
from pysus.online_data import parquets_to_dataframe as to_df

class SIATestCase(unittest.TestCase):
    @unittest.skip  # Takes a long time to complete
    def test_download_large_PA(self):
        res = to_df(download('SP', 2020, 12, group='PA')[0])
        if isinstance(res, pd.DataFrame):
            assert not res.empty
        else:
            pass


if __name__ == '__main__':
    unittest.main()
