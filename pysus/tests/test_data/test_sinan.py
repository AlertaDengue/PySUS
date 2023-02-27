import datetime
import os
from pathlib import Path
import shutil
import unittest
from glob import glob
from pathlib import Path

import numpy as np
import pandas as pd

from pysus.online_data.SINAN import (
    download,
    list_diseases,
    download_all_years_in_chunks,
)
from pysus.preprocessing.sinan import read_sinan_dbf

from pysus import SINAN

PATH_ROOT = Path(__file__).resolve().parent


class TestSINANClass(unittest.TestCase):
    data_path = '/tmp/pysus'
    d1 = 'Raiva Humana'
    r1 = [
        'RAIVBR07.parquet',
        'RAIVBR08.parquet',
        'RAIVBR09.parquet',
        'RAIVBR10.parquet',
        'RAIVBR11.parquet',
        'RAIVBR12.parquet',
        'RAIVBR13.parquet',
        'RAIVBR14.parquet',
        'RAIVBR15.parquet',
        'RAIVBR16.parquet',
        'RAIVBR17.parquet',
        'RAIVBR18.parquet',
        'RAIVBR19.parquet',
    ]

    def test_list_all_diseases(self):
        all_diseases = SINAN.diseases
        self.assertIn('Dengue', all_diseases)
        self.assertIn('Zika', all_diseases)
        self.assertIn('Chikungunya', all_diseases)

    def test_download(self):
        SINAN.download_parquets(self.d1)
        self.assertTrue(
            all([(path in os.listdir(self.data_path)) for path in self.r1])
        )

    def test_read_dataframe(self):
        df = SINAN.parquets_to_df(self.d1, '2007')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (1, 89))
    
    def test_metadata_dataframe(self):
        df = SINAN.metadata_df('Raiva Humana')
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape, (68, 7))


class TestSINANDownload(unittest.TestCase):
    def test_download(self):
        df = download(year=2007, disease='Botulismo')
        self.assertIsInstance(df, pd.DataFrame)

    def test_filename_only(self):
        fname = download(year=2015, disease='Botulismo', return_chunks=True)
        self.assertIsInstance(fname, str)
        self.assertTrue(os.path.exists(fname))
        shutil.rmtree(fname, ignore_errors=True)

    def test_fetch_viol_dom(self):
        df = download(year=2011, disease='Hantavirose')
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_cancer_prelim(self):
        df = download(year=2022, disease='Cancer')
        self.assertIsInstance(df, pd.DataFrame)

    def test_fetch_sifilis(self):
        self.assertRaises(
            Exception, download(year=2021, disease='Sífilis Adquirida')
        )

    def test_fetch_sifilis_gestante(self):
        df = download(year=2021, disease='Sífilis em Gestante')
        self.assertIsInstance(df, pd.DataFrame)

    def test_lista_agravos(self):
        lista = list_diseases()
        self.assertIsInstance(lista, list)
        self.assertGreater(len(lista), 0)

    def test_chunked_df_size(self):
        df1 = download(year=2018, disease='Chikungunya')
        s1 = len(df1)
        del df1
        fn = download(year=2018, disease='Chikungunya', return_chunks=True)
        for i, f in enumerate(glob(f'{fn}/*.parquet')):
            if i == 0:
                df2 = pd.read_parquet(f)
            else:
                df2 = pd.concat([df2, pd.read_parquet(f)], ignore_index=True)
        self.assertEqual(s1, df2.shape[0])
        shutil.rmtree(fn, ignore_errors=True)

    def test_download_all_dbfs_for_zika(self):
        download_all_years_in_chunks('zika')
        self.assertTrue(Path('/tmp/pysus/ZIKABR16.parquet').exists())
        self.assertTrue(Path('/tmp/pysus/ZIKABR17.parquet').exists())
        self.assertTrue(Path('/tmp/pysus/ZIKABR18.parquet').exists())
        self.assertTrue(Path('/tmp/pysus/ZIKABR19.parquet').exists())
        self.assertTrue(Path('/tmp/pysus/ZIKABR20.parquet').exists())


class TestSinanDBF(unittest.TestCase):
    dbf_name = PATH_ROOT / 'EPR-2016-06-01-2016.dbf'

    def test_read_dbf(self):
        df = read_sinan_dbf(self.dbf_name, encoding='latin-1')
        self.assertTrue(self.dbf_name.exists())
        self.assertIsInstance(df, pd.DataFrame)
        for cname in df.columns:
            if cname.startswith('DT_'):
                self.assertIsInstance(df[cname][0], datetime.date)
            elif cname.startswith('SEM'):
                self.assertLessEqual(df[cname][0], 52)
                self.assertIsInstance(df[cname][0], (int, np.int64))
            elif cname.startswith(('NU', 'ID')):
                if cname == 'ID_AGRAVO':
                    continue
                self.assertIsInstance(
                    df[cname][0],
                    (int, float, np.int64),
                    msg='Failed on column {}, type:{}'.format(
                        cname, type(df[cname][0])
                    ),
                )

    def test_type_convertion(self):
        df = read_sinan_dbf(self.dbf_name, encoding='latin-1')
        self.assertTrue(self.dbf_name.exists())
        assert not all(df.dtypes == 'object')


if __name__ == '__main__':
    unittest.main()
