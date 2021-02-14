import unittest
from pysus.demography import geobase
import geopandas as gpd
from time import time

class Geobase(unittest.TestCase):
    def test_levels(self):
        self.assertIn('Municipality', geobase.LEVELS)

    def test_get_map(self):
        GB = geobase.GeoBase(level='Municipality')
        gdf = GB.map(4108304)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        t0 = time()
        gdf = GB.map() # from second time it should skip download
        tf = time()
        self.assertLess(t0-tf, 1)
        self.assertEqual(len(gdf), 1)



if __name__ == '__main__':
    unittest.main()
