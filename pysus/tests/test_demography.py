import unittest
from pysus.demography import geobase
import geopandas as gpd

class Geobase(unittest.TestCase):
    def test_levels(self):
        self.assertIn('Municipality', geobase.LEVELS)

    def test_get_map(self):
        GB = geobase.GeoBase(level='Municipality')
        gdf = GB.get_map(4108304)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)


if __name__ == '__main__':
    unittest.main()
