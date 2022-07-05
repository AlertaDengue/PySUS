import os
import unittest
from time import time

import geopandas as gpd

from pysus.demography import geobase


class Geobase(unittest.TestCase):
    def tearDown(self) -> None:
        if os.path.exists("Municipality_map.parquet"):
            os.unlink("Municipality_map.parquet")
        if os.path.exists("State_map.parquet"):
            os.unlink("State_map.parquet")

    def test_levels(self):
        self.assertIn("Municipality", geobase.LEVELS)

    def test_get_map(self):
        GB = geobase.GeoBase(level="Municipality")
        gdf = GB.map(4108304)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        t0 = time()
        gdf = GB.map()  # from second time it should skip download
        tf = time()
        self.assertLess(tf - t0, 1)
        self.assertEqual(int(gdf["code_muni"].values[0]), 4108304)
        self.assertEqual(len(gdf), 1)

    def test_get_map_2019(self):
        GB = geobase.GeoBase(level="Municipality", reset=True)
        gdf = GB.map(4108304, year=2019)
        self.assertIsInstance(gdf, gpd.GeoDataFrame)
        t0 = time()
        gdf = GB.map()  # from second time it should skip download
        tf = time()
        self.assertLess(tf - t0, 1)
        self.assertEqual(int(gdf["code_muni"].values[0]), 4108304)
        self.assertEqual(len(gdf), 1)

    def test_demographics(self):
        GB = geobase.GeoBase(level="State")
        gdf = GB.map("all")
        self.assertEqual(len(gdf), 27)
        GB.demographics()
        self.assertIn("population", GB.mapdf)


if __name__ == "__main__":
    unittest.main()
