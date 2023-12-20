import unittest
import pytest

from pysus.utilities.brasil import get_city_name_by_geocode


class TestGetMunNameByGeocode(unittest.TestCase):

    @pytest.mark.timeout(5)
    def test_get_mun_name_by_geocode(self):
        rio = get_city_name_by_geocode(3304557)
        self.assertEqual(rio, "Rio de Janeiro")

        vale = get_city_name_by_geocode(1101757)
        self.assertEqual(vale, "Vale do Anari")

        santa_helena = get_city_name_by_geocode(5219308)
        self.assertEqual(santa_helena, "Santa Helena de Goiás")


if __name__ == "__main__":
    unittest.main()
