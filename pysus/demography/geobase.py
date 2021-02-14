"""
Fetches the geographical base for demographical analyses.
All data comes from IBGE
"""

import geobr
import dask
import geopandas as gpd


LEVELS = {"Country": geobr.read_country,
          "Region": geobr.read_region,
          "State": geobr.read_state,
          "Meso region": geobr.read_meso_region,
          "Micro region": geobr.read_micro_region,
          "Immediate region": geobr.read_immediate_region,
          "Census weighting area": geobr.read_weighting_area,
          "Census tract": geobr.read_census_tract,
          "Municipality": geobr.read_municipality,
          "Municipality seats": geobr.read_municipal_seat,
          "Metropolitan areas": geobr.read_metro_area,
          "Urban footprints": geobr.read_urban_area,
          "Brazil's Legal Amazon": geobr.read_amazon,
          "Biomes": geobr.read_biomes,
          "Environmental Conservation Units": geobr.read_conservation_units,
          "Disaster risk areas": geobr.read_disaster_risk_area,
          "Indigenous lands": geobr.read_indigenous_land,
          "Semi Arid region": geobr.read_semiarid,
          "Health facilities": geobr.read_health_facilities,
          "Health regions": geobr.read_health_region,
          "Neighborhood limits": geobr.read_neighborhood,
          }


class GeoBase:
    """
    Parameterized geographical base
    """
    def __init__(self, level='Country'):
        try:
            assert level in LEVELS
        except AssertionError:
            print(f"Please select one of these levels: {', '.join(LEVELS.keys())}")
        self.level = level
        self.mapdf = None

    def map(self, *args, **kwargs):
        """
        Fetches map of `self.level` given parameters
        :param args: positional parameters for geobr map reading function
        :param kwargs: keyword parameters for geobr map reading function
        :return:
        """
        if self.mapdf is None:
            self.mapdf = LEVELS[self.level](*args, **kwargs)
        return self.mapdf

    def _demographics(self):
        pass


