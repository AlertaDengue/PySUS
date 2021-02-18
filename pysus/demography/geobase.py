"""
Fetches the geographical base for demographical analyses.
All data comes from IBGE
"""

import geobr
import dask
import geopandas as gpd
import numpy as np
from owslib.wms import WebMapService
import georasters as gr
import tempfile
import lzma
import os
from shapely import geometry


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
        self.map()

    def help_fetch_map(self):
        return help(LEVELS[self.level])

    def map(self, *args, **kwargs):
        """
        Fetches map of `self.level` given parameters
        :param args: positional parameters for geobr map reading function
        :param kwargs: keyword parameters for geobr map reading function
        :return: GeoDataFrame
        """
        if self.mapdf is None:
            self.mapdf = LEVELS[self.level](*args, **kwargs)
        return self.mapdf

    def demographics(self):
        """
        Adds population data to geoographical base
        :return:
        """
        if 'population' in self.mapdf.columns:
            return
        bbox = self.mapdf.to_crs('EPSG:4326').total_bounds
        raster = fetch_gpw4_raster(bbox)
        self.mapdf['population'] = [raster.clip(geom)[0].sum() for geom in self.mapdf.geometry]

    def generate_populations(self, scale=1):
        popdf = gpd.GeoDataFrame(columns=['sex','age','geometry'])
        for row in self.mapdf.itertuples():
            people = sample_random_people(row.population*scale,row.geometry)
            sex = np.random.randint(0,2,size=len(people))
            age = np.random.randint(0,100,size=len(people))



def sample_random_people(n, polygon, overestimate=2):
    min_x, min_y, max_x, max_y = polygon.bounds
    ratio = polygon.area / polygon.envelope.area
    samples = np.random.uniform((min_x, min_y), (max_x, max_y), (int(size / ratio * overestimate), 2))
    multipoint = geometry.MultiPoint(samples)
    multipoint = multipoint.intersection(polygon)
    samples = np.array(multipoint)
    return samples[:n]


def get_population(geometry, raster):
    return raster.clip(geometry)[0].sum()

def get_full_pop_raster(path='.'):
    fn = os.path.join(path, 'brazil_pop.tif.tar.xz')
    f = lzma.open('brazil_pop.tif.tar.xz')
    with tempfile.NamedTemporaryFile('w+b') as brr:
        brr.write(f.read())
        raster = gr.from_file(brr.name)
    return raster


def fetch_gpw4_raster(bbox):
    """
    Fetches raster population data from GPW4
    :param bbox: bounds of the geometry
    :return:
    """
    url = "https://sedac.ciesin.columbia.edu/geoserver/wms"
    wms = WebMapService(url)
    img = wms.getmap(
    layers=['gpw-v4:gpw-v4-population-count_2020'],
    srs='EPSG:4326',
    bbox=bbox,
    size=(1440, 720),
    format='image/geotiff',
    transparent=True
    )
    with tempfile.NamedTemporaryFile('w+b') as out:
        out.write(img.read())
        raster = gr.from_file(out.name)
    return raster