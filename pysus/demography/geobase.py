"""
Fetches the geographical base for demographical analyses.
All data comes from IBGE
"""

import geobr
import dask
from multiprocessing import Pool
import wget
import geopandas as gpd
import numpy as np
import georasters as gr
import datashader as ds
import colorcet
import tempfile
import lzma, tarfile
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

    def __init__(self, level):
        try:
            assert level in LEVELS
        except AssertionError:
            print(f"Please select one of these levels: {', '.join(LEVELS.keys())}")
        self.level = level
        self.mapdf = None
        self.pop = None
        self.pop_raster = None

    def __str__(self):
        return f"{self.level} level Geobase"

    def _persist(self, what):
        if what == 'map':
            self.mapdf.to_parquet(f"{self.level}_map.parquet")
        elif what == 'pop':
            self.pop.to_parquet(f"{self.level}_pop.parquet")
        elif what == 'raster':
            self.raster.to_parquet(f"{self.level}_raster.parquet")




    def help_fetch_map(self):
        return help(LEVELS[self.level])

    def map(self, *args, **kwargs):
        """
        Fetches map of `self.level` given parameters
        :param args: positional parameters for geobr map reading function
        :param kwargs: keyword parameters for geobr map reading function
        :return: GeoDataFrame
        """
        if os.path.exists(f'{self.level}_map.parquet'):
            self.mapdf = gpd.read_parquet(f'{self.level}_map.parquet')
            return self.mapdf
        print("Dowloading the Map...")
        if self.mapdf is None:
            self.mapdf = LEVELS[self.level](*args, **kwargs)
            self._persist('map')
        return self.mapdf

    def plot_pop(self, **kwargs):
        """
        Plots a chropletic representation of the population
        :param kwargs: Additional parameters passed to geopandas plot command
        """
        self.mapdf.plot(column='population', **kwargs)

    def demographics(self):
        """
        Adds population data to geoographical base
        :return:
        """
        if 'population' in self.mapdf.columns:
            return
        print("Fetching population data...")
        bbox = self.mapdf.to_crs('EPSG:4326').total_bounds
        # raster = fetch_gpw4_raster(bbox)
        self.pop_raster = raster = get_full_pop_raster()
        self.mapdf['population'] = [raster.clip(geom)[0].sum() for geom in self.mapdf.geometry]
        self._persist('map')


    def generate_populations(self, scale=0.05):
        """
        Generate a synthetic population of size scale*population size for each polygon in self.mapdf
        :param scale:
        """
        if os.path.exists(f'{self.level}_pop.parquet'):
            self.pop = gpd.read_parquet(f'{self.level}_pop.parquet')
            return
        if 'population' not in self.mapdf.columns:
            self.demographics()
        for row in self.mapdf.itertuples():
            people = sample_random_people(int(row.population * scale), 
row.geometry)
            sex = np.random.randint(0, 2, size=len(people))
            age = np.random.randint(0, 100, size=len(people))
        print(len(people), people[0])
        self.pop = gpd.GeoDataFrame({'sex': sex, 'age': age, 'geometry': 
people})
        self.pop['longitude'] = [pt.x for pt in self.pop.geometry]
        self.pop['latitude'] = [pt.y for pt in self.pop.geometry]
        self._persist('pop')
        

    def plot_synthetic_pop(self):
        canvas = ds.Canvas(plot_width=800, plot_height=600)
        agg = canvas.points(self.pop, x='longitude', y='latitude')
        return ds.tf.shade(agg, cmap=colorcet.fire, how='log')


def contains(args):
    polygon, point = args
    pt = geometry.Point(point)
    return pt, polygon.contains(pt)

def sample_random_people(n, polygon, overestimate=1.5):
    print(f"Synthetizing {n} individuals")
    min_x, min_y, max_x, max_y = polygon.bounds
    ratio = polygon.area / polygon.envelope.area
    samples = np.random.uniform((min_x, min_y), (max_x, max_y), (int((n / 
ratio) * overestimate), 2))[:n,:]
    #multipoint = geometry.MultiPoint(samples)
    #multipoint = multipoint.intersection(polygon)
    po = Pool()
    res = po.map(contains, ((polygon, p) for p in samples))
    pts = [p for p, c in res if c]  # List of inscribed points
    po.terminate()
    po.join()
   
    return pts


def get_population(geometry, raster):
    return raster.clip(geometry)[0].sum()


def get_full_pop_raster(path='.'):
    url = "https://www.dropbox.com/s/l9iphmawfjzt4lf/brazil_pop.tif.tar.xz?dl=1"
    fn = os.path.join(path, 'brazil_pop.tif.tar.xz')
    wget.download(url=url, out=path)
    fn = os.path.join(path, 'brazil_pop.tif.tar.xz')
    with lzma.open('brazil_pop.tif.tar.xz') as f:
        with tarfile.open(fileobj=f) as tar:
            tar.extractall()
            # with open('brazil_pop.tif', 'wb') as brr:
            #     brr.write(tar.extractall(path=path))
    os.unlink('brazil_pop.tif.tar.xz')
    raster = gr.from_file('brazil_pop.tif.tif')
    os.unlink('brazil_pop.tif.tif')

    return raster



