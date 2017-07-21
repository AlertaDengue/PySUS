from setuptools import setup, find_packages

ld="""
PySUS
=====

This package collects a set of utilities for handling with public databases published by Brazil's DATASUS
The documentation of how to use PySUS can be found [here](http://pysus.readthedocs.io/en/latest/)

Features
--------

- Decode encoded patient age to any time unit (years, months, etc)
- Convert `.dbc` files to DBF databases or read them into pandas dataframes. DBC files are basically DBFs compressed by a proprietary algorithm.
- Loads SINAN files into Pandas Dataframes
- Geocodes SINAN notified cases in batch. You can use your Google API KEY to avoid Google's free limits.

Instalation
-----------

`$ sudo pip install PySUS`

"""

setup(
    name='PySUS',
    version='0.1.10',
    packages=find_packages(),
    package_data={
        '': ['*.c', '*.h', '*.o', '*.so', '*.md', '*.txt']
    },
    zip_safe=False,
    url='https://github.com/fccoelho/PySUS',
    license='gpl-v3',
    author='Flavio Codeco Coelho',
    author_email='fccoelho@gmail.com',
    description="Tools for dealing with Brazil's Public health data",
    long_description=ld,
    setup_requires=['cffi>=1.0.0'],
    cffi_modules=["pysus/utilities/_build_readdbc.py:ffibuilder"],
    install_requires=['pandas', 'dbfread', 'cffi>=1.0.0', 'geocoder', 'requests']
)
