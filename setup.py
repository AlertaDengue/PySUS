import setuptools
import glob, os
import atexit
import shutil
from setuptools import setup, find_packages
from setuptools.command.install import install

ld = """
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
Make sure your system has libffi-dev package installed,

`$ sudo pip install PySUS`

"""

# class PostInstall(install):
#     def run(self):
#         def _post_install():
#             setuptools_path = setuptools.__path__[0].replace('/setuptools', '')
#             pysus_path = setuptools.__path__[0].replace('setuptools', 'pysus')
#
#             ## This code is to make sure the compiled extension module inside the
#             # utilities module is named _readdbc.so
#             if os.path.exists(pysus_path):
#                 comp_mod_list = glob.glob(os.path.join(pysus_path, 'utilities', '_readdbc*.so'))
#                 for mod in comp_mod_list:
#                     mod = os.path.split(mod)[-1]
#                     if mod != '_readdbc.so':
#                         print(f'Renaming {mod} to _readdbc.so')
#                         os.chdir(os.path.join(pysus_path, 'utilities'))
#                         os.rename(mod, '_readdbc.so')
#             # Sometimes the compiled module is thrown on the (site|dist)-packages directory
#             try:
#                 shutil.copy(os.path.join(setuptools_path, '_readdbc.abi3.so'),
#                             os.path.join(setuptools_path, 'pysus', 'utilities', '_readdbc.abi3.so')
#                             )
#                 print(f'Copied _readdbc.abi3.so to pysus/utilities')
#             except:
#                 pass
#         atexit.register(_post_install)
#         install.run(self)
#
# def _post_install():
#     setuptools_path = setuptools.__path__[0].replace('/setuptools', '')
#     pysus_path = setuptools.__path__[0].replace('setuptools', 'pysus')
#
#     ## This code is to make sure the compiled extension module inside the
#     # utilities module is named _readdbc.so
#     if os.path.exists(pysus_path):
#         comp_mod_list = glob.glob(os.path.join(pysus_path, 'utilities', '_readdbc*.so'))
#         for mod in comp_mod_list:
#             mod = os.path.split(mod)[-1]
#             if mod != '_readdbc.so':
#                 print(f'Renaming {mod} to _readdbc.so')
#                 os.chdir(os.path.join(pysus_path, 'utilities'))
#                 os.rename(mod, '_readdbc.so')
#     # Sometimes the compiled module is thrown on the (site|dist)-packages directory
#     try:
#         shutil.copy(os.path.join(setuptools_path, '_readdbc.abi3.so'),
#                     os.path.join(setuptools_path, 'pysus', 'utilities', '_readdbc.abi3.so')
#                     )
#         print(f'Copied _readdbc.abi3.so to pysus/utilities')
#     except:
#         pass
# atexit.register(_post_install)
setup(
    name='PySUS',
    version='0.5.1',
    packages=find_packages(),
    package_data={
        '': ['*.c', '*.h', '*.o', '*.so', '*.md', '*.txt']
    },
    include_package_data=True,
    zip_safe=False,
    url='https://github.com/fccoelho/PySUS',
    license='gpl-v3',
    author='Flavio Codeco Coelho',
    author_email='fccoelho@gmail.com',
    description="Tools for dealing with Brazil's Public health data",
    long_description=ld,
    setup_requires=['cffi>=1.0.0', 'setuptools>26.0.0'],
    cffi_modules=["pysus/utilities/_build_readdbc.py:ffibuilder"],
    install_requires=['pandas', 'dbfread', 'cffi>=1.0.0', 'geocoder', 'requests', 'pyarrow', 'fastparquet'],
    # cmdclass={'install': PostInstall},
)




