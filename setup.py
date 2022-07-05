from setuptools import find_packages, setup

ld = """
PySUS
=====

This package collects a set of utilities for handling with public databases
published by Brazil's DATASUS
The documentation of how to use PySUS can be found
here: http://pysus.readthedocs.io/en/latest/

Features
--------

- Decode encoded patient age to any time unit (years, months, etc)
- Convert `.dbc` files to DBF databases or read them into pandas dataframes.
  DBC files are basically DBFs compressed by a proprietary algorithm.
- Loads SINAN DBC files into Pandas Dataframes
- Downloads SIA, SIH, SIM, SINASC, and ESUS (covid data)
- Geocodes SINAN notified cases in batch. You can use your Google API KEY
  to avoid Google's free limits.

Installation
------------
Make sure your system has libffi-dev package installed::

$ sudo pip install PySUS

"""

with open("requirements.txt") as f:
    requirements = f.readlines()

setup(
    name="PySUS",
    version="0.5.17",
    packages=find_packages(),
    package_data={"": ["*.c", "*.h", "*.o", "*.so", "*.md", "*.txt"]},
    include_package_data=True,
    zip_safe=False,
    url="https://github.com/fccoelho/PySUS",
    license="gpl-v3",
    author="Flavio Codeco Coelho",
    author_email="fccoelho@gmail.com",
    description="Tools for dealing with Brazil's Public health data",
    long_description=ld,
    setup_requires=["cffi>=1.0.0", "setuptools>26.0.0"],
    cffi_modules=["pysus/utilities/_build_readdbc.py:ffibuilder"],
    install_requires=requirements,
    # cmdclass={'install': PostInstall},
    extras_require={
        "dev": [
            "pytest",
            "pytest-cov",
            "pre-commit",
            "black",
            "isort",
            "flake8",
            "coverage",
            "wheel",
            "setuptools",
            "jupyterlab",
        ]
    },
    test_suite="tests",
    tests_require=["pytest", "flake8"],
)
