=================================
Welcome to PySUS documentation!
=================================

.. image:: https://badge.fury.io/py/pysus.svg
   :target: https://pypi.org/project/PySUS/
   :alt: PyPI version
.. image:: https://readthedocs.org/projects/pysus/badge/?version=latest
   :target: https://pysus.readthedocs.io/en/latest/?badge=latest
   :alt: Documentation Status
.. image:: https://zenodo.org/badge/63720586.svg
   :target: https://zenodo.org/badge/latestdoi/63720586
   :alt: DOI
.. image:: https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml/badge.svg
   :target: https://github.com/AlertaDengue/PySUS/actions/workflows/release.yaml
   :alt: Release

PySUS is a Python package for accessing and analyzing Brazil's public health data
(`DATASUS <https://datasus.saude.gov.br/>`_). It provides tools to download, process,
and work with health datasets including SINAN (disease notifications), SIM (mortality),
SINASC (births), SIH (hospitalizations), SIA (ambulatory), CIHA, CNES, PNI, and more.

A local web server (`pysus web`) is included for interactive browsing and downloading
of datasets through a graphical Streamlit interface.

This documentation covers PySUS 2.0+.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   Installation <installation>
   Data Sources <databases/data-sources>
   Working with DATASUS data <working-with-datasus-data>
   Tutorials <tutorials>
   API Reference <api>



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
