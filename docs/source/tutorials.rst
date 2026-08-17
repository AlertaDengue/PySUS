=========
Tutorials
=========

Jupyter notebooks with usage examples are available in the source repository:

* `API Overview <https://github.com/AlertaDengue/PySUS/blob/main/pysus/api/README.ipynb>`_
* `DuckLake Client <https://github.com/AlertaDengue/PySUS/blob/main/pysus/api/ducklake/README.ipynb>`_
* `FTP Client <https://github.com/AlertaDengue/PySUS/blob/main/pysus/api/ftp/README.ipynb>`_
* `DadosGov Client <https://github.com/AlertaDengue/PySUS/blob/main/pysus/api/dadosgov/README.ipynb>`_

Quick Start
-----------

Simplified Database Functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pysus import sinan, sinasc, sim, sih, sia, pni, ibge, cnes, ciha

   # Download SINAN Dengue data
   df = sinan(disease="deng", year=2000)

   # Multiple years
   df = sinan(disease="deng", year=[2023, 2024])

   # SINASC births for São Paulo
   df = sinasc(state="SP", year=[2020, 2021, 2022, 2023])

   # SIM mortality data
   df = sim(state="SP", year=2024)

   # SIH hospitalizations with month filter
   df = sih(state="SP", year=2024, month=[1, 2, 3])

   # CNES health facilities
   df = cnes(state="SP", year=2024, month=1)

Listing Available Files
^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pysus import list_files

   list_files("SINAN")

Using the PySUS Client
^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python

   from pysus import PySUS

   async def main():
       async with PySUS() as pysus:
           # Query DuckLake catalog
           files = await pysus.query(
               dataset="sinan",
               group="DENG",
               state="SP",
               year=2024,
           )

           # Download files
           for f in files:
               local = await pysus.download(f)
               print(local.path)

           # Read multiple parquet files
           import glob
           paths = glob.glob("/cache/sinan/**/*.parquet")
           df = pysus.read_parquet(paths, mode="union").df()

read_parquet Modes
^^^^^^^^^^^^^^^^^^

.. code-block:: python

   # Union (default) - all columns from any file
   df = pysus.read_parquet(paths, mode="union").df()

   # Intersection - only common columns across all files
   df = pysus.read_parquet(paths, mode="intersection").df()

   # Strict - raises error if schemas don't match
   df = pysus.read_parquet(paths, mode="strict").df()

   # With custom SQL filter
   df = pysus.read_parquet(paths, sql="SELECT * WHERE column > 100").df()

Terminal User Interface (TUI)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: bash

   pysus tui -l pt

Or from Python:

.. code-block:: python

   from pysus.tui.app import PySUS
   app = PySUS(lang="pt")
   app.run()
