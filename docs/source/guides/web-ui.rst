==============
Web UI & CLI
==============

PySUS ships a Streamlit interface (``pysus web``) and a CLI built with
Typer.

Launching the web interface
---------------------------

.. code-block:: bash

   pysus web            # http://0.0.0.0:8501
   pysus web -p 8080    # custom port

Three tabs map to the three data clients:

* **Default (DuckLake)** — the S3 catalog, with group/state/year/month
  filters
* **FTP DATASUS** — legacy FTP browsing (auto-connects on tab open)
* **API DATASUS (DadosGov)** — the open-data API (token required)

Select files, queue them, and download with one click. Each query shows
the equivalent Python snippet to reproduce it in a script.

Installing the web extra
------------------------

.. code-block:: bash

   pip install pysus[web]

CLI Reference
-------------

.. code-block:: bash

   pysus version              # print the installed version
   pysus web                  # launch Streamlit web interface
   pysus web -p 8080          # custom port

Cache management:

.. code-block:: bash

   pysus cache status         # show cache statistics
   pysus cache clear          # clear all cached files

Configuration:

.. code-block:: bash

   pysus configure            # interactive configuration wizard

   # or set via environment variables:
   export PYSUS_CACHEPATH=/data/pysus
   export DADOSGOV_TOKEN=your-token

Source-specific CLI commands:

.. code-block:: bash

   pysus ducklake list-datasets       # list S3 catalog datasets
   pysus ducklake search dengue       # search S3 catalog
   pysus ftp list-datasets            # list FTP datasets
   pysus dadosgov list-datasets       # list DadosGov datasets
