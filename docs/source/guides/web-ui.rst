=========
Web UI
=========

PySUS ships a Streamlit interface (``pysus web``) and a CLI built with
Typer.

Launching the web interface
---------------------------

.. code-block:: bash

   pysus web            # http://0.0.0.0:8501
   pysus web -p 8080    # custom port

Three tabs map to the three clients:

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

CLI
---

.. code-block:: bash

   pysus version        # print the installed version
