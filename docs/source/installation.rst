============
Installation
============

pip
---

The simplest way to install PySUS is via pip:

.. code-block:: bash

   pip install pysus

Extras
^^^^^^

For the terminal user interface (TUI):

.. code-block:: bash

   pip install pysus[tui]

Docker
------

A pre-built JupyterLab image is available on Docker Hub:

.. code-block:: bash

   docker pull alertadengue/pysus
   docker run -p 8888:8888 alertadengue/pysus

Or build locally and start the container:

.. code-block:: bash

   docker compose up --build

Then open http://127.0.0.1:8888/lab in your browser.

Stop the container with:

.. code-block:: bash

   docker compose down

Development
-----------

Using Conda:

.. code-block:: bash

   conda env create -f conda/dev.yaml
   conda activate pysus

Using Poetry:

.. code-block:: bash

   poetry install

Web Interface
-------------

PySUS includes a local Streamlit-based web server for browsing and downloading
datasets interactively. Start it with:

.. code-block:: bash

   pysus web

Or directly:

.. code-block:: bash

   streamlit run pysus/web/app.py

This opens a browser at ``http://localhost:8501`` with a graphical interface for
querying PySUS s3, DATASUS FTP, and dados.gov.br sources.

Configuration
-------------

Cache Directory
^^^^^^^^^^^^^^^

By default, downloaded files are cached in ``~/pysus``. Override this with the
``PYSUS_CACHEPATH`` environment variable:

.. code-block:: python

   import os
   os.environ["PYSUS_CACHEPATH"] = "/my/custom/path"
