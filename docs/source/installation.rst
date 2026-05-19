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

For DBC file support (requires ``libffi``):

.. code-block:: bash

   # Ubuntu/Debian
   sudo apt install libffi-dev
   pip install pysus[dbc]

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

   docker compose -f docker/docker-compose.yaml up --build

Then open http://127.0.0.1:8888/lab in your browser.

Stop the container with:

.. code-block:: bash

   docker compose -f docker/docker-compose.yaml down

Development
-----------

Using Conda:

.. code-block:: bash

   conda env create -f conda/dev.yaml
   conda activate pysus

Using Poetry:

.. code-block:: bash

   poetry install

Configuration
-------------

Cache Directory
^^^^^^^^^^^^^^^

By default, downloaded files are cached in ``~/pysus``. Override this with the
``PYSUS_CACHEPATH`` environment variable:

.. code-block:: python

   import os
   os.environ["PYSUS_CACHEPATH"] = "/my/custom/path"
