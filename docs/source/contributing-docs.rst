==========================
Contributing Documentation
==========================

The docs are Sphinx sources in ``docs/source``, published on ReadTheDocs.
Docstrings are the single source of truth (numpydoc style, English);
prose lives in the guides.

Building locally
----------------

.. code-block:: bash

   sphinx-build -W --keep-going -b html docs/source docs/_build/html

The ``-W`` flag turns warnings into errors — keep the build clean.

Checking links
--------------

.. code-block:: bash

   sphinx-build -b linkcheck docs/source docs/_build/linkcheck

Where things live
-----------------

* ``quickstart.rst`` — five-minute start
* ``guides/`` — client and topic guides
* ``api.rst`` — ``automodule`` directives only (no hand-written API pages)
* ``tutorials.rst`` — notebook-style tutorials
* ``migration.rst`` — breaking changes between major versions

Docstring style
---------------

numpydoc conventions: summary line, ``Parameters`` / ``Returns`` /
``Raises`` sections, one blank line between sections. Every public
module must appear in ``api.rst``; every public class/method gets a
docstring (pydocstyle checks run in CI).

Translations
------------

Messages are extracted with gettext:

.. code-block:: bash

   sphinx-build -b gettext docs/source docs/_build/locale

Update ``locale/pt`` and ``locale/pt_BR`` with ``sphinx-intl`` and add
the compiled catalogs to the repo.
