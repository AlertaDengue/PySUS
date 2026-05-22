import os
import sys

sys.path.insert(0, os.path.abspath("../.."))

from pysus import get_version

# -- General configuration ------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.mathjax",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
    "sphinx.ext.napoleon",
    "nbsphinx",
]

intersphinx_mapping = {
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
}

templates_path = ["_templates"]
source_suffix = ".rst"
master_doc = "index"

project = "PySUS"
copyright = "2016, Flavio Codeco Coelho"
author = "Flavio Codeco Coelho"

version = get_version()
release = version

language = "en"

locale_dirs = ["locale"]
gettext_compact = False

exclude_patterns = ["_build", "**.ipynb_checkpoints"]

pygments_style = "sphinx"


# -- Options for HTML output ----------------------------------------------

html_theme = "sphinx_rtd_theme"
htmlhelp_basename = "PySUSdoc"


# -- Options for LaTeX output ---------------------------------------------

latex_documents = [
    (
        master_doc,
        "PySUS.tex",
        "PySUS Documentation",
        "Flavio Codeco Coelho",
        "manual",
    ),
]


# -- Options for manual page output ---------------------------------------

man_pages = [(master_doc, "pysus", "PySUS Documentation", [author], 1)]


# -- Options for Texinfo output -------------------------------------------

texinfo_documents = [
    (
        master_doc,
        "PySUS",
        "PySUS Documentation",
        author,
        "PySUS",
        "Python package for accessing and analyzing Brazil's public health data.",
        "Miscellaneous",
    ),
]


# -- Options for Epub output ----------------------------------------------

epub_title = project
epub_author = author
epub_publisher = author
epub_copyright = copyright
epub_exclude_files = ["search.html"]
