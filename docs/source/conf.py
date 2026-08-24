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

# pydantic v2 internals are not documentable; excluding them keeps the
# build warning-free under -W
autodoc_default_options = {
    "exclude-members": (
        "__pydantic_validator__,__pydantic_serializer__,"
        "model_config,model_fields"
    ),
}


def _skip_pydantic_internals(app, what, name, obj, skip, options):
    """Never document pydantic's runtime internals."""
    if name.startswith("__pydantic_"):
        return True
    return skip


def setup(app):
    import warnings

    app.connect("autodoc-skip-member", _skip_pydantic_internals)
    # Sphinx 5.3 autodoc re-emits docutils RST parsing warnings when
    # processing inherited class attributes on exception subclasses.
    # These are cosmetic (the docs render correctly) and cannot be
    # suppressed via suppress_warnings.
    warnings.filterwarnings(
        "ignore",
        message="Inline literal start-string without end-string",
        module="docutils",
    )
    print("DEBUG: skip-member handler registered", flush=True)

intersphinx_mapping = {
    "sqlalchemy": ("https://docs.sqlalchemy.org/en/20/", None),
}

# Ambiguous short cross-references (e.g. 'Dataset' exists in the ftp and
# dadosgov models) and other python-domain resolution noise are
# suppressed; fully-qualified references are used in new docstrings.
suppress_warnings = ["ref.python"]

templates_path = ["_templates"]

# Explicitly map extensions to ensure notebooks are routed to nbsphinx
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
