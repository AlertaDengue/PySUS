=========
Tutorials
=========

PySUS includes some Jupyter notebooks in its distribution package to serve as tutorials.

Working with SINASC databases
=============================


Working with SINAN files
========================

The sinan module in the preprocessing package can load SINAN files from DBF, returning a pandas DataFrame fixing the typing of some columns.

It also offers geocoding capabilities which attributes geographical coordinates to every notified case in a SINAN Dataframe.
You can use your Google API KEY to avoid Google's free limits. To do this just create an environment variable called GOOGLE_API_KEY.
Warning: This can take a long time! and can stop halfway through, due to connections timing out. But PySUS creates knows how to restart from the last
geocoded address.
