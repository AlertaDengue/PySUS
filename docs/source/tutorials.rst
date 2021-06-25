=========
Tutorials
=========

PySUS includes some Jupyter notebooks in its distribution package to serve as tutorials.

Working with SINASC databases
=============================
SINASC is the national registry of live births. With PySUS, You can download SINASC tables directly and have them as dataframes to integrate in your analysis.


Working with SINAN files
========================

SINAN is the national registry of cases for diseases of required reporting. PySUS offers the possibility of downloading records of individual cases selected for futher laboratory investigation, not the entirety of the reported cases.
To see how to download these data look at the example notebook provided.

The sinan module in the preprocessing package can load SINAN files from DBF, returning a pandas DataFrame fixing the typing of some columns.

It also offers geocoding capabilities which attributes geographical coordinates to every notified case in a SINAN Dataframe.
You can use your Google API KEY to avoid Google's free limits. To do this just create an environment variable called GOOGLE_API_KEY.
Warning: This can take a long time! and can stop halfway through, due to connections timing out. But PySUS creates knows how to restart from the last
geocoded address.

Working with SIH DATA
=====================
SIH is DATASUS' Hospital information system and it contains detailed information about hospitalizations. SIH Data can also be downloaded directly with PySUS.
