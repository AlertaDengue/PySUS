u"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
from pathlib import Path

# create pysus cache directory
if not os.path.exists(os.path.join(str(Path.home()), 'pysus')):
    os.mkdir(os.path.join(str(Path.home()), 'pysus'))

CACHEPATH = os.path.join(str(Path.home()), 'pysus')


def cache_contents():
    """
    List the files currently cached in ~/pysus
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]
