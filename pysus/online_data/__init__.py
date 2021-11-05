u"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
from pathlib import Path

CACHEPATH = os.getenv("PYSUS_CACHEPATH", os.path.join(str(Path.home()), "pysus"))

# create pysus cache directory
if not os.path.exists(CACHEPATH):
    os.mkdir(CACHEPATH)


def cache_contents():
    """
    List the files currently cached in ~/pysus
    :return:
    """
    cached_data = os.listdir(CACHEPATH)
    return [os.path.join(CACHEPATH, f) for f in cached_data]
