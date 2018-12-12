u"""
Created on 21/09/18
by fccoelho
license: GPL V3 or Later
"""
import os
from pathlib import Path

# create pysus cache directory
if not os.path.exists(os.path.join(Path.home(),'pysus')):
    os.mkdir(os.path.join(Path.home(), 'pysus'))

CACHEPATH = os.path.join(Path.home(), 'pysus')

