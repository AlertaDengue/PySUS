u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
import _readdbc as rdbc
from _readdbc import ffi, lib

def dbc2dbf(infile, outfile):
    p = ffi.new('char[]', os.path.abspath(infile))
    q = ffi.new('char[]', os.path.abspath(outfile))

    lib.dbc2dbf([p], [q])

    print(os.path.exists(outfile))

if __name__ == "__main__":
    dbc2dbf(b'/tmp/DNRJ2014.dbc', b'/tmp/output.dbf')

