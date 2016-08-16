u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
import _readdbc as rdbc
from _readdbc import ffi, lib

print(dir(lib))
print(dir(ffi))
print(dir(rdbc))

def dbc2dbf(infile, outfile):
    infile = ffi.addressof(ffi.new('char[]', os.path.abspath(infile)))
    outfile = ffi.addressof(ffi.new('char[]', os.path.abspath(outfile)))
    lib.dbc2dbf(infile, outfile)
    print(os.path.exists(outfile))

if __name__ == "__main__":
    dbc2dbf(b'/tmp/DNRJ2014.dbc', b'test.dbf')

