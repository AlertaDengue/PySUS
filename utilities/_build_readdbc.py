u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
from cffi import FFI
blastbuilder = FFI()
ffibuilder = FFI()
with open(os.path.join(os.path.dirname(__file__), "c-src/blast.c")) as f:
    blastbuilder.set_source("blast", f.read(), libraries=["c"])
with open(os.path.join(os.path.dirname(__file__), "c-src/blast.h")) as f:
    blastbuilder.cdef(f.read())
blastbuilder.compile(verbose=True)

with open('c-src/dbc2dbf.c','r') as f:
    ffibuilder.set_source("_readdbc",
                          f.read(),
                          libraries=["c"],
                          sources=["c-src/blast.c"])
ffibuilder.cdef(
    """
    static unsigned inf(void *how, unsigned char **buf);
    static int outf(void *how, unsigned char *buf, unsigned len);
    void dbc2dbf(char** input_file, char** output_file);
    """
)

with open(os.path.join(os.path.dirname(__file__), "c-src/blast.h")) as f:
    ffibuilder.cdef(f.read(), override=True)

if __name__ == "__main__":
    # ffibuilder.include(blastbuilder)
    ffibuilder.compile(verbose=True)
