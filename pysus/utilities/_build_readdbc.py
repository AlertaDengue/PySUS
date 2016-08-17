u"""
Created on 16/08/16
by fccoelho
license: GPL V3 or Later
"""
import os
from cffi import FFI

ffibuilder = FFI()

PATH = os.path.dirname(__file__)

with open(os.path.join(PATH, 'c-src/dbc2dbf.c'),'r') as f:
    ffibuilder.set_source("_readdbc",
                          f.read(),
                          libraries=["c"],
                          sources=[os.path.join(PATH, "c-src/blast.c")],
                          include_dirs=[os.path.join(PATH, "c-src/")]
                          )
ffibuilder.cdef(
    """
    static unsigned inf(void *how, unsigned char **buf);
    static int outf(void *how, unsigned char *buf, unsigned len);
    void dbc2dbf(char** input_file, char** output_file);
    """
)

with open(os.path.join(PATH, "c-src/blast.h")) as f:
    ffibuilder.cdef(f.read(), override=True)

if __name__ == "__main__":
    ffibuilder.compile(verbose=True)
