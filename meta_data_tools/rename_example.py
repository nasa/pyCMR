#!/usr/bin/python
import os

from pyCMR.print_metadata import print_metadata
from pyCMR.read_variable_nc import read_variable_nc

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/home/nwharton/nc"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = ( ("*.nc" , (read_variable_nc, "timed", "lat", "lon")),)

def mover(filename, startDT, endDT):
    bn = os.path.basename(filename)
    return startDT.strftime("%Y") + "/" + startDT.strftime("%m") + "/" + startDT.strftime("%d") + "/" + bn

print_metadata(rootDir, patternsToParsers, renameFunction=mover)
