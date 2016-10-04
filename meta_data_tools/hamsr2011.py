#!/usr/bin/python
import socket

from pyCMR.print_metadata import print_metadata
from read_variable_nc_nolim import read_variable_nc_nolim

rootDir = "/ftp/public/sandbox/dev/public/pub/hs3/HAMSR2011"

def postParse(filename, ht):
    for i in ("NLat", "SLat", "ELon", "WLon"):
        if i in ht:
            ht[i] = float(ht[i]) / 1000

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.nc" , (read_variable_nc_nolim, "time", "lat", "lon"), postParse),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "HS3"
static_data["ds"] = "hs3hamsr"
static_data["inv"] = "HS3_INV"
static_data["browse"] = "N"

print_metadata(rootDir, patternsToParsers, static_data)
