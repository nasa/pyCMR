#!/usr/bin/python
import socket

from pyCMR.print_metadata import print_metadata
from pyCMR.read_variable_nc import read_variable_nc

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/public/gpm_validation/iphex/HIWRAP/data"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.nc" , (read_variable_nc, "timed", "lat", "lon")),
                      ("*.h5" , (read_variable_nc, "timeUTC", "lat", "lon") ),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "IPHEX"
static_data["ds"] = "gpmhiwrapiphx"
static_data["inv"] = "GPM_INV"
static_data["browse"] = "N"

print_metadata(rootDir, patternsToParsers, static_data)
