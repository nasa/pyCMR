#!/usr/bin/python
import datetime
import os
import re
import socket
import sys

sys.path.append("/usr/local/lib/granule-metadata-tools")
from pyCMR.print_metadata import print_metadata

dateTimeFormat = "%Y%m%d_%H%M%S"
def findStartAndEndTimesInFilename(filename, ht):
    m = re.match(r".*(\d\d\d\d\d\d\d\d_\d\d\d\d\d\d).*",
                 os.path.basename(filename))
    if m:
        dt = datetime.datetime.strptime(m.group(1), dateTimeFormat)
        ht["start"] = dt
        ht["end"]   = dt + datetime.timedelta(minutes=1)

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/public/namma/2DS-CPI/browse"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.png" , None, findStartAndEndTimesInFilename),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "NAMMA"
static_data["ds"] = "nam2ds"
static_data["inv"] = "CAMEX_BROWSE_INV"
static_data["NLat"] = 30
static_data["SLat"] = -5
static_data["ELon"] = 10
static_data["WLon"] = -50
static_data["format"] = 'PNG'

print_metadata(rootDir, patternsToParsers, static_data)
