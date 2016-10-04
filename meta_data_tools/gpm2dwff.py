#!/usr/bin/python
from __future__ import print_function

import os
import re
import socket
import sys

sys.path.append("/usr/local/lib/granule-metadata-tools")
from pyCMR.print_metadata import print_metadata
from read_table_yDhm import read_table_yDhm
from read_table_colon_time_only import read_table_colon_time_only
from read_rain_events import read_rain_events

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/public/gpm_validation/related_projects/wff/2dvd/data"

def postParse(filename, ht):
    # Set "NLat","SLat","WLon","ELon"
    # From N375617_W752726
    m = re.match(r".*_([NS]\d*)[\.\d]*_([EW]\d*)[\.\d]*_.*",
                 os.path.basename(filename))
    if m:
        code = int(m.group(1)[1:])
        latS = code % 100
        latM = ((code - latS) % 10000) / 100
        latD = (code - 100*latM - latS) / 10000
        lat  = float(latD) + float(latM)/60 + float(latS)/3600
        if m.group(1)[0] == "S":
            lat = -lat
        ht["NLat"] = lat
        ht["SLat"] = lat

        code = int(m.group(2)[1:])
        lonS = code % 100
        lonM = ((code - lonS) % 10000) / 100
        lonD = (code - 100*lonM - lonS) / 10000
        lon  = float(lonD) + float(lonM)/60 + float(lonS)/3600
        if m.group(2)[0] == "W":
            lon = -lon
        ht["ELon"] = lon
        ht["WLon"] = lon
#    m = re.match(r".*_(\d\d\d\d\d\d\d\d)_.*",
#                 os.path.basename(filename))
#    if m:
#        if "start" not in ht:
#            ht["start"] = datetime.datetime.strptime(m.group(1), "%Y%m%d")
#        if "end" not in ht:
#            ht["end"] = datetime.datetime.strptime(m.group(1), "%Y%m%d") + datetime.timedelta(seconds=86399)

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*rainEvents.txt" , (read_rain_events, 0, 1, 2, 0, 3, 4), postParse),
                      ("*drops.txt" , (read_table_colon_time_only, 0, "%H:%M:%S.%f"), postParse),
                      ("*.txt" , (read_table_yDhm, 0 ,1 ,2 ,3), postParse),
                      ("*.tar" , None),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "WFF"
static_data["ds"] = "gpm2dwff"
static_data["inv"] = "GPM_INV"
static_data["browse"] = "N"
static_data["format"] = "ASCII"


if __name__ == "__main__":
    if len(sys.argv) > 1:
        rootDir=sys.argv[1]
        print_metadata(rootDir, patternsToParsers, static_data)
    else:
        print_metadata(".", (("*.py" , None),))

