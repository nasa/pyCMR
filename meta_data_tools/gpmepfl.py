#!/usr/bin/python
from __future__ import print_function

import datetime
import os
import re
import socket
import sys

sys.path.append("/usr/local/lib/granule-metadata-tools")
from pyCMR.print_metadata import print_metadata

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/ops/public/gpm_validation/related_projects/epfl/data"

stationCoords = {}
stationCoords["10"] = ( 46.520500, 6.565200 )
stationCoords["11"] = ( 46.520433, 6.562833 )
stationCoords["12"] = ( 46.521900, 6.565183 )
stationCoords["13"] = ( 46.521267, 6.566767 )
stationCoords["20"] = ( 46.519800, 6.570500 )
stationCoords["21"] = ( 46.519583, 6.572317 )
stationCoords["22"] = ( 46.521200, 6.572583 )
stationCoords["23"] = ( 46.520533, 6.571100 )
stationCoords["30"] = ( 46.518333, 6.563933 )
stationCoords["31"] = ( 46.519650, 6.563900 )
stationCoords["32"] = ( 46.518700, 6.562733 )
stationCoords["33"] = ( 46.517633, 6.564583 )
stationCoords["40"] = ( 46.521017, 6.569733 )
stationCoords["41"] = ( 46.519500, 6.567883 )
stationCoords["42"] = ( 46.520600, 6.567850 )
stationCoords["43"] = ( 46.521400, 6.567867 )

almostDay = datetime.timedelta(seconds=86399)

def postParseTxt(filename, ht):
    m = re.match(r"DSDfilt-(\d\d)_ascii_(\d\d\d\d\d\d\d\d).txt.gz",
                 os.path.basename(filename))
    if m:
        station = m.group(1)
        dt = datetime.datetime.strptime(m.group(2), "%Y%m%d")
        if "start" not in ht:
            ht["start"] = dt
            ht["end"]   = dt + almostDay
        if station in stationCoords:
            lat, lon = stationCoords[station]
            ht["NLat"] = lat
            ht["SLat"] = lat
            ht["ELon"] = lon
            ht["WLon"] = lon

def postParseDat(filename, ht):
    m = re.match(r"(\d\d)_ascii_(\d\d\d\d\d\d\d\d).dat.gz",
                 os.path.basename(filename))
    if m:
        station = m.group(1)
        dt = datetime.datetime.strptime(m.group(2), "%Y%m%d")
        if "start" not in ht:
            ht["start"] = dt
            ht["end"]   = dt + almostDay
        if station in stationCoords:
            lat, lon = stationCoords[station]
            ht["NLat"] = lat
            ht["SLat"] = lat
            ht["ELon"] = lon
            ht["WLon"] = lon

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.txt.gz" , None, postParseTxt),
                      ("*.dat.gz" , None, postParseDat),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "TBD"
static_data["ds"] = "gpmepfl"
static_data["inv"] = "GPM_INV"
static_data["browse"] = "N"
static_data["format"] = "ASCII"

print_metadata(rootDir, patternsToParsers, static_data)
