#!/usr/bin/python
import datetime
import re
import socket

from pyCMR.print_metadata import print_metadata

rootDir = "/ftp/public/pub/fieldCampaigns/gpmValidation/ifloods/XPOL/data"

radarViews = {
    "XPOL2" : {
        "NLat" : 43.537461,
        "SLat" : 42.81995,
        "ELon" : -91.365653,
        "WLon" : -92.351086
        },
    "XPOL3" : {
        "NLat" : 42.004147,
        "SLat" : 41.283644,
        "ELon" : -91.142367,
        "WLon" : -92.104153
        },
    "XPOL4" : {
        "NLat" : 43.282133,
        "SLat" : 42.54235,
        "ELon" : -90.911408,
        "WLon" : -91.900978
        },
    "XPOL5" : {
        "NLat" : 42.250956,
        "SLat" : 41.529344,
        "ELon" : -91.246122,
        "WLon" : -92.215189
        }
}

def ncPostParse(filename, ht):
    m = re.match(r"(XPOL.)/.*(\d\d\d\d\d\d\d\d-\d\d\d\d\d\d).*",
                 filename)
    if m:
        dt = datetime.datetime.strptime(m.group(2), "%Y%m%d-%H%M%S")
        ht["start"] = dt - datetime.timedelta(minutes=1, seconds=12)
        ht["end"]   = dt
        radarName = m.group(1)
        if radarName in radarViews:
            ht["NLat"] = radarViews[radarName]["NLat"]
            ht["SLat"] = radarViews[radarName]["SLat"]
            ht["ELon"] = radarViews[radarName]["ELon"]
            ht["WLon"] = radarViews[radarName]["WLon"]
    ht["format"] = "netCDF-3"
    ht["inv"]    = "GPM_INV"

patternsToParsers = (
                      ("*.nc"  , None, ncPostParse ),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"]    = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"]     = "ops"
static_data["project"] = "GPMGV"
static_data["ds"]      = "gpmxpolifld"

print_metadata(rootDir, patternsToParsers, static_data)
