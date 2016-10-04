#!/usr/bin/python
import re
import socket

from pyCMR.print_metadata import print_metadata
from read_table_ymdhmll import read_table_ymdhmll
from read_table_ymdhmsll import read_table_ymdhmsll

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/public/gpm_validation/iphex/disdrometers_and_gauges/rain_gauge_NASA/data"

# A function to calculate a new name of a datafile
ymdFormat = "%Y%m%d"
def renamer(filename, md):
    startDT = md["start"]
    endDT = md["end"]
    # IPHEx-NASA0026_A-2013.gmin -> iphex_raingauge_NASA0026_A_20130911_20131229_gmin.txt
    m = re.match(r"(.*)IPHEx-NASA(....)_(.)-.....(.*)", filename)
    if m:
        return m.group(1) + "iphex_raingauge_NASA" + m.group(2) + "_" + m.group(3) + "_" + startDT.strftime(ymdFormat) + "_" + endDT.strftime(ymdFormat) + "_" + m.group(4) + ".txt"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.gmin"    , (read_table_ymdhmll, 0, 1, 2, 4, 5, 7, 8), renamer),
                      ("*.gag"     , (read_table_ymdhmsll, 0, 1, 2, 4, 5, 6, 8, 9), renamer),
                      ("*gmin.txt" , (read_table_ymdhmll, 0, 1, 2, 4, 5, 7, 8)),
                      ("*gag.txt"  , (read_table_ymdhmsll, 0, 1, 2, 4, 5, 6, 8, 9)),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "IPHEX"
static_data["ds"] = "gpmrgnaiphx2"
static_data["inv"] = "GPM_INV"
static_data["browse"] = "N"

# The subdirs of root to limit the search.  Set to [ ] or None in order to not limit
# Set to list of directories to limit, as in ( "dir1", "dir2" )
topLevelDirs = ( "gauge", "gmin" )

print_metadata(rootDir, patternsToParsers, static_data, topLevelDirs)
