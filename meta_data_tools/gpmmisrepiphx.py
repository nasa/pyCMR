#!/usr/bin/python
import datetime
import os
import re
import socket
import sys

sys.path.append("/usr/local/lib/granule-metadata-tools")
from pyCMR.print_metadata import print_metadata

dateTimeFormat = "%Y-%m-%dT%H%M"
dateTimeFormat2 = "%Y-%m-%dT%H-%M"
def findStartAndEndTimesInFilename(filename, ht):
    if re.match(".*tar", filename):
        ht["format"] = "TAR"
    if re.match(".*pdf", filename):
        ht["format"] = "PDF"
    # pairs of strings like 2014-05-11T1137
    m = re.match(r".*(\d\d\d\d-\d\d-\d\dT\d\d\d\d).*(\d\d\d\d-\d\d-\d\dT\d\d\d\d).*",
                 os.path.basename(filename))
    if m:
        ht["start"] = datetime.datetime.strptime(m.group(1), dateTimeFormat)
        ht["end"]   = datetime.datetime.strptime(m.group(2), dateTimeFormat)
        if ht["end"] < ht["start"]:
            t = ht["end"]
            ht["end"] = ht["start"]
            ht["start"] = t
        return
    # Same as above, except with a '-' between the hour and minute
    m = re.match(r".*(\d\d\d\d-\d\d-\d\dT\d\d-\d\d).*(\d\d\d\d-\d\d-\d\dT\d\d-\d\d).*",
                 os.path.basename(filename))
    if m:
        ht["start"] = datetime.datetime.strptime(m.group(1), dateTimeFormat2)
        ht["end"]   = datetime.datetime.strptime(m.group(2), dateTimeFormat2)
        if ht["end"] < ht["start"]:
            t = ht["end"]
            ht["end"] = ht["start"]
            ht["start"] = t
        return

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/ops/public/gpm_validation/iphex/reports/data"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.pdf" , None, findStartAndEndTimesInFilename),
                      ("*.tar" , None, findStartAndEndTimesInFilename),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "IPHEX"
static_data["ds"] = "gpmmisrepiphx"
static_data["inv"] = "GPM_INV"
static_data["browse"] = "N"

print_metadata(rootDir, patternsToParsers, static_data)
