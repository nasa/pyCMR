#!/usr/bin/python
import socket

from pyCMR.print_metadata import print_metadata
from pyCMR.read_eol_sf import read_eol_sf

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/ops/public/pub/fieldCampaigns/hs3/AVAPS/data"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.eol" , (read_eol_sf,)),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "HS3"
static_data["ds"] = "hs3avaps2"
static_data["inv"] = "HS3_INV"
static_data["browse"] = "N"
static_data["format"] = "ASCII"

print_metadata(rootDir, patternsToParsers, static_data)
