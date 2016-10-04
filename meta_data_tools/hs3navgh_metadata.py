#!/usr/bin/python
import socket

from pyCMR.print_metadata import print_metadata
from read_headerful_table import read_headerful_table
from read_iwg1 import read_iwg1
from read_table_tll_tunits import read_table_tll_tunits

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/public/pub/hs3/NAV_GH/data"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.ict"        , (read_headerful_table, 0, 1, 2)),
                      ("*_0.dat"      , (read_table_tll_tunits, 28, 29, 30, "seconds")),
                      ("*_0raw.txt"   , None),
                      ("*.xml"        , None),
                      ("*_IWG1_*.txt" , (read_iwg1,)),
                    )

# The variables that are the same for every line
static_data = {}
static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
static_data["env"] = "ops"
static_data["project"] = "HS3"
static_data["ds"] = "hs3navgh"
static_data["inv"] = "HS3_INV"
static_data["browse"] = "N"

print_metadata(rootDir, patternsToParsers, static_data)
