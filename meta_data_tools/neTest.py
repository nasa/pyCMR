from  pyCMR.read_eol_sf import read_eol_sf

# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory
rootDir = "/ftp/ops/public/pub/fieldCampaigns/hs3/AVAPS/data"

# The file patterns to look at and the functions to parse them followed by arguments for the parser
patternsToParsers = (
                      ("*.eol" , (read_eol_sf,)),
                    )