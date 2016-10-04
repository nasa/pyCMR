# Command-line call:
# python generate_metadata.py DATA_SET_ID GRANULE_FILE_PATH
# The XML file can be pushed to CMR using pyCMR, or cURL

import datetime
import netCDF4
import os
import sys
import xml.sax.saxutils


granule_file_path = sys.argv[2]
# Not currently used, but allows us access to the netCDF's
# `groups` (eg, `Source`, `Description`, `ProjectName`) and
# `variables` (eg, `Latitude`, `Longitude`, `Time`)
granule = netCDF4.Dataset(granule_file_path, 'r')

SIMPLEST_GRANULE_TEMPLATE = '''
<Granule>
   <GranuleUR>{granule_ur}</GranuleUR>
   <InsertTime>{insert_time}</InsertTime>
   <LastUpdate>{last_update}</LastUpdate>
   <Collection>
     <DataSetId>{data_set_id}</DataSetId>
   </Collection>
   <Orderable>true</Orderable>
</Granule>
'''

granule_info = {
    'granule_ur': os.path.split(granule_file_path)[-1],
    'insert_time': datetime.datetime.utcnow().isoformat(),
    'last_update': datetime.datetime.utcnow().isoformat(),
    'data_set_id': sys.argv[1]
}
# Ensure that no XML-invalid characters are included
granule_info = {k:xml.sax.saxutils.escape(v) for k,v in granule_info.items()}

metadata_xml = SIMPLEST_GRANULE_TEMPLATE.format(**granule_info)

metadata_path = granule_file_path + '.meta.xml'
with open(metadata_path, 'w') as file_:
    file_.write(metadata_xml)
