'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
from __future__ import print_function
import os, sys, datetime

def read_eol_sf(filename, fp):
    r = {}
    fp.readline() # Data Type/Direction
    fp.readline() # File Format/Version
    fp.readline() # Project Name/Platform
    fp.readline() # Launch Site

    # Launch Location example: Launch Location (lon,lat,alt):             61 33.33'W -61.555444, 31 34.35'N 31.572562, 19156.16
    launchLocString = fp.readline().strip().split(':', 1)[1].strip()
    launchLocTokens = launchLocString.split()
    lat = float(launchLocTokens[5].strip(','))
    lon = float(launchLocTokens[2].strip(','))
    minLat = lat
    maxLat = lat
    minLon = lon
    maxLon = lon

    # Next is like 'UTC Launch Time (y,m,d,h,m,s): 2012, 09, 07, 12:41:16'
    timeString = fp.readline().strip().split(':', 1)[1].strip()
    startTime  = datetime.datetime.strptime(timeString, "%Y, %m, %d, %H:%M:%S")
    minTime = startTime
    maxTime = startTime

    fp.readline() # Sonde Id/Sonde Type
    fp.readline() # Reference Launch Data Source/Time
    fp.readline() # System Operator/Comments
    fp.readline() # Post Processing Comments
    fp.readline() # /
    fp.readline() # Field Names
    fp.readline() # Units
    fp.readline() # -----

    # Now at start of data
    for line in fp:
        tokens     = line.replace('-', ' -').split()
        offsetTime = datetime.timedelta(seconds=float(tokens[0]))
        dt         = startTime + offsetTime
        lat        = float(tokens[15])
        lon        = float(tokens[14])
        if dt < minTime:
            minTime = dt
        if dt > maxTime:
            maxTime = dt
        if lat >= -90 and lat <= 90 and lat != 0:
            if lat < minLat and (minLat - lat) < 1:
                minLat = lat
            if lat > maxLat and (lat - maxLat) < 1:
                maxLat = lat
        if lon >= -180 and lon <= 180 and lon != 0:
            if lon < minLon and (minLon - lon) < 1:
                minLon = lon
            if lon > maxLon and (lon - maxLon) < 1:
                maxLon = lon
    fp.close()
    r["start"] = minTime
    r["end"] = maxTime
    r["NLat"] = maxLat
    r["SLat"] = minLat
    r["ELon"] = maxLon
    r["WLon"] = minLon
    return r

if __name__ == "__main__":
    filename = sys.argv[1]
    args = sys.argv[2:]
    r = read_eol_sf(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
