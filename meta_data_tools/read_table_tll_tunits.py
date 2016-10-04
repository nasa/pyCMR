#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, re

def read_table_tll_tunits(filename, fp, timeField, latField, lonField, units):
    maxSplit  = max(timeField, latField, lonField) + 1
    r = {}
    foundValidTime = False
    foundValidLat  = False
    foundValidLon  = False
    m = re.match(r".*_(\d\d\d\d)(\d\d)(\d\d)_.*",
                 os.path.basename(filename))
    if m:
        startTime = datetime.datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    oneDay = datetime.timedelta(days=1)
    for line in fp:
        try:
            tokens = line.split(',', maxSplit)
            if units == "seconds":
                offsetTime = datetime.timedelta(seconds=float(tokens[timeField].strip(',')))
            elif units == "minutes":
                offsetTime = datetime.timedelta(minutes=float(tokens[timeField].strip(',')))
            elif units == "hours":
                offsetTime = datetime.timedelta(hours=float(tokens[timeField].strip(',')))
            dt = startTime + offsetTime
            lat = float(tokens[latField].strip(','))
            lon = float(tokens[lonField].strip(','))
            if not foundValidTime:
                minTime = dt
                maxTime = dt
                foundValidTime = True
            else:
                if dt < maxTime:  # Rollover
                    startTime += oneDay
                    dt = startTime + offsetTime
                maxTime = dt
            if lat >= -90 and lat <= 90 and lat != 0:
                if not foundValidLat:
                    minLat = lat
                    maxLat = lat
                    foundValidLat = True
                else:
                    if lat < minLat:
                        minLat = lat
                    if lat > maxLat:
                        maxLat = lat
            if lon >= -180 and lon <= 180 and lon != 0:
                if not foundValidLon:
                    minLon = lon
                    maxLon = lon
                    foundValidLon = True
                else:
                    if lon < minLon:
                        minLon = lon
                    if lon > maxLon:
                        maxLon = lon
        except:
            pass
    fp.close()
    if foundValidTime:
        r["start"] = minTime
        r["end"] = maxTime
    if foundValidLat:
        r["NLat"] = maxLat
        r["SLat"] = minLat
    if foundValidLon:
        r["ELon"] = maxLon
        r["WLon"] = minLon
    return r

if __name__ == "__main__":
    filename = sys.argv[1]
    args = sys.argv[2:]
    r = read_table_tll_tunits(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
