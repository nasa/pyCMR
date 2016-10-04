#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime

def read_headerful_table(filename, fp, timeField, latField, lonField):
    maxSplit  = max(timeField, latField, lonField) + 1
    r = {}
    foundValidTime = False
    foundValidLat  = False
    foundValidLon  = False
    lineNumber = 0
    oneDay = datetime.timedelta(days=1)
    # First line appears to be number of header lines before table followed by 1001
    numHeaderLines = int(fp.readline().split()[0].strip(','))
    lineNumber += 1
    # Next 4 lines appear to be the source of the data
    source = (fp.readline().strip(), fp.readline().strip(), fp.readline().strip(), fp.readline().strip())
    lineNumber += 4
    # I don't know what 1, 1 refers to
    fp.readline()
    lineNumber += 1
    # The next line seems to be the year, month, day of start and some future year, month, day
    tokens = fp.readline().split(None, 3)
    startTime = datetime.datetime(int(tokens[0].strip(',')),
                                  int(tokens[1].strip(',')),
                                  int(tokens[2].strip(',')))
    lineNumber += 1
    # I don't know what 1.0000 means
    fp.readline()
    lineNumber += 1
    # Next is a line that descibes the time units.
    line = fp.readline()
    if "seconds" in line:
        units = 0
    if "minutes" in line:
        units = 1
    if "hours" in line:
        units = 2
    lineNumber += 1
    for x in xrange(lineNumber, numHeaderLines):
        fp.readline()
    # Now at start of data
    for line in fp:
        tokens = line.split(None, maxSplit)
        if units == 0:
            offsetTime = datetime.timedelta(seconds=float(tokens[timeField].strip(',')))
        elif units == 1:
            offsetTime = datetime.timedelta(minutes=float(tokens[timeField].strip(',')))
        elif units == 2:
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
    r = read_headerful_table(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
