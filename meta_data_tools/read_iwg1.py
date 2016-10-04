#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime

dateTimeFormatNAV = "%Y-%m-%dT%H:%M:%S.%f"
def read_iwg1(filename, fp):
    r = {}
    foundValidTime = False
    foundValidLat  = False
    foundValidLon  = False
    for line in fp:
        tokens = line.split(',')
        dt = datetime.datetime.strptime(tokens[1], dateTimeFormatNAV)
        lat = float(tokens[2])
        lon = float(tokens[3])
        if not foundValidTime:
            minTime = dt
            maxTime = dt
            foundValidTime = True
        else:
            if dt < minTime:
                minTime = dt
            if dt > maxTime:
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
    r = read_iwg1(filename, open(filename))
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
