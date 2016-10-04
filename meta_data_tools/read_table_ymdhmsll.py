#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, re

def read_table_ymdhmsll(filename, fp, yearField, monthField, dayField, hoursField, minutesField, secondsField, latField, lonField):
    maxSplit     = max(yearField,monthField,dayField,hoursField,minutesField,secondsField,latField,lonField) + 1
    r = {}
    foundValidTime = False
    foundValidLat  = False
    foundValidLon  = False
    for line in fp:
        try:
            tokens = line.split(None, maxSplit)
            dt = datetime.datetime(int(tokens[yearField]), int(tokens[monthField]), int(tokens[dayField]), int(tokens[hoursField]), int(tokens[minutesField]), int(tokens[secondsField]))
            lat = float(tokens[latField])
            lon = float(tokens[lonField])
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
    r = read_table_ymdhmsll(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
