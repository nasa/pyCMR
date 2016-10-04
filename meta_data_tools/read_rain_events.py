#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, re

def read_rain_events(filename, fp, yearField1, dayOyField1, timeField1, yearField2, dayOyField2, timeField2):
    maxSplit     = max(yearField1, dayOyField1, timeField1, yearField2, dayOyField2, timeField2) + 1
    r = {}
    foundValidTime = False
    for line in fp:
        try:
            tokens = line.split(None, maxSplit)
            t1 = datetime.datetime(int(tokens[yearField1]), 1, 1, int(tokens[timeField1].split(':')[0]), int(tokens[timeField1].split(':')[1]))
            t1 += datetime.timedelta(int(tokens[dayOyField1])-1)
            t2 = datetime.datetime(int(tokens[yearField2]), 1, 1, int(tokens[timeField2].split(':')[0]), int(tokens[timeField2].split(':')[1]))
            t2 += datetime.timedelta(int(tokens[dayOyField2])-1)
            if not foundValidTime:
                minTime = t1
                maxTime = t2
                foundValidTime = True
            else:
                if t1 < minTime:
                    minTime = t1
                if t2 > maxTime:
                    maxTime = t2
        except KeyboardInterrupt:
            pass
    fp.close()
    if foundValidTime:
        r["start"] = minTime
        r["end"] = maxTime
    return r

if __name__ == "__main__":
    filename = sys.argv[1]
    args = sys.argv[2:]
    r = read_rain_events(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
