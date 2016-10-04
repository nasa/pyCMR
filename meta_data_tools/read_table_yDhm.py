#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, re

def read_table_yDhm(filename, fp, yearField, dayOyField, hoursField, minutesField):
    maxSplit     = max(yearField,dayOyField,hoursField,minutesField) + 1
    r = {}
    foundValidTime = False
    for line in fp:
        try:
            tokens = line.split(None, maxSplit)
            dt = datetime.datetime(int(tokens[yearField]), 1, 1, int(tokens[hoursField]), int(tokens[minutesField]))
            dt += datetime.timedelta(int(tokens[dayOyField])-1)
            if not foundValidTime:
                minTime = dt
                maxTime = dt
                foundValidTime = True
            else:
                if dt < minTime:
                    minTime = dt
                if dt > maxTime:
                    maxTime = dt
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
    r = read_table_yDhm(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
