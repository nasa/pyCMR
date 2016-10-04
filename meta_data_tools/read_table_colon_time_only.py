#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, re

def read_table_colon_time_only(filename, fp, timeField, formatStr):
    maxSplit     = timeField + 1
    m = re.match(r".*_(\d\d\d\d\d\d\d\d)_.*",
                 os.path.basename(filename))
    if m:
        fileDate = datetime.datetime.strptime(m.group(1), "%Y%m%d")
    else:
        fileDate = None
    r = {}
    foundValidTime = False
    for line in fp:
        try:
            tokens = line.split(None, maxSplit)
            t = datetime.datetime.strptime(tokens[timeField], formatStr)
            if not foundValidTime:
                minTime = t
                maxTime = t
                foundValidTime = True
            else:
                if t < minTime:
                    minTime = t
                if t > maxTime:
                    maxTime = t
        except ValueError:
            pass
        except KeyboardInterrupt:
            pass
    fp.close()
    if foundValidTime and fileDate:
        r["start"] = datetime.datetime.combine(fileDate, minTime.time())
        r["end"] = datetime.datetime.combine(fileDate, maxTime.time())
    return r

if __name__ == "__main__":
    filename = sys.argv[1]
    args = sys.argv[2:]
    r = read_table_colon_time_only(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
