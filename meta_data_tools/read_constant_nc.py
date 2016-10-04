#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, subprocess

dateTimeFormatISO = "%Y-%m-%dT%H:%M:%SZ"

CONSTANT_NCDUMP_AWK=r'''\
$1 == ":" timeFieldName {
  printf("baseTime=%s\n", strftime("%Y-%m-%dT%H:%M:%SZ", $3));
  printf("minTime=0\nmaxTime=0\ntimeUnits=seconds\n");
}
$1 == ":" latFieldName { printf("NLat=%f\nSLat=%f\n", $3, $3) }
$1 == ":" lonFieldName { printf("ELon=%f\nWLon=%f\n", $3, $3) }
'''

def read_constant_nc(filename, fp, timeFieldName, latFieldName, lonFieldName):
    fp.close()
    results = {}          # Where the keys,values go to be put on the 'q' to go back to the main thread
    # Use ncdump to get the values of the variables dumped out of the file
    ncdump_sp  = \
        subprocess.Popen(("ncdump", "-c", filename), stdout=subprocess.PIPE)
    # Run the awk script to read the ncdump and write out information on
    # min and max latitudes, longitudes, times, time units, and base time
    awk_result = \
        subprocess.Popen(("awk",
                          "-v", "timeFieldName=" + timeFieldName,
                          "-v", "latFieldName="  + latFieldName,
                          "-v", "lonFieldName="  + lonFieldName,
                          CONSTANT_NCDUMP_AWK),
                         stdin=ncdump_sp.stdout, stdout=subprocess.PIPE)
    # Create a hashtable for the results from awk
    a = {}
    for line in awk_result.communicate()[0].split("\n"):
        terms = line.split("=")
        if len(terms) == 2:
            a[terms[0].strip()] = terms[1].strip()
    # If Awk found latitudes, put them in the results hashtable
    for k in ["NLat", "SLat", "ELon", "WLon"]:
        if k in a:
            results[k] = a[k]
    # If Awk found baseTime, timeUnits, and a min and max, we can set start and end
    if "minTime" in a and "maxTime" in a and "baseTime" in a and "timeUnits" in a:
        bt = datetime.datetime.strptime(a["baseTime"], dateTimeFormatISO)
        # Flag to tell if we understood the timeUnits
        timeUnitsValid = True
        if a["timeUnits"] == "hours":
            std = datetime.timedelta(hours=float(a["minTime"]))
            etd = datetime.timedelta(hours=float(a["maxTime"]))
        elif a["timeUnits"] == "minutes":
            std = datetime.timedelta(minutes=float(a["minTime"]))
            etd = datetime.timedelta(minutes=float(a["maxTime"]))
        elif a["timeUnits"] == "seconds":
            std = datetime.timedelta(seconds=float(a["minTime"]))
            etd = datetime.timedelta(seconds=float(a["maxTime"]))
        else:
            timeUnitsValid = False
        if timeUnitsValid:
            results["start"] = (bt + std)
            results["end"]   = (bt + etd)
    return results

if __name__ == "__main__":
    filename = sys.argv[1]
    args = sys.argv[2:]
    r = read_constant_nc(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
