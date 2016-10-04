#!/usr/bin/python
from __future__ import print_function
import os, sys, datetime, subprocess

dateTimeFormatISO = "%Y-%m-%dT%H:%M:%SZ"

VARIABLE_NCDUMP_AWK='''\
$1 == "variables:" {inVariables=1; inData=0; next; }
$1 == "data:"      {inVariables=0; inData=1; next; }
inData && $1 == timeFieldName { inTime      = 1; maxTime = minTime = $3 }
inData && $1 == latFieldName  { inLatitude  = 1; NLat = SLat = 0 }
inData && $1 == lonFieldName  { inLongitude = 1; ELon = WLon = 0 }
$1 == timeFieldName ":units" && $4 == "since" {
  sub(/"/, "", $5);
  baseTime=$5;
  sub(/"/, "", $3);
  timeUnits=$3;
}
inTime{
  for (i=1; i<=NF; i++)
    if ($i != "=" && $i != ";" && $i != timeFieldName)
      if ($i > maxTime) maxTime = $i;
}
inLatitude{
  for (i=1; i<=NF; i++)
    if ($i != "=" && $i != ";" && $i != latFieldName)
    {
      if ($i <=90 && $i >= -90 && $i != 0)
      {
        if (NLat==0 || $i > NLat) NLat = $i;
        if (SLat==0 || $i < SLat) SLat = $i;
      }
    }
}
inLongitude{
  for (i=1; i<=NF; i++)
    if ($i != "=" && $i != ";" && $i != lonFieldName)
    {
      if ($i <=180 && $i >= -180 && $i != 0)
      {
        if (ELon==0 || $i > ELon) ELon = $i;
        if (WLon==0 || $i < WLon) WLon = $i;
      }
    }
}
inTime      && $NF == ";"{ inTime      = 0; }
inLatitude  && $NF == ";"{ inLatitude  = 0; }
inLongitude && $NF == ";"{ inLongitude = 0; }
END{
  print "minTime=" minTime;
  print "maxTime=" maxTime;
  if (baseTime) print "baseTime=" baseTime;
  if (timeUnits) print "timeUnits=" timeUnits;
  if (NLat) print "NLat=" NLat;
  if (SLat) print "SLat=" SLat;
  if (ELon) print "ELon=" ELon;
  if (WLon) print "WLon=" WLon;
}
'''

def read_variable_nc(filename, fp, timeFieldName, latFieldName, lonFieldName):

    fp.close()
    results = {}          # Where the keys,values go to be put on the 'q' to go back to the main thread
    # Use ncdump to get the values of the variables dumped out of the file
    ncdump_sp  = \
        subprocess.Popen(("ncdump", "-v",
                          "{0},{1},{2}".format(timeFieldName,
                                               latFieldName,
                                               lonFieldName), filename), stdout=subprocess.PIPE)
    # Use tr to get rid of all the commas so our awk script doesn't have to deal with them
    tr_sp      = \
        subprocess.Popen(("tr", "-d", ","), stdin=ncdump_sp.stdout, stdout=subprocess.PIPE)
    # Run the awk script to read the ncdump and write out information on
    # min and max latitudes, longitudes, times, time units, and base time
    awk_result = \
        subprocess.Popen(("awk",
                          "-v", "timeFieldName=" + timeFieldName,
                          "-v", "latFieldName="  + latFieldName,
                          "-v", "lonFieldName="  + lonFieldName,
                          VARIABLE_NCDUMP_AWK),
                         stdin=tr_sp.stdout, stdout=subprocess.PIPE)
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
    r = read_variable_nc(filename, open(filename), *args)
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
