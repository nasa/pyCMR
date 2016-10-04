#!/usr/bin/python
from __future__ import print_function
import sys, struct, datetime

headerStruct = struct.Struct('>' # > means big endian, no alignment
'I'  # Record Length
)

bodyStruct = struct.Struct('>' # > means big endian, no alignment
     # Index Byte   
'2s' # 0        1   UF (ASCII)
'h'  # 1        2   Record length (16-bit words)
'h'  # 2        3   Position of first word of nonmandatory header block.  (If no
     #               nonmandatory header block exists, this points to the first
     #               existing header block following the mandatory.  In this way,
     #               word (3) always gives 1 + the length of the mandatory header.
'h'  # 3        4   Position of first word of local use header block.  (If no local
     #                use headers exist, this points to the start of the data
     #                header block.)
'h'  # 4        5   Position of first word of data header block
'h'  # 5        6   Physical record number relative to beginning of file
'h'  # 6        7   Volume scan number relative to beginning of tape
'h'  # 7        8   Ray number within volume scan
'h'  # 8        9   Physical record number within the ray (one for the first
     #               physical record of each ray)
     #      
'h'  # 9       10   Sweep number within this volume scan
'8s' # 10   11-14   Radar name (8ASCII characters, includes processor ID.)
'8s' # 11   15-18   Site name (8 ASCII characters)
'h'  # 12      19   Degrees of latitude (North is positive, South is negative)
'h'  # 13      20   Minutes of latitude
'h'  # 14      21   Seconds (x64) of latitude
'h'  # 15      22   Degrees of longitude (East is positive, West is negative)
'h'  # 16      23   Minutes of longitude
'h'  # 17      24   Seconds (x64) of longitude (Note:  minutes and seconds have same
     #               sign as degrees.)
'h'  # 18      25   Height of antenna above sea level (meters)
'h'  # 19      26   Year (of data)  (last 2 digits)
'h'  # 20      27   Month
'h'  # 21      28   Day
'h'  # 22      29   Hour
'h'  # 23      30   Minute
'h'  # 24      31   Second
'2s' # 25      32   Time zone (2 ASCII -- UT, CS, MS, etc.)
'h'  # 26      33   Azimuth (degrees x 64) to midpoint of sample
'h'  # 27      34   Elevation (degrees x 64)
'h'  # 28      35   Sweep mode:  0 - Calibration
     #                           1 - PPI (Constant elevation)
     #                           2 - Coplane
     #                           3 - RHI (Constant azimuth)
     #                           4 - Vertical
     #                           5 - Target (stationary)
     #                           6 - Manual
     #                           7 - Idle (out of control
'h'  # 29      36   Fixed angle (degrees x 64) (e.g., elevation of PPI; azimuth
     #                of RHI; coplane angle)
'h'  # 30      37   Sweep rate (degrees/seconds x 64)
'h'  # 31      38   Generation date of common format - Year
'h'  # 32      39   Month
'h'  # 33      40   Day
'8s' # 34   41-44   Tape generator facility name (8 character ASCII)
'h'  # 35      45   Deleted of missing data flag (Suggest 100000 octal)
)

def read_uf(filename, fp):
    r = {}
    firstRecord = True
    while True:
        headerBuffer = fp.read(headerStruct.size)
        if not headerBuffer:
            break
        header = headerStruct.unpack(headerBuffer)
        bodyBuffer = fp.read(bodyStruct.size)
        if not bodyBuffer:
            break
        body = bodyStruct.unpack(bodyBuffer)
        dt = datetime.datetime(2000 + body[19], body[20], body[21], body[22], body[23], body[24])
        lat = float(body[12]) + float(body[13]) / 60 + float(body[14]) / 230400
        lon = float(body[15]) + float(body[16]) / 60 + float(body[17]) / 230400
        if firstRecord:
            minTime = dt
            maxTime = dt
            minLat = lat
            maxLat = lat
            minLon = lon
            maxLon = lon
            firstRecord = False
        else:
            if dt < minTime:
                minTime = dt
            if dt > maxTime:
                maxTime = dt
            if lat < minLat:
                minLat = lat
            if lat > maxLat:
                maxLat = lat
            if lon < minLon:
                minLon = lon
            if lon > maxLon:
                maxLon = lon
        fp.read(header[0] - bodyStruct.size + 4)
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
    r = read_uf(filename, open(filename))
    for k, v in r.iteritems():
        print("{0}={1}".format(k, v))
