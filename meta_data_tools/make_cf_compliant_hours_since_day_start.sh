#!/bin/sh
experimentName="NASA IPHEX May-June 2014"
timeFieldName=timed
( find $1 -type f -name '*.nc' -print0 ) | while read -d $'\0' pathVal ; do
    # basename of the file:
    file=${pathVal##*/}
    baseTime=
    # Try to get baseTime from the filename
    if [ ! "$baseTime" ] ; then
        t=`echo $file | sed 's|.*[^0-9]\([0-9][0-9][0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)[^0-9]\([0-9][0-9]\)\([0-9][0-9]\)\([0-9][0-9]\)[^0-9].*|\1-\2-\3T00:00:00Z|'`
        [ "$t" = "$file" ] || baseTime=$t
    fi
    # Can repeat like the above for more ways to get baseTime
    if [ ! "$baseTime" ] ; then
        echo Could not get a baseTime for file $pathVal 1>&2
        exit 1
    fi
    ncatted -O \
        -a Conventions,global,o,c,CF-1.6 \
        -a units,$timeFieldName,o,c,"hours since $baseTime" \
        -a axis,$timeFieldName,o,c,T \
        -a axis,range,o,c,X \
        -a calendar,$timeFieldName,o,c,standard \
        -a units,tilt,o,c,degrees \
        -a units,head,o,c,degrees \
        -a units,lat,o,c,degrees_north \
        -a units,lon,o,c,degrees_east \
        -a units,year,o,c,year \
        -a experiment,global,o,c,"$experimentName" \
        "$pathVal" -o "$pathVal"
    ncks -4 -L 4 -O "$pathVal" -o "$pathVal"
done
#        -a units,incid,o,c,degrees \
