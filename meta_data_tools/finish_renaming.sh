#!/bin/sh
if [ ! -d "$1" ] ; then
    echo Please Provide Directory
    exit
fi
if [ `find -L "$1" -type l` ] ; then
    echo Cannot do this with bad links:
    find -L "$1" -type l
    exit
fi
echo Are you sure you want to replace the symlinks under $1 with the files they point to \?
echo The files they point to will no longer be at their original location
read y
if [ "$y" != y ] ; then
    echo Running Away...
    exit
fi
( find "$1" -type l -print0 ) | while read -d $'\0' pathVal ; do
  (
    cd `dirname "$pathVal"`
    destination=`basename "$pathVal"`
    sourcefile=`readlink "$destination"`
    mv -f "$sourcefile" "$destination"
  )
done
echo Prune empty directories\?
read y
if [ "$y" == y ] ; then
    find "$1" -empty -delete
fi
