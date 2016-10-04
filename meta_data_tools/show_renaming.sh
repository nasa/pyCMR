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
cd "$1"
( find -type l -print0 ) | while read -d $'\0' pathVal ; do
  (
    ls -l "$pathVal" | cut -d ' ' -f 9- | sed "s:^./::g ; s:$PWD/::g ; s: -> : was :g"
  )
done
