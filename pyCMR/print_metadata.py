#!/usr/bin/python
from __future__ import print_function
import sys, os, fnmatch
import hashlib
import threading
import Queue

import errno
import tarfile, shutil, tempfile, gzip, mimetypes

def _fileParserPostparseGenerator(patternsToParsers, relpath, files):
    for filename in files:
        for patternToParser in patternsToParsers:
            if fnmatch.fnmatchcase(filename, patternToParser[0]):
                if len(patternToParser) > 2:
                    postParse = patternToParser[2]
                else:
                    postParse = None
                yield os.path.join(relpath, filename), patternToParser[1], postParse
                break

def _findFiles(root, patternsToParsers, subdirs=None):
    stripLen = len(root) + 1 # Used to strip top of path and trailing '/'
    if subdirs:
        for subdir in subdirs:
            for abspath, dirs, files in os.walk(os.path.join(root, subdir)):
                for a, b, c in  _fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                    yield a, b, c

    else:
        for abspath, dirs, files in os.walk(root):
            for a, b, c in _fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                yield a, b, c

def _inspectTarFile(filename, patternsToParsers):
    r = {}
    tf = tarfile.open(filename)
    td = tempfile.mkdtemp()
    tf.extractall(td)
    for pathVal, parserAndArgs, postParseFunction in _findFiles(td, patternsToParsers):
        absFilename = os.path.join(td, pathVal)
        dynamicData = _inspectFile(absFilename, parserAndArgs, patternsToParsers)
        if postParseFunction is not None:
            postParseFunction(pathVal, dynamicData)
        for i in ("start", "SLat", "WLon"):
            if i in dynamicData:
                if i in r:
                    if r[i] > dynamicData[i]:
                        r[i] = dynamicData[i]
                else:
                    r[i] = dynamicData[i]
        for i in ("end", "NLat", "ELon"):
            if i in dynamicData:
                if i in r:
                    if r[i] < dynamicData[i]:
                        r[i] = dynamicData[i]
                else:
                    r[i] = dynamicData[i]
    shutil.rmtree(td)
    tf.close()
    return r

def _threadTarget(filename, parserAndArgs, q, pipeR):
    if mimetypes.guess_type(filename)[1] == 'gzip':
        os.close(pipeR)
        gz = gzip.open(filename, 'rb')
        r  = parserAndArgs[0](filename, gz, *parserAndArgs[1:])
    else:
        with os.fdopen(pipeR, 'rb') as fp:
            r  = parserAndArgs[0](filename, fp, *parserAndArgs[1:])
    q.put(r)

def _inspectFile(filename, parserAndArgs, patternsToParsers):
    if tarfile.is_tarfile(filename) and not parserAndArgs:
        r = _inspectTarFile(filename, patternsToParsers)
    else:
        r = {}
    sha1 = hashlib.sha1()
    if parserAndArgs and parserAndArgs[0]:
        q = Queue.Queue()
        pipeR, pipeW = os.pipe()
        t = threading.Thread(target=_threadTarget, args=(filename, parserAndArgs, q, pipeR))
        t.daemon = True
        t.start()
    else:
        t = None
    pipeError = False
    with open(filename, 'rb') as fp:
        while True:
            buf = fp.read(65536)
            if not buf:
                break
            if t and not pipeError:
                try:
                    os.write(pipeW, buf)
                except OSError as exc:
                    if exc.errno == errno.EPIPE:
                        pipeError = True
                    else:
                        raise
            sha1.update(buf)
    if t:
        os.close(pipeW)
    if t:
        t.join()
        r = q.get()
    r["checksum"] = sha1.hexdigest()
    return r

# The order in which to print the fields on a line.
# If a parse function extracts additional data not mentioned here,
# it will be added in random order at the end of the line.
defaultFieldOrder = [ "host","env","project","ds","inv","file","path","size","start_date","end_date",
                      "browse","checksum","NLat","SLat","WLon","ELon" ]
"""
The default order in which to print the fields on a line
"""

# The format for the start and end fields
defaultDateTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
"""
The default string passed to strftime of the start and end times
and used to format the date string printed in the metadata
"""

def print_metadata(rootDir,
        patternsToParsers,
        staticData={},
        topLevelDirs=None,
        dateTimeFormat=None,
        fieldOrder=None):


    dataToReturn=[]
    try:
        canDoRenaming = True
        if dateTimeFormat == None:
            dateTimeFormat = defaultDateTimeFormat
        if fieldOrder == None:
            fieldOrder = defaultFieldOrder
        # Make all the time parsing and formatting use UTC
        os.environ["TZ"] = "UTC"
        rootDir = os.path.abspath(rootDir)  # Work with absolute paths
        for pathVal, parserAndArgs, postParseFunction in _findFiles(rootDir, patternsToParsers, topLevelDirs):
            # pathVal is the relative path from rootDir
            # parserAndArgs is the parser and arguments for the first pattern that matched
            absFilename = os.path.join(rootDir, pathVal)
            if os.path.islink(absFilename):
                continue
            dynamicData = _inspectFile(absFilename, parserAndArgs, patternsToParsers)
            dynamicData["size"] = os.path.getsize(absFilename)
            dynamicData["path"] = pathVal
            dynamicData["granule_name"] = os.path.basename(pathVal)
            if postParseFunction is not None:
                newPathVal = postParseFunction(pathVal, dynamicData)
                if newPathVal is not None and newPathVal != pathVal:
                    if canDoRenaming:
                        newAbsPathVal = os.path.join(rootDir, newPathVal)
                        newAbsPathDir = os.path.dirname(newAbsPathVal)
                        try:
                            os.makedirs(newAbsPathDir)
                        except OSError as exc:
                            if exc.errno == errno.EEXIST and os.path.isdir(newAbsPathDir):
                                pass
                            elif exc.errno == errno.EACCES:
                                canDoRenaming = False
                            else:
                                raise
                        try:
                            os.symlink(absFilename, newAbsPathVal)
                        except OSError as exc:
                            if exc.errno == errno.EEXIST and os.readlink(newAbsPathVal) == absFilename:
                                pass
                            elif exc.errno == errno.EACCES:
                                canDoRenaming = False
                            else:
                                raise
                    pathVal = newPathVal
                    dynamicData["path"] = pathVal
                    dynamicData["granule_name"] = os.path.basename(pathVal)
            if "start" in dynamicData:
                dynamicData["start_date"] = dynamicData["start"].strftime(dateTimeFormat)
            if "end" in dynamicData:
                dynamicData["end_date"] = dynamicData["end"].strftime(dateTimeFormat)
            # At this point we have staticData and dynamicData
            # Anything not in dynamicData should be filled in using staticData
            for k, v in staticData.iteritems():
                if k not in dynamicData:
                    dynamicData[k] = v
            # We want to print out in order of fieldOrder
            inFirstField = True
            data=''
            for k in fieldOrder:
                if k in dynamicData:
                    if inFirstField:
                        data="{0}={1}".format(k, dynamicData[k])
                        inFirstField = False
                    else:
                        data+=",{0}={1}".format(k, dynamicData[k])
                    dynamicData.pop(k)
            # Print anything else left at end of line
            for k, v in dynamicData.iteritems():
                if inFirstField:
                    data+=",{0}={1}".format(k, v)
                    inFirstField = False
                else:
                    data+=",{0}={1}".format(k, v)

            data=[data]
            dataToReturn.append(data)
        if not canDoRenaming:
            sys.stderr.write("Did not have permission to make symbolic links to do renaming\n")
    except KeyboardInterrupt:
        pass
    return dataToReturn

if __name__ == "__main__":
    if len(sys.argv) > 1:
        print_metadata(sys.argv[1], (("*" , None),))
    else:
        print_metadata(".", (("*.py" , None),))
