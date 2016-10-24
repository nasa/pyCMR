import Queue
import errno
import fnmatch
import gzip
import hashlib
import mimetypes
import os
import shutil
import socket
import tarfile
import tempfile
import threading
from datetime import datetime
import xml.etree.ElementTree as ET

from read_variable_nc import read_variable_nc
import requests
from read_eol_sf import read_eol_sf
from itertools import izip
import sys
# The root of where ds_url refers to.
# The 'path=' metadata for files will be relative to this directory


# The file patterns to look at and the functions to parse them followed by arguments for the parser



class metaDataTool:
    def __init__(self, metaDataURLResources, metaDataAPI):

        self.defaultDateTimeFormat = "%Y-%m-%dT%H:%M:%SZ"
        self.defaultFieldOrder = [ "host","env","project","ds","inv","file","path","size","start","end",
                      "browse","checksum","NLat","SLat","WLon","ELon" ]
        self._META_DATA_URL_RESOURCES=metaDataURLResources
        self._API_META_DATA_RESOURCES=metaDataAPI


    def __fileParserPostparseGenerator(self,patternsToParsers, relpath, files):
        for filename in files:
            for patternToParser in patternsToParsers:
                if fnmatch.fnmatchcase(filename, patternToParser[0]):
                    if len(patternToParser) > 2:
                        postParse = patternToParser[2]
                    else:
                        postParse = None
                    yield os.path.join(relpath, filename), patternToParser[1], postParse
                    break

    def __findFiles(self, root, patternsToParsers, subdirs=None):
        stripLen = len(root) + 1  # Used to strip top of path and trailing '/'
        if subdirs:
            for subdir in subdirs:
                for abspath, dirs, files in os.walk(os.path.join(root, subdir)):
                    for a, b, c in self.__fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                        yield a, b, c

        else:
            for abspath, dirs, files in os.walk(root):
                for a, b, c in self.__fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                    yield a, b, c

    def __threadTarget(self,filename, parserAndArgs, q, pipeR):
        if mimetypes.guess_type(filename)[1] == 'gzip':
            os.close(pipeR)
            gz = gzip.open(filename, 'rb')
            r = parserAndArgs[0](filename, gz, *parserAndArgs[1:])
        else:
            with os.fdopen(pipeR, 'rb') as fp:
                r = parserAndArgs[0](filename, fp, *parserAndArgs[1:])
        q.put(r)

    def __inspectFile(self,filename, parserAndArgs, patternsToParsers):
        if tarfile.is_tarfile(filename) and not parserAndArgs:
            r = self.__inspectTarFile(filename, patternsToParsers)
        else:
            r = {}
        sha1 = hashlib.sha1()
        if parserAndArgs and parserAndArgs[0]:
            q = Queue.Queue()
            pipeR, pipeW = os.pipe()
            t = threading.Thread(target=self.__threadTarget, args=(filename, parserAndArgs, q, pipeR))
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

    def __inspectTarFile(self,filename, patternsToParsers):
        r = {}
        tf = tarfile.open(filename)
        td = tempfile.mkdtemp()
        tf.extractall(td)
        for pathVal, parserAndArgs, postParseFunction in self.__findFiles(td, patternsToParsers):
            absFilename = os.path.join(td, pathVal)
            dynamicData = self.__inspectFile(absFilename, parserAndArgs, patternsToParsers)
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





    def processAvapsData(self, rootDir):
        patternsToParsers = (
            ("*.eol", (read_eol_sf,)),
        )

        # The variables that are the same for every line

        static_data = {}
        static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'
        static_data["env"] = "ops"
        static_data["project"] = "HS3"
        static_data["ds_short_name"] = "hs3avaps2"
        static_data["inv"] = "HS3_INV"
        static_data["browse"] = "N"
        static_data["format"] = "ASCII"

        self.getMetadataList(rootDir, patternsToParsers, static_data)

    def _fileParserPostparseGenerator(self, patternsToParsers, relpath, files):
        for filename in files:
            for patternToParser in patternsToParsers:
                if fnmatch.fnmatchcase(filename, patternToParser[0]):
                    if len(patternToParser) > 2:
                        postParse = patternToParser[2]
                    else:
                        postParse = None
                    yield os.path.join(relpath, filename), patternToParser[1], postParse
                    break


    def _findFiles(self, root, patternsToParsers, subdirs=None):
        stripLen = len(root) + 1  # Used to strip top of path and trailing '/'
        if subdirs:
            for subdir in subdirs:
                for abspath, dirs, files in os.walk(os.path.join(root, subdir)):
                    for a, b, c in self._fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                        yield a, b, c

        else:
            for abspath, dirs, files in os.walk(root):
                for a, b, c in self._fileParserPostparseGenerator(patternsToParsers, abspath[stripLen:], files):
                    yield a, b, c

    def _inspectTarFile(self,filename, patternsToParsers):
        r = {}
        tf = tarfile.open(filename)
        td = tempfile.mkdtemp()
        tf.extractall(td)
        for pathVal, parserAndArgs, postParseFunction in self._findFiles(td, patternsToParsers):
            absFilename = os.path.join(td, pathVal)
            dynamicData = self._inspectFile(absFilename, parserAndArgs, patternsToParsers)
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

    def _threadTarget(self,filename, parserAndArgs, q, pipeR):
        if mimetypes.guess_type(filename)[1] == 'gzip':
            os.close(pipeR)
            gz = gzip.open(filename, 'rb')
            r = parserAndArgs[0](filename, gz, *parserAndArgs[1:])
        else:
            with os.fdopen(pipeR, 'rb') as fp:
                r = parserAndArgs[0](filename, fp, *parserAndArgs[1:])
        q.put(r)


    def _inspectFile(self, filename, parserAndArgs, patternsToParsers):
        if tarfile.is_tarfile(filename) and not parserAndArgs:
            r = self._inspectTarFile(filename, patternsToParsers)
        else:
            r = {}
        sha1 = hashlib.sha1()
        if parserAndArgs and parserAndArgs[0]:
            q = Queue.Queue()
            pipeR, pipeW = os.pipe()
            t = threading.Thread(target=self._threadTarget, args=(filename, parserAndArgs, q, pipeR))
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


    def getMetadataList(self,rootDir,
        patternsToParsers,
        staticData={},
        topLevelDirs=None,
        dateTimeFormat=None,
        fieldOrder=None):

        dataToReturn = []
        try:
            canDoRenaming = True
            if dateTimeFormat == None:
                dateTimeFormat = self.defaultDateTimeFormat
            if fieldOrder == None:
                fieldOrder = self.defaultFieldOrder
            # Make all the time parsing and formatting use UTC
            os.environ["TZ"] = "UTC"
            rootDir = os.path.abspath(rootDir)  # Work with absolute paths
            for pathVal, parserAndArgs, postParseFunction in self._findFiles(rootDir, patternsToParsers, topLevelDirs):
                # pathVal is the relative path from rootDir
                # parserAndArgs is the parser and arguments for the first pattern that matched
                absFilename = os.path.join(rootDir, pathVal)
                if os.path.islink(absFilename):
                    continue
                dynamicData = self._inspectFile(absFilename, parserAndArgs, patternsToParsers)
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
                data = ''
                for k in fieldOrder:
                    if k in dynamicData:
                        if inFirstField:
                            data = "{0}={1}".format(k, dynamicData[k])
                            inFirstField = False
                        else:
                            data += ",{0}={1}".format(k, dynamicData[k])
                        dynamicData.pop(k)
                # Print anything else left at end of line
                for k, v in dynamicData.iteritems():
                    if inFirstField:
                        data += ",{0}={1}".format(k, v)
                        inFirstField = False
                    else:
                        data += ",{0}={1}".format(k, v)

                data = [data]
                dataToReturn.append(data)
            if not canDoRenaming:
                sys.stderr.write("Did not have permission to make symbolic links to do renaming\n")
        except KeyboardInterrupt:
            pass
        return dataToReturn




    def processIphexHiwrapeData(self, rootDir):


        # The root of where ds_url refers to.
        # The 'path=' metadata for files will be relative to this directory


        # The file patterns to look at and the functions to parse them followed by arguments for the parser
        patternsToParsers = (
            ("*.nc", (read_variable_nc, "timed", "lat", "lon")),
            ("*.h5", (read_variable_nc, "timeUTC", "lat", "lon")),
        )

        # The variables that are the same for every line
        static_data = {}
        static_data["host"] = socket.gethostname().split(".")[0]  # Only the part before the first '.'

        static_data["browse"] = "N"


        return self.getMetadataList(rootDir, patternsToParsers, static_data)

    def _getdata(self, data, keyword):
        try:
            return data[keyword]
        except:

            return None






    def fromJsonToXML(self, data, ds_short_name):
        head=ET.Element("Granules")
        today = datetime.now()
        for ele in data:

            # ====Top level tag =====
            top = ET.Element("Granule")
            GranuleUR = ET.SubElement(top, "GranuleUR")
            GranuleUR.text = ele['granule_name']
            InsertTime = ET.SubElement(top, "InsertTime")
            InsertTime.text = today.strftime("%Y-%m-%dT%H:%M:%SZ")
            LastUpdate = ET.SubElement(top, "LastUpdate")
            LastUpdate.text = today.strftime("%Y-%m-%dT%H:%M:%SZ")
            Collection = ET.SubElement(top, "Collection")
            ShortName = ET.SubElement(Collection, "ShortName")
            ShortName.text = ds_short_name
            VersionId=ET.SubElement(Collection, "VersionId")
            VersionId.text = "1"

            # =============DataGranule tag ========================#
            DataGranule = ET.Element("DataGranule")
            SizeMBDataGranule = ET.SubElement(DataGranule, "SizeMBDataGranule")
            SizeMBDataGranule.text = self._getdata(ele, 'size')
            DayNightFlag = ET.SubElement(DataGranule, "DayNightFlag")
            DayNightFlag.text = "UNSPECIFIED"
            ProductionDateTime = ET.SubElement(DataGranule, "ProductionDateTime")
            ProductionDateTime.text = today.strftime("%Y-%m-%dT%H:%M:%SZ")
            if SizeMBDataGranule.text:
                SizeMBDataGranule.text = str(int(SizeMBDataGranule.text) * 10E-6)  # Convert to MiB units
                top.append(DataGranule)

            # =============Temporal tag ========================#
            Temporal = ET.Element("Temporal")
            RangeDateTime = ET.SubElement(Temporal, "RangeDateTime")
            BeginningDateTime = ET.SubElement(RangeDateTime, "BeginningDateTime")
            BeginningDateTime.text = self._getdata(ele, 'start_date')
            EndingDateTime = ET.SubElement(RangeDateTime, "EndingDateTime")
            EndingDateTime.text = self._getdata(ele, 'start_date')
            BeginningDateTime.text = self._getdata(ele, 'start_date')
            if None not in [BeginningDateTime.text, BeginningDateTime.text]:
                top.append(Temporal)


            # =============Spatial tag ========================#


            Spatial = ET.Element("Spatial")
            HorizontalSpatialDomain = ET.SubElement(Spatial, "HorizontalSpatialDomain")
            Geometry = ET.SubElement(HorizontalSpatialDomain, "Geometry")
            BoundingRectangle = ET.SubElement(Geometry, "BoundingRectangle")
            WestBoundingCoordinate = ET.SubElement(BoundingRectangle, "WestBoundingCoordinate")
            WestBoundingCoordinate.text = self._getdata(ele, 'WLon')
            NorthBoundingCoordinate = ET.SubElement(BoundingRectangle, "NorthBoundingCoordinate")
            NorthBoundingCoordinate.text = self._getdata(ele, 'NLat')
            EastBoundingCoordinate = ET.SubElement(BoundingRectangle, "EastBoundingCoordinate")
            EastBoundingCoordinate.text = self._getdata(ele, 'ELon')
            SouthBoundingCoordinate = ET.SubElement(BoundingRectangle, "SouthBoundingCoordinate")
            SouthBoundingCoordinate.text = self._getdata(ele, 'SLat')
            if None not in [SouthBoundingCoordinate.text, EastBoundingCoordinate.text, WestBoundingCoordinate.text,
                        NorthBoundingCoordinate.text]:
                top.append(Spatial)

            # ===================OnlineAccessURLs tag =================#

            #===========================OnlineResources tag ===============#

            OnlineAccessURLs = ET.Element("OnlineAccessURLs")
            OnlineResources=ET.Element("OnlineResources")

            metaDataURLS=self.getMetaDataURLS(ds_short_name=ds_short_name)
            for urlData in metaDataURLS:

                OnlineResource = ET.SubElement(OnlineResources, "OnlineResource")
                URL = ET.SubElement(OnlineResource, "URL")
                URL.text = urlData['url']

                if urlData['description']:
                    Description=ET.SubElement(OnlineResource, "Description")
                    Description.text = urlData['description']
                Type=ET.SubElement(OnlineResource, "Type")
                Type.text = urlData['url_type']
                if urlData['url_type'] == 'OPeNDAP':
                    URL.text += ele['granule_name']
                elif urlData['url_type'] == 'Data Access':
                    OnlineAccessURL = ET.SubElement(OnlineAccessURLs, "OnlineAccessURL")
                    URL = ET.SubElement(OnlineAccessURL, "URL")
                    URL.text = urlData['url'] + ele['granule_name']
                    top.append(OnlineAccessURLs)



            if len(metaDataURLS):
                top.append(OnlineResources)



            # ===================Ordorable tag =================#
            Orderable = ET.SubElement(top, "Orderable")
            Orderable.text = "true"
            head.append(top)

        return ET.tostring(head)

    def ComaSeperatedDataToListJson(self,data):
        lines = []
        listJson = []
        for ele in data:
            ele = ele[0]  # the data is a list of data so I am taking the first elemnt from each
            ele = ele.replace("=", ",")
            lines.append(ele)
        for ele in lines:
            iterator = iter(ele.split(","))
            args = dict(izip(iterator, iterator))
            # post=py.ingestGranule(**args)
            # print post['status'], post['result']
            listJson.append(args)
        return listJson

    def getMetaData(self, rootDir, ds_short_name):
        data=self.processIphexHiwrapeData(rootDir)

        commaSeperatedFile=self.ComaSeperatedDataToListJson(data=data)

        return self.fromJsonToXML(commaSeperatedFile,ds_short_name)


    def getMetaDataURLS(self,ds_short_name):
        urls=[]


        url=self._META_DATA_URL_RESOURCES+ds_short_name+self._API_META_DATA_RESOURCES

        req=requests.get(url)
        req=req.json()

        for ele in req['resource']:
            urls.append({'url': ele['ds_url'],'url_type':ele['ds_url_type'], 'description':ele['ds_url_comments']})

        return urls






