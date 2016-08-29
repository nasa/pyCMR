import xml.etree.ElementTree as ET
import json
from Result import Collection, Granule
from xmlParser import XmlListConfig, ComaSeperatedToListJson
import os
import re
import requests


try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser


class CMR():
    def __init__(self, configFilePath):
        """

        :param configFilePath: It is the config file containing the credentials to make CRUD requests to CMR (extention .cfg)
               Please make sure that this process can read and write to the file (changing the Token key)
        """

        if not os.access(configFilePath,
                         os.R_OK | os.W_OK):  # check if the config file has the reading and writing permissions set
            raise IOError("[CONFIGFILE ERROR] the config file can't be opened for reading/writing")

        self.config = ConfigParser()
        self.config.read(configFilePath)
        self.configFilePath = configFilePath

        self.granuleUrl = self.config.get("search", "GRANULE_URL")
        self._granuleMetaUrl = self.config.get("search", "granule_meta_url")
        self._collectionUrl = self.config.get("search", "collection_url")
        self._collectionMetaUrl = self.config.get("search", "collection_meta_url")

        self._INGEST_URL = self.config.get("ingest", "ingest_url")  # Base URL for ingesting to CMR


        self._CONTENT_TYPE = self.config.get("ingest", "content_type")

        self._PROVIDER = self.config.get("credentials", "provider")
        self._USERNAME = self.config.get("credentials", "username")
        self._PASSWORD = self.config.get("credentials", "password")
        self._CLIENT_ID = self.config.get("credentials", "client_id")


        self._createSession()
        self._REQUEST_TOKEN_URL=self.config.get("request", "request_token_url")
        self._generateNewToken()
        self._INGEST_HEADER = {'Content-type': self._CONTENT_TYPE}


    def _searchResult(self, url, limit, **kwargs):
        """
        Search using the CMR apis
        :param url:
        :param limit:
        :param args:
        :param kwargs:
        :return: generator of xml strings
        """
        # Format the url with customized parameters
        for k, v in kwargs.items():
            url += "&{}={}".format(k, v)
        result = [self.session.get(url.format(pagenum)).content
                  for pagenum in xrange(1, (limit - 1) / 50 + 2)]
        # for res in result:
        #     for ref in re.findall("<reference>(.*?)</reference>", res):
        #         yield ref
        return [ref for res in result
                for ref in re.findall("<reference>(.*?)</reference>", res)]

    def searchGranule(self, limit=100, **kwargs):
        """
        Search the CMR granules
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        print ("======== Waiting for response ========")
        metaUrl = self._granuleMetaUrl
        for k, v in kwargs.items():
            metaUrl += "&{}={}".format(k, v)

        metaResult = [self.session.get(metaUrl.format(pagenum)).content
                      for pagenum in xrange(1, (limit - 1) / 50 + 2)]

        # The first can be the error msgs
        root = ET.XML(metaResult[0])
        if root.tag == "errors":
            raise ValueError(" |- Error: " + str([ch.text for ch in root._children]))

        metaResult = [ref for res in metaResult
                      for ref in XmlListConfig(ET.XML(res))[2:]]

        return [Granule(m) for m in metaResult][:limit]

    def searchCollection(self, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        metaUrl = self._collectionMetaUrl
        for k, v in kwargs.items():
            metaUrl += "&{}={}".format(k, v)

        print ("======== Waiting for response ========")
        print('Request URL: {}'.format(metaUrl))
        print('Request headers:')
        print(self.session.headers)
        metaResult = [self.session.get(metaUrl.format(pagenum))
                      for pagenum in xrange(1, (limit - 1) / 50 + 2)]

        try:
            metaResult = [ref for res in metaResult
                          for ref in json.loads(res.content)['feed']['entry']]
        except KeyError:
            raise KeyError(str((json.loads(metaResult[0].content))["errors"]))
        except ValueError:
            raise ValueError(
                "Server responded with status code {}".format(metaResult[0].status_code) +
                "At least one of the results cannot be parsed; here's the first:\n{}".format(metaResult[0].content)
            )
        locationResult = self._searchResult(self._collectionUrl, limit=limit, **kwargs)

        return [Collection(m, l) for m, l in zip(metaResult, locationResult)][:limit]

    def isTokenExpired(self):
        """
        purpose: check if the token has been expired
        :return: True if the token has been expired; False otherwise.
        """
        url = self._INGEST_URL + self._PROVIDER + "/collections/LarcDatasetId"
        putGranule = requests.put(url=url, headers=self.session.headers)
        list_ = ['Token', 'expired', 'exists']  # if the token expired or does not exists
        if (len(putGranule.text.split('<error>')) > 1):  # if there is an error in the request
            if any(word in putGranule.text for word in list_):
                return True

        return False

    def _getDataSetId(self, pathToXMLFile):
        """
        Purpose : a private function to parse the xml file and returns the dataset ID
        :param pathToXMLFile:
        :return:  the dataset id
        """
        tree = ET.parse(pathToXMLFile)
        try:
            return tree.find("DataSetId").text
        except:
            return("Could not find <DataSetId> tag")

    def _getShortName(self, pathToXMLFile):
        """
            Purpose : a private function to parse the xml file and returns the datasetShortName
            :param pathToXMLFile:
            :return:  the datasetShortName
            """
        tree = ET.parse(pathToXMLFile)
        try:
            return tree.find("Collection").find("ShortName").text
        except:
            return("Could not find <ShortName> tag")

    def _getGranuleUR(self, pathToXMLFile):
        """
            Purpose : a private function to parse the xml file and returns the datasetShortName
            :param pathToXMLFile:
            :return:  the datasetShortName
            """
        tree = ET.parse(pathToXMLFile)
        try:
            return tree.find("GranuleUR").text
        except:
            return ("Could not find <GranuleUR> tag")

    def ingestCollection(self, pathToXMLFile):
        """
        :purpose : ingest the collections using cmr rest api
        :param pathToXMLFile:
        :return: the ingest collection request if it is successfully validated
        """

        data = self._getXMLData(pathToXMLFile=pathToXMLFile)
        dataset_id = self._getDataSetId(pathToXMLFile=pathToXMLFile)
        url = self._INGEST_URL + self._PROVIDER + "/collections/" + dataset_id
        validationRequest = self._validateCollection(data=data, dataset_id=dataset_id)
        if validationRequest.ok:  # if the collection is valid
            if self.isTokenExpired():  # check if the token has been expired
                self._generateNewToken()
            putCollection = self.session.put(url=url, data=data, headers=self._INGEST_HEADER)  # ingest granules

            if not putCollection.ok:
                # Failing a PUT causes the CMR to lock session requests for the next minute or so
                # Creating a new session will avoid that
                self._createSession()

            return putCollection.content

        else:
            raise ValueError("Collection failed to validate:\n{}".format(validationRequest.content))


    def updateCollection(self, pathToXMLFile):
        return self.ingestCollection(pathToXMLFile=pathToXMLFile)

    def deleteCollection(self, dataset_id):
        """
        Delete an existing colection
        :param dataset_id: the collection id
        :return: response content of the deletion request
        """

        if self.isTokenExpired():  # check if the token has been expired
            self._generateNewToken()
        url = self._INGEST_URL + self._PROVIDER + "/collections/" + dataset_id
        removeCollection=self.session.delete(url, headers=self._INGEST_HEADER)
        return removeCollection.content







    def __ingestGranuleData(self, data,granule_ur):
        validateGranuleRequest = self._validateGranule(data=data,
                                                       granule_ur=granule_ur)
        url = self._INGEST_URL + self._PROVIDER + "/granules/" + granule_ur

        if validateGranuleRequest.ok:
            if self.isTokenExpired():
                self._generateNewToken()
            putGranule = self.session.put(url=url, data=data, headers=self._INGEST_HEADER)

            if not putGranule.ok:
                # Failing a PUT causes the CMR to lock session requests for the next minute or so
                # Creating a new session will avoid that
                self._createSession()

            return {"log": putGranule.content, "status": putGranule.status_code}

        else:
            raise ValueError("Collection failed to validate:\n{}".format(validateGranuleRequest.content))


    def ingestGranule(self, pathToXMLFile):
        """
        :purpose : ingest granules using cmr rest api
        :param pathToXMLFile:
        :return: the ingest granules request if it is successfully validated
        """

        granule_ur= self._getGranuleUR(pathToXMLFile=pathToXMLFile)

        data = self._getXMLData(pathToXMLFile=pathToXMLFile)
        return self.__ingestGranuleData(data=data, granule_ur=granule_ur)






    def fromJsonToXML(self, data):

        top = ET.Element("Granule")
        GranuleUR = ET.SubElement(top, "GranuleUR")
        GranuleUR.text = data['granule_name']
        InsertTime = ET.SubElement(top, "InsertTime")
        InsertTime.text = data['start_date']
        LastUpdate = ET.SubElement(top, "LastUpdate")
        LastUpdate.text = data['start_date']
        Collection = ET.SubElement(top, "Collection")
        DataSetId=ET.SubElement(Collection, "DataSetId")
        DataSetId.text = data['ds_short_name']
        Orderable=ET.SubElement(top, "Orderable")
        Orderable.text="true"
        return ET.tostring(top)




    def ingestGranuleTextFile(self, pathToTextFile="/home/marouane/PycharmProjects/cmr-master/dataexample.txt"):
        """
        :purpose : ingest granules using cmr rest api
        :param pathToTextFile: a comma seperated values text file
       
        :return: logs of the requests and the overall successful ingestions
        """
        listargs = ComaSeperatedToListJson(pathToFile=pathToTextFile) # convert comma seperated text file into list of json data
        returnList = []
        args = {}
        errorCount = 0

        for ele in listargs: # for each element in list of json data
            xmldata= self.fromJsonToXML(ele) # convert from json to xml
            data=self.__ingestGranuleData(data=xmldata, granule_ur=ele['granule_name']) # ingest each granule
            returnList.append(data)
            if (data['status'] >= 400): # if there is an error during the ingestion 
                errorCount += 1 # increment the counter
            returnList.append(data['log'])
           
        return {'logs': returnList,
                'result': str(len(listargs) - errorCount) + " successful ingestion out of " + str(len(listargs))}



    def _validateCollection(self, data, dataset_id):
        """
        :purpose : To validate the colection before the actual ingest
        :param data: the collection data
        :param dataset_id:
        :return: the request to validate the ingest of the collection
        """

        url = self._INGEST_URL + self._PROVIDER + "/validate/collection/" + dataset_id

        return self.session.post(url=url, data=data, headers=self._INGEST_HEADER)

    def _validateGranule(self, data, granule_ur):
        url = self._INGEST_URL + self._PROVIDER + "/validate/granule/" + granule_ur
        req = self.session.post(url, data=data, headers=self._INGEST_HEADER)
        return req

    def _getEchoToken(self):
        """
        purpose : Requesting a new token
        :return: the new token
        """
        top=ET.Element("token")
        username=ET.SubElement(top,"username")
        username.text=self._USERNAME
        psw=ET.SubElement(top,"password")
        psw.text=self._PASSWORD
        client_id=ET.SubElement(top,"client_id")
        client_id.text=self._CLIENT_ID
        user_ip_address=ET.SubElement(top,"user_ip_address")
        user_ip_address.text=self._getIPAddress()
        provider=ET.SubElement(top,"provider")
        provider.text=self._PROVIDER

        data = ET.tostring(top)
        print("Requesting and setting up a new token... Please wait...")
        req = requests.post(url=self._REQUEST_TOKEN_URL, data=data, headers={'Content-Type': 'application/xml'})
        return req.text.split('<id>')[1].split('</id>')[0]


    def updateGranule(self, pathToXMLFile):
        return self.ingestGranule(pathToXMLFile=pathToXMLFile)



    def deleteGranule(self, granule_ur):
        """

        :param granule_ur: The granule name
        :return: the content of the deletion request
        """
        if self.isTokenExpired():
            self._generateNewToken()

        url = self._INGEST_URL + self._PROVIDER + "/granules/" + granule_ur
        removeGranule=self.session.delete(url=url, headers=self._INGEST_HEADER)

        return removeGranule.content





    def _getIPAddress(self):
        """
        Get the public IP address of the machine running the program
        (used to request ECHO token)
        :return: machine's public IP
        """
        response = requests.get('http://httpbin.org/ip')
        ip_address = json.loads(response.text)['origin']
        return ip_address

    def _getXMLData(self, pathToXMLFile):
        data = open(pathToXMLFile).read()  # read the XML file
        return data

    def _generateNewToken(self):
        """
        replacing the expired token by a new one in the config file
        :return:
        """
        print("Replacing the Echo Tocken ... ")
        theNewToken= self._getEchoToken()
        self.config.set('ingest', 'ECHO_TOKEN',theNewToken)
        self.config.write(open(self.configFilePath, 'w'))
        self._ECHO_TOKEN = theNewToken
        self.session.headers.update({'Echo-Token': self._ECHO_TOKEN})

    def _createSession(self):
        ''' Create a new request session for the CMR object '''
        self.session = requests.Session()
        self.session.headers.update({'Client-Id': self._CLIENT_ID})


# if __name__=="__main__":
    # cmr=CMR("cmr.cfg")
    #print cmr.ingestCollection("/home/marouane/PycharmProjects/cmr-master/test-collection.xml")
    #print cmr.ingestGranuleTextFile("PathToTxtFile.txt)

    #print cmr.searchGranule(GranuleUR="wwllnrt_20151106_daily_v1_lit.raw")

    #print cmr.searchGranule(granule_ur="AMSR_2_L2_RainOcean_R00_201508190926_A.he5")
    #print cmr.searchCollection(short_name="A2_RainOcn_NRT")
    #print cmr.getGranuleUR("granuleupdated.xml")
    #print cmr.searchGranule(granule_ur="UR_1.he13")
    #print cmr.searchGranule(granule_ur="UR_1.he15")
    #print cmr.ingestCollection("collection.xml")
    #print cmr.ingestGranule("granule.xml")
    #print cmr.ingestGranule("granuleupdated.xml")
    #print cmr.ingestGranule("/home/marouane/cmr/test-granule.xml")
    #print cmr.deleteGranule("UR_1.he11")
    #print cmr.deleteCollection("NRT AMSR2 L2B GLOBAL SWATH GSFC PROFILING ALGORITHM 2010: SURFACE PRECIPITATION, WIND SPEED OVER OCEAN, WATER VAPOR OVER OCEAN AND CLOUD LIQUID WATER OVER OCEAN V0")
