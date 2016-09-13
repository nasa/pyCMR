import json
import logging
import math
import os
import shutil
import xml.etree.ElementTree as ET
try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

import requests

from Result import Collection, Granule
from xmlParser import XmlListConfig, ComaSeperatedToListJson


class CMR(object):
    def __init__(self, configFilePath=''):
        """
        :param configFilePath: The config file containing the credentials to make CRUD requests to CMR (extention .cfg)
        These con
        """
        self.config = ConfigParser()
        if os.path.isfile(configFilePath) and os.access(configFilePath, os.R_OK | os.W_OK):
            # Open the config file as normal
            self.config.read(configFilePath)
            self.configFilePath = configFilePath
        elif not os.path.isfile(configFilePath) and \
            set(['CMR_PROVIDER', 'CMR_USERNAME', 'CMR_PASSWORD', 'CMR_CLIENT_ID']).issubset(set(os.environ.keys())):
            logging.info("Creating new config file, using information in the `CMR_*` environment variables")

            pycmr_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            example_config_path = os.path.join(pycmr_base_dir, 'cmr.cfg.example')

            if not os.path.isfile((example_config_path)):
                with open(os.path.join(pycmr_base_dir, 'cmr.cfg.example'), 'w') as e:
                    e.write(base_cfg)
                    e.close()

            new_config_path = os.path.join(pycmr_base_dir, 'cmr.cfg')
            shutil.copyfile(example_config_path, new_config_path)
            configFilePath = new_config_path

            self.config.read(configFilePath)
            self.configFilePath = configFilePath
            self.config.set('credentials', 'provider', os.environ['CMR_PROVIDER'])
            self.config.set('credentials', 'username', os.environ['CMR_USERNAME'])
            self.config.set('credentials', 'password', os.environ['CMR_PASSWORD'])
            self.config.set('credentials', 'client_id', os.environ['CMR_CLIENT_ID'])
            self.config.write(open(self.configFilePath, 'w'))
            logging.info("Config file created, at {}".format(new_config_path))
        else:
            raise IOError("The config file can't be opened for reading/writing")

        self._SEARCH_GRANULE_URL = self.config.get("request", "search_granule_url")
        self._SEARCH_COLLECTION_URL = self.config.get("request", "search_collection_url")

        self._INGEST_URL = self.config.get("request", "ingest_url")
        self._REQUEST_TOKEN_URL = self.config.get("request", "request_token_url")

        self._PROVIDER = self.config.get("credentials", "provider")
        self._USERNAME = self.config.get("credentials", "username")
        self._PASSWORD = self.config.get("credentials", "password")
        self._CLIENT_ID = self.config.get("credentials", "client_id")

        self._ECHO_TOKEN = self.config.get("credentials", "echo_token")
        self._createSession()
        if not self.config.get('credentials', 'ECHO_TOKEN'):
            self._generateNewToken()

        self._CONTENT_TYPE = self.config.get("request", "content_type")
        self._INGEST_HEADER = {'Content-type': self._CONTENT_TYPE}
        self._SEARCH_HEADER = {'Accept': self._CONTENT_TYPE}
        self._CMR_HOST = self.config.get("request", "cmr_host")

    def searchGranule(self, page_size=50, limit=100, **kwargs):
        """
        Search the CMR granules
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        logging.info("======== Waiting for response ========")

        metaResult = [
            self.session.get(
                self._SEARCH_GRANULE_URL,
                params=kwargs.update({'page_size': page_size, 'page_num': page_num}),
                headers=self._SEARCH_HEADER
            ).content
            for page_num in xrange(1, int(math.ceil((limit - 1.0) / page_size)) + 2)
        ]

        root = ET.XML(metaResult[0])
        if root.tag == "results":
            metaResult = [
                ref for res in metaResult
                for ref in XmlListConfig(ET.XML(res))[2:]
            ]
            return [Granule(m) for m in metaResult][:limit]
        else:
            # Errors have the top-level tag of `<errors>`
            raise ValueError(metaResult[0])

    def searchCollection(self, page_size=50, limit=100, **kwargs):
        """
        Search the CMR collections
        :param limit: limit of the number of results
        :param kwargs: search parameters
        :return: list of results (<Instance of Result>)
        """
        logging.info("======== Waiting for response ========")

        metaResult = [
            self.session.get(
                self._SEARCH_COLLECTION_URL,
                params=kwargs.update({'page_size': page_size, 'page_num': page_num}),
                headers=self._SEARCH_HEADER
            ).content
            for page_num in xrange(1, int(math.ceil((limit - 1.0) / page_size)) + 2)
        ]

        root = ET.XML(metaResult[0])
        if root.tag == "results":
            metaResult = [
                ref for res in metaResult
                for ref in XmlListConfig(ET.XML(res))[2:]
            ]
            return [Collection(m, self._CMR_HOST) for m in metaResult][:limit]
        else:
            # Errors have the top-level tag of `<errors>`
            raise ValueError(metaResult[0])

    def isTokenExpired(self):
        """
        purpose: check if the token has been expired
        :return: True if the token has been expired; False otherwise.
        """
        url = self._INGEST_URL + self._PROVIDER + "/collections/PYCMR_TEST"
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
            raise KeyError("Could not find <DataSetId> tag")

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
            raise KeyError("Could not find <ShortName> tag")

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
            raise KeyError("Could not find <GranuleUR> tag")

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
        removeCollection = self.session.delete(url)
        return removeCollection.content

    def __ingestGranuleData(self, data,granule_ur):
        validateGranuleRequest = self._validateGranule(data=data,
                                                       granule_ur=granule_ur)
        url = self._INGEST_URL + self._PROVIDER + "/granules/" + granule_ur

        if validateGranuleRequest.ok:
            if self.isTokenExpired():
                self._generateNewToken()
            putGranule = self.session.put(url=url, data=data, headers=self._INGEST_HEADER)

            return putGranule.content

        else:
            raise ValueError("Collection failed to validate:\n{}".format(validateGranuleRequest.content))

    def ingestGranule(self, pathToXMLFile):
        """
        :purpose : ingest granules using cmr rest api
        :param pathToXMLFile:
        :return: the ingest granules request if it is successfully validated
        """
        granule_ur = self._getGranuleUR(pathToXMLFile=pathToXMLFile)
        data = self._getXMLData(pathToXMLFile=pathToXMLFile)
        response = self.__ingestGranuleData(data=data, granule_ur=granule_ur)
        return response

    def fromJsonToXML(self, data):
        top = ET.Element("Granule")

        GranuleUR = ET.SubElement(top, "GranuleUR")
        GranuleUR.text = data['granule_name']
        InsertTime = ET.SubElement(top, "InsertTime")
        InsertTime.text = data['start_date']
        LastUpdate = ET.SubElement(top, "LastUpdate")
        LastUpdate.text = data['start_date']
        Collection = ET.SubElement(top, "Collection")
        DataSetId = ET.SubElement(Collection, "DataSetId")
        DataSetId.text = data['ds_short_name']
        Orderable = ET.SubElement(top, "Orderable")
        Orderable.text = "true"

        return ET.tostring(top)

    def ingestGranuleTextFile(self, pathToTextFile):
        """
        :purpose : ingest granules using cmr rest api
        :param pathToTextFile: a comma seperated values text file

        :return: logs of the requests and the overall successful ingestions
        """
        listargs = ComaSeperatedToListJson(pathToFile=pathToTextFile) # convert comma seperated text file into list of json data
        returnList = []
        errorCount = 0

        for ele in listargs: # for each element in list of json data
            xmldata = self.fromJsonToXML(ele) # convert from json to xml
            data = self.__ingestGranuleData(data=xmldata, granule_ur=ele['granule_name']) # ingest each granule
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
        response = self.session.post(url=url, data=data, headers=self._INGEST_HEADER)
        return response

    def _validateGranule(self, data, granule_ur):
        url = self._INGEST_URL + self._PROVIDER + "/validate/granule/" + granule_ur
        response = self.session.post(url, data=data, headers=self._INGEST_HEADER)
        return response

    def _getEchoToken(self):
        """
        purpose : Requesting a new token
        :return: the new token
        """
        top = ET.Element("token")
        username = ET.SubElement(top,"username")
        username.text = self._USERNAME
        psw = ET.SubElement(top,"password")
        psw.text = self._PASSWORD
        client_id = ET.SubElement(top,"client_id")
        client_id.text = self._CLIENT_ID
        user_ip_address = ET.SubElement(top,"user_ip_address")
        user_ip_address.text = self._getIPAddress()
        provider = ET.SubElement(top,"provider")
        provider.text = self._PROVIDER

        data = ET.tostring(top)
        logging.info("Requesting and setting up a new token... Please wait...")
        response = requests.post(url=self._REQUEST_TOKEN_URL, data=data, headers={'Content-Type': 'application/xml'})
        return response.text.split('<id>')[1].split('</id>')[0]

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
        removeGranule = self.session.delete(url)

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
        ''' Read all text from the XML file '''
        with open(pathToXMLFile, 'r') as xml_file:
            data = xml_file.read()
            return data

    def _generateNewToken(self):
        """
        replacing the expired token by a new one in the config file
        :return:
        """
        logging.info("Replacing the Echo Token")
        theNewToken = self._getEchoToken()
        self.config.set('credentials', 'ECHO_TOKEN',theNewToken)
        self.config.write(open(self.configFilePath, 'w'))
        self._ECHO_TOKEN = theNewToken
        self.session.headers.update({'Echo-Token': self._ECHO_TOKEN})

    def _createSession(self):
        ''' Create a new request session for the CMR object '''
        self.session = requests.Session()
        self.session.headers.update({
            'Client-Id': self._CLIENT_ID,
            'Echo-Token': self._ECHO_TOKEN
        })

base_cfg = """[credentials]
provider =
username =
password =
client_id =
echo_token =

[request]
request_token_url = https://api-test.echo.nasa.gov/echo-rest/tokens/
content_type = application/echo10+xml
cmr_host = cmr.uat.earthdata.nasa.gov

ingest_url = https://%(cmr_host)s/ingest/providers/

search_granule_url = https://%(cmr_host)s/search/granules
search_collection_url = https://%(cmr_host)s/search/collections"""
