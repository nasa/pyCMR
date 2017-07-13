'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''

import requests
import urllib
from configparser import ConfigParser

import xml.etree.ElementTree as ET

class GenerateMetadata():
    def __init__(self, configFilePath="configFile.cfg"):
        """

        :param configFilePath: It is the config file containing the credentials to talk to the database (extention .cfg)
        """
        config = ConfigParser()
        config.read(configFilePath)
        self.REST_HOST_URL = config.get("restapi", "REST_HOST_URL")
        self.WR_API_KEY = config.get("restapi", "WR_API_KEY")
        self.DATABASENAME = config.get("restapi", "DATABASENAME")
        self.SCHEMA = config.get("restapi", "SCHEMA")

    def getRestAPIURL(self, tableName):
        """

        :param tableName: Table name from where to fetch the data
        :return: The rest api url to ingest to database
        """

        return self.REST_HOST_URL + "api/v2/" + self.DATABASENAME + "/_table/" + self.SCHEMA + "." + tableName + "?api_key=" + self.WR_API_KEY


    def getDataFromDatabase(self, tableName, **kwargs):
        """

        :param tableName: table name as defined in the database
        :param kwargs: Fields in the database with and operation
        :return:
        """

        filter = "&filter="

        andSign = ''
        orderby=''

        for ele in kwargs.keys():
            orderby=ele
            filter = filter + andSign + "(" + ele + '=' + urllib.quote_plus(kwargs[ele]) + ')'
            andSign = '%20AND%20'

        url = self.getRestAPIURL(tableName=tableName)
        url = url + filter

        allresults=[]

        condition = True
        offset = 0
        while condition:
            data = requests.get(url=url+'&orderby='+orderby+'&offset='+str(offset))
            if not data.ok:
                print(data.content)
                return data.reason
            data = data.json()
            for ele in data['resource']:
                allresults.append(ele)
            try:
                offset = data['meta']['next']
                print(offset)
            except:
                condition = False

        return allresults


    def generateCMRXMLTags(self, top, data):
        for key, value in data.items():
            child = ET.SubElement(top, key)
            child.text = str(value)
        return top

    def addsubTags(self, topTag, listTagsName, data):
        """
        A subtag will be added if it has a value
        :param topTag: a tag that will contain subtags to be added
        :param list: list of tag names that will be added
        :param data: value of the subtag
        :return: tag containing subtags
        """

        for ele in listTagsName:
            if data[ele]: # if the subtag has a value
                tagToAdd = ET.SubElement(topTag, ele)
                tagToAdd.text = data[ele]

        return topTag

    def parseBoolean(self, data):
        """

        :param data: boolean type
        :return: CMR specific notation for a boolean
        """

        if data:
            return 'true'
        return 'false'


if __name__ == "__main__":
    ghrc = GenerateMetadata(configFilePath="/home/marouane/PycharmProjects/cmr/cmr.cfg.example")
    data2=ghrc.getDataFromDatabase(tableName='ds_info',ims_visible='1')
