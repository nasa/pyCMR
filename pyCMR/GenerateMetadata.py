
import sys
import requests
import urllib
from ConfigParser import ConfigParser

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
        # print url + filter + '&limit=' + str(limit)




        allresults=[]

        condition = True
        offset = 0
        while condition:
            data = requests.get(url=url+'&orderby='+orderby+'&offset='+str(offset))
            if not data.ok:
                print data.content
                return data.reason
            data = data.json()
            for ele in data['resource']:
                allresults.append(ele)


            try:
                offset = data['meta']['next']
                print offset

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
    # print ghrc.ingestCollection("/home/marouane/GHRCCatalgue/collection.csv")
    # print ghrc.deleteFromDatabase(tableName="ds_info", CSVFile="/home/marouane/GHRCCatalgue/delete_ds_info.csv")
    # print ghrc.getDataExample(tableName='ds_urls', ds_url_type='data_access')
    # print ghrc.getData(tableName="ds_info", limit=13)

    data2=ghrc.getDataFromDatabase(tableName='ds_info',ims_visible='1')



    # print ghrc.getData(tableName="ds_url_descriptions")
    # print ghrc.getPlatformsCMRtag(short_name='GOES-8')
    # print ghrc.getRestAPIURL("ds_info")

