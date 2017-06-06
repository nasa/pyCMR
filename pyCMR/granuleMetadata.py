'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
from .GenerateMetadata import GenerateMetadata
import xml.etree.ElementTree as ET

class GranuleCMRXMLTags(GenerateMetadata):

    def __init__(self, configFilePath="/home/marouane/PycharmProjects/cmr/cmr.cfg.example"):
        GenerateMetadata.__init__(self, configFilePath=configFilePath)

    def getMultipleGranulesXML(self,ds_short_name):
        """

        :param ds_short_name: provide a ds_short name for the data set
        :return: List of data
        """
        data=self.getDataFromDatabase(tableName='CMRGranuleView',ShortName=ds_short_name )
        ds_urls = self.getDataFromDatabase(tableName="ds_urls",
                                           ds_short_name=ds_short_name)  # get ds urls from database

        OnlineResources = ET.Element("OnlineResources")
        OnlineResources = self.getOnlineRessourcesCMRtags(topTag=OnlineResources, ds_urls=ds_urls)

        XMLsGranule=[]

        for ele in data:
            OnlineAccess = self.getAccessURLS(ds_urls=ds_urls, granule_name=ele['GranuleUR'],OnlineResources=OnlineResources)

            XMLsGranule.append(self.generateGranuleXMLToIngest(granule_name=ele['GranuleUR'], accessURLS=OnlineAccess,data=ele))
        return XMLsGranule

    def getOnlineRessourcesCMRtags(self, topTag, ds_urls):
        """

        :param topTag:  <OnlineRessources?
        :param ds_urls:
        :return: online ressources without data_access or opendap data
        """
        data = self.getDataFromDatabase(tableName='ds_url_descriptions')
        for descr in data:
            for url in ds_urls:
                if descr['ds_url_type'] == url['ds_url_type'] and descr['ds_url_type'] not in ['data_access', 'opendap']:
                    OnlineResource = ET.SubElement(topTag, "OnlineResource")
                    URL = ET.SubElement(OnlineResource, "URL")
                    URL.text = url['ds_url']
                    Description = ET.SubElement(OnlineResource, "Description")
                    if url['ds_url_comments']:
                        Description.text = url['ds_url_comments']
                    else:
                        Description.text = descr['description']
                    Type = ET.SubElement(OnlineResource, "Type")
                    Type.text = descr['label']

        return topTag

    def generateGranuleXMLToIngest(self, granule_name,accessURLS=None, data=None):
        """

        :param data: a dictionary of the data you want to ingest to CMR
        :return: XML data according to CMR format
        """
        if None in [data]:
            data = self.getDataFromDatabase(tableName="CMRGranuleView", GranuleUR=granule_name)
            data = data[0]

        if not data:
            print("Error occurred while fetching your data")
            return False

        ds_short_name = data['ShortName']

        # ====Top level tag =====
        Granule = ET.Element("Granule")
        topList = ['GranuleUR', 'InsertTime', 'LastUpdate']

        top=self.addsubTags(topTag=Granule,listTagsName=topList,data=data) #add subtags to Granule

        # =============Collection tag tag ========================#

        Collection = ET.Element("Collection")

        collList=['ShortName','VersionId'] # Tags for collection list

        Collection = self.addsubTags(topTag=Collection,listTagsName=collList,data=data)

        top.append(Collection)

        # =============TDataGranule tag ========================#
        DataGranule= ET.Element("DataGranule")
        DataGranuleList=['SizeMBDataGranule','DayNightFlag','ProductionDateTime']

        DataGranule= self.addsubTags(topTag=DataGranule,listTagsName=DataGranuleList,data=data)

        top.append(DataGranule)

        # =============Temporal tag ========================#
        Temporal = ET.Element("Temporal")
        RangeDateTime = ET.SubElement(Temporal, "RangeDateTime")
        temporalList = ["BeginningDateTime"]

        if data['EndingDateTime']: # if it has stop data
            temporalList.append("EndingDateTime")

        self.addsubTags(topTag=RangeDateTime,listTagsName=temporalList,data=data)

        top.append(Temporal)

        # =============Spatial tag ========================#
        Spatial = ET.Element("Spatial")
        HorizontalSpatialDomain = ET.SubElement(Spatial, "HorizontalSpatialDomain")
        Geometry = ET.SubElement(HorizontalSpatialDomain, "Geometry")
        BoundingRectangle = ET.SubElement(Geometry, "BoundingRectangle")

        geomList = ['WestBoundingCoordinate', 'NorthBoundingCoordinate', 'EastBoundingCoordinate',
                    'SouthBoundingCoordinate']

        self.addsubTags(topTag=BoundingRectangle,listTagsName=geomList,data=data)

        top.append(Spatial)

        # =============Price tag ========================#
        Price= ET.Element("Price")
        Price.text='0.0'
        top.append(Price)

        # ========Access URLs===========================#
        if None in [accessURLS]:
            ds_urls = self.getDataFromDatabase(tableName="ds_urls",
                                               ds_short_name=ds_short_name)  # get ds urls from database

            OnlineResources = ET.Element("OnlineResources")
            OnlineResources = self.getOnlineRessourcesCMRtags(topTag=OnlineResources, ds_urls=ds_urls)
            accessURLS=self.getAccessURLS(granule_name=granule_name, ds_urls=ds_urls, OnlineResources=OnlineResources)

        top.append(accessURLS['OnlineAccessURLs'])
        top.append(accessURLS['OnlineResources'])

        #==============Orderable tag =====================#
        Orderable=ET.SubElement(top, "Orderable")
        Orderable.text=self.parseBoolean(data['Orderable'])

        # ==============DataFormat tag =====================#
        DataFormat = ET.SubElement(top, "DataFormat")
        DataFormat.text = data['DataFormat']
        return ET.tostring(top, encoding='utf-8')

        # =======AssociatedDIFs ================#

    def getAccessURLS(self, ds_urls, granule_name,OnlineResources):
        OnlineAccessURLs = ET.Element("OnlineAccessURLs")
        OnlineAccessURL = ET.SubElement(OnlineAccessURLs, "OnlineAccessURL")
        URL = ET.SubElement(OnlineAccessURL, "URL")

        URL.text = self.geturlType(data=ds_urls, ds_url_type='data_access')+granule_name

        opendapURL = self.geturlType(data=ds_urls, ds_url_type='opendap')

        if opendapURL:
            OnlineResource = ET.SubElement(OnlineResources, "OnlineResource")
            URL = ET.SubElement(OnlineResource, "URL")
            opendapURL = opendapURL.split('contents.html')[0]
            URL.text = opendapURL+granule_name
            Type = ET.SubElement(OnlineResource, "Type")
            Type.text = "OPENDAP DATA"

        return {'OnlineAccessURLs':OnlineAccessURLs, 'OnlineResources':OnlineResources}

    def geturlType(self, data, ds_url_type):
        for ele in data:
            if ele['ds_url_type'] == ds_url_type:
                return ele['ds_url']
        return None

if __name__ == "__main__":
    ghrc = GranuleCMRXMLTags("/home/marouane/PycharmProjects/cmr/cmr.cfg.example")
    #print ghrc.generateGranuleXMLToIngest(granule_name="HS3_CPL_layers_14212b_20140829.txt")
    data=ghrc.getMultipleGranulesXML(ds_short_name='rasipapag')
    #data=ghrc.generateGranuleXMLToIngest(granule_name='RASI_Papagayo_2003.nc')
    print(data)

