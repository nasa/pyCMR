'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
import sys

from .GenerateMetadata import GenerateMetadata

import xml.etree.ElementTree as ET

class CollectionCMRXMLTags(GenerateMetadata):

    def __init__(self, configFilePath="/home/marouane/PycharmProjects/cmr/cmr.cfg.example"):
        GenerateMetadata.__init__(self, configFilePath=configFilePath)


    def getCampaignCMRTags(self, topTag, ds_short_name):
        """

        :param top: Campaigns
        :param ds_short_name:
        :return:
        """

        data = self.getDataFromDatabase(tableName="ds_project", ds_short_name=ds_short_name)
        for ele in data:
            Campaign = ET.SubElement(topTag, "Campaign")
            ShortName = ET.SubElement(Campaign, "ShortName")
            ShortName.text = ele['project_short_name']
        return topTag


    def discardNoneValues(self, parentTag,subTagName, tagValue):
        """
        This function is to remove the non value from the tag
        :param parentTag: Top level tag
        :param subTagName: subtags ot tag children
        :param tagValue: the value of the tag
        :return:
        """
        if tagValue:
            generatedTag = ET.SubElement(parentTag, subTagName)
            generatedTag.text = tagValue


    def getScienceKeywordsTags(self, toptag, ds_short_name):

        data = self.getDataFromDatabase(tableName="science_keyword", ds_short_name=ds_short_name)

        for ele in data:
            ScienceKeyword = ET.SubElement(toptag, "ScienceKeyword")
            CategoryKeyword = ET.SubElement(ScienceKeyword, "CategoryKeyword")
            CategoryKeyword.text = 'EARTH SCIENCE'

            self.discardNoneValues(ScienceKeyword,'TopicKeyword',ele['topic'])
            self.discardNoneValues(ScienceKeyword, 'TermKeyword', ele['term'])

            VariableLevel1Keyword = ET.SubElement(ScienceKeyword, "VariableLevel1Keyword")

            self.discardNoneValues(VariableLevel1Keyword, 'Value', ele['var_level_1'])

            if ele['var_level_2']:
                VariableLevel2Keyword = ET.SubElement(ScienceKeyword, "VariableLevel2Keyword")
                value = ET.SubElement(VariableLevel2Keyword, "Value")
                value.text = ele['var_level_2']

            if ele['var_level_3']:
                VariableLevel3Keyword = ET.SubElement(ScienceKeyword, "VariableLevel3Keyword")
                value = ET.SubElement(VariableLevel3Keyword, "Value")
                value.text = ele['var_level_3']

        return toptag


    def getPlatformInstrumentCMRtag(self, topTag, ds_short_name):
        """
        Get the tags for instruments to ingest to CMR
        :param topTag: <Platforms>
        :param ds_short_name: dataset short_name
        :return:
        """

        data = self.getDataFromDatabase(tableName="ds_instrument", ds_short_name=ds_short_name)
        platforms_inst=[]
        for ele in data:
            platforms_inst.append(ele['platform_short_name'])



        for ele in set(platforms_inst):
            platform_short_name = ele
            Platform = self.getPlatformsCMRtag(topTag=topTag, short_name=platform_short_name)
            data = self.getDataFromDatabase(tableName="ds_instrument", ds_short_name=ds_short_name, platform_short_name=platform_short_name)
            Instruments = ET.SubElement(Platform, "Instruments")
            for ins in data:


                Instrument = ET.SubElement(Instruments, "Instrument")

                ShortName = ET.SubElement(Instrument, "ShortName")
                ShortName.text = ins['short_name']
                LongName = ET.SubElement(Instrument, "LongName")
                if ins['long_name']:
                    LongName.text = ins['long_name']
                else:
                    LongName.text = ins['short_name']

        return topTag


    def getPlatformsCMRtag(self, topTag, short_name):

        data = self.getDataFromDatabase(tableName="platform", short_name=short_name)
        data = data[0]

        Platform = ET.SubElement(topTag, "Platform")
        ShortName = ET.SubElement(Platform, "ShortName")
        ShortName.text = data['short_name']

        LongName = ET.SubElement(Platform, "LongName")

        if data['long_name']:
            LongName.text = data['long_name']
        else:
            LongName.text = data['short_name']

        Type = ET.SubElement(Platform, "Type")
        Type.text = data['type']

        return Platform


    def getOnlineRessourcesCMRtags(self, topTag, ds_urls):
        """

        :param topTag:  <OnlineRessources?
        :param ds_urls:
        :return:
        """
        data = self.getDataFromDatabase(tableName='ds_url_descriptions')
        for descr in data:
            for url in ds_urls:
                if descr['ds_url_type'] == url['ds_url_type']:
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


    def getCommunData(self, data, listToCompare):
        """
        To get common data with commun keys
        :param data:
        :param listToCompare:
        :return:
        """
        keys = list(set(listToCompare).intersection(data))
        return {k: data[k] for k in keys}


    def generateCollectionXMLToIngest(self, ds_short_name="hs3cpl"):
        """

        :param data: a dictionary of the data you want to ingest to CMR
        :return: XML data according to CMR format
        """

        data = self.getDataFromDatabase(tableName="CMRCollectionView", ShortName=ds_short_name)

        if not data:
            print("Error occurred while ingesting this dataset; Please check if the dataset exists and if you have the right to ingest to CMR")
            return False

        data = data[0]
        if not data['Visible']:
            return "The data is flagged as not Visible"

        topList = ['ShortName','VersionId', 'InsertTime', 'LastUpdate', 'LongName', 'DataSetId', 'Description',
                   'Orderable', 'Visible', 'ProcessingLevelId']

        # ====Top level tag =====
        top = ET.Element("Collection")

        for ele in topList:
            tagToAdd=ET.SubElement(top, ele)
            if type(data[ele])==bool:
                data[ele] = self.parseBoolean(data[ele])

            tagToAdd.text =data[ele]

        ArchiveCenter=ET.SubElement(top, "ArchiveCenter")
        ArchiveCenter.text="NASA/MSFC/GHRC"

        Price=ET.SubElement(top, "Price")
        Price.text='0.0'

        # =============Spatial Keywords tag ========================#


        SpatialKeywords = ET.Element("SpatialKeywords")
        Keyword = ET.SubElement(SpatialKeywords, "Keyword")
        Keyword.text = data['SpatialKeywords']

        top.append(SpatialKeywords)

        # =============TemporalKeywords Keywords tag ========================#
        TemporalKeywords = ET.Element("TemporalKeywords")
        Keyword = ET.SubElement(TemporalKeywords, "Keyword")
        Keyword.text = data['TemporalKeywords']
        top.append(TemporalKeywords)

        # =============Temporal tag ========================#

        temporalList = ["BeginningDateTime"]
        if data["EndingDateTime"]:
            temporalList.append("EndingDateTime")
        Temporal = ET.Element("Temporal")
        RangeDateTime = ET.SubElement(Temporal, "RangeDateTime")

        for ele in temporalList:
            newTag=ET.SubElement(RangeDateTime, ele)
            newTag.text=data[ele]

        top.append(Temporal)

        # ====Contact ==========#
        Contacts = ET.Element("Contacts")
        Contact = ET.SubElement(Contacts, "Contact")
        Role = ET.SubElement(Contact, "Role")
        Role.text = "GHRC USER SERVICES"
        OrganizationAddresses = ET.SubElement(Contact, "OrganizationAddresses")
        Address = ET.SubElement(OrganizationAddresses, "Address")
        StreetAddress = ET.SubElement(Address, "StreetAddress")
        StreetAddress.text = "320 Sparkman Drive"
        City = ET.SubElement(Address, "City")
        City.text = "Huntsville"
        StateProvince = ET.SubElement(Address, "StateProvince")
        StateProvince.text = "Alabama"
        PostalCode = ET.SubElement(Address, "PostalCode")
        PostalCode.text = "35805"
        Country = ET.SubElement(Address, "Country")
        Country.text = "USA"
        OrganizationPhones = ET.SubElement(Contact, "OrganizationPhones")
        Phone = ET.SubElement(OrganizationPhones, "Phone")
        Number = ET.SubElement(Phone, "Number")
        Number.text = "+1 256-961-7932"
        Type = ET.SubElement(Phone, "Type")
        Type.text = "Telephone"
        Phone = ET.SubElement(OrganizationPhones, "Phone")
        Number = ET.SubElement(Phone, "Number")
        Number.text = "+1 256-824-5149"
        Type = ET.SubElement(Phone, "Type")
        Type.text = "Fax"
        OrganizationEmails = ET.SubElement(Contact, "OrganizationEmails")
        Email = ET.SubElement(OrganizationEmails, "Email")
        Email.text = "support-ghrc@earthdata.nasa.gov"

        top.append(Contacts)

        # =============ScienceKeyword tag ========================#

        ScienceKeywords = ET.Element("ScienceKeywords")
        ScienceKeywords = self.getScienceKeywordsTags(toptag=ScienceKeywords, ds_short_name=ds_short_name)

        top.append(ScienceKeywords)

        Platforms = ET.Element("Platforms")
        Platforms = self.getPlatformInstrumentCMRtag(topTag=Platforms, ds_short_name=ds_short_name)
        top.append(Platforms)

        # =====================Additional Attributes===========#

        ds_urls = self.getDataFromDatabase(tableName="ds_urls", ds_short_name=ds_short_name)
        doi = self.geturlType(data=ds_urls, ds_url_type='doi')

        doiAuthority = "http://dx.doi.org/"
        doi = doi.split(doiAuthority)

        AdditionalAttributes = ET.Element("AdditionalAttributes")
        AdditionalAttribute = ET.SubElement(AdditionalAttributes, "AdditionalAttribute")
        Name = ET.SubElement(AdditionalAttribute, "Name")
        Name.text = 'identifier_product_doi'
        DataType = ET.SubElement(AdditionalAttribute, "DataType")
        DataType.text = 'STRING'
        Description = ET.SubElement(AdditionalAttribute, "Description")
        Description.text = 'product DOI'
        Value = ET.SubElement(AdditionalAttribute, "Value")
        Value.text = doi[1]

        AdditionalAttribute = ET.SubElement(AdditionalAttributes, "AdditionalAttribute")
        Name = ET.SubElement(AdditionalAttribute, "Name")
        Name.text = 'identifier_product_doi_authority'
        DataType = ET.SubElement(AdditionalAttribute, "DataType")
        DataType.text = 'STRING'
        Description = ET.SubElement(AdditionalAttribute, "Description")
        Description.text = 'DOI authority'
        Value = ET.SubElement(AdditionalAttribute, "Value")
        Value.text = doiAuthority

        top.append(AdditionalAttributes)

        # =========Campaigns===============#

        Campaigns = ET.Element("Campaigns")

        Campaigns = self.getCampaignCMRTags(topTag=Campaigns, ds_short_name=ds_short_name)

        top.append(Campaigns)

        # ========Access URLs===========================#

        OnlineAccessURLs = ET.Element("OnlineAccessURLs")
        OnlineAccessURL = ET.SubElement(OnlineAccessURLs, "OnlineAccessURL")
        URL = ET.SubElement(OnlineAccessURL, "URL")
        URL.text = self.geturlType(data=ds_urls, ds_url_type='data_access')
        URLDescription = ET.SubElement(OnlineAccessURL, "URLDescription")
        URLDescription.text = "Files may be downloaded directly to your workstation from this link"

        top.append(OnlineAccessURLs)

        # ======Online Resources========#
        OnlineResources = ET.Element("OnlineResources")

        OnlineResources = self.getOnlineRessourcesCMRtags(topTag=OnlineResources, ds_urls=ds_urls)

        OnlineResource = ET.SubElement(OnlineResources, "OnlineResource")
        URL = ET.SubElement(OnlineResource, "URL")
        URL.text = "https://ghrc.nsstc.nasa.gov/home/about-ghrc/citing-ghrc-daac-data"
        Description = ET.SubElement(OnlineResource, "Description")
        Description.text = "Instructions for citing GHRC data"
        Type = ET.SubElement(OnlineResource, "Type")
        Type.text = "Citing GHRC data"

        top.append(OnlineResources)

        # =======AssociatedDIFs ================#

        AssociatedDIFs = ET.Element("AssociatedDIFs")
        DIF = ET.SubElement(AssociatedDIFs, "DIF")
        EntryId = ET.SubElement(DIF, "EntryId")
        EntryId.text = ds_short_name

        top.append(AssociatedDIFs)

        # =============Spatial tag ========================#

        Spatial = ET.Element("Spatial")
        SpatialCoverageType = ET.SubElement(Spatial, "SpatialCoverageType")
        SpatialCoverageType.text = 'Horizontal'
        HorizontalSpatialDomain = ET.SubElement(Spatial, "HorizontalSpatialDomain")
        Geometry = ET.SubElement(HorizontalSpatialDomain, "Geometry")
        CoordinateSystem = ET.SubElement(Geometry, "CoordinateSystem")
        CoordinateSystem.text = 'CARTESIAN'
        BoundingRectangle = ET.SubElement(Geometry, "BoundingRectangle")

        geomList = ['WestBoundingCoordinate', 'NorthBoundingCoordinate', 'EastBoundingCoordinate',
                    'SouthBoundingCoordinate']

        for ele in geomList:
            newTag= ET.SubElement(BoundingRectangle, ele)
            newTag.text=str(data[ele])

        GranuleSpatialRepresentation=ET.SubElement(Spatial, "GranuleSpatialRepresentation")
        GranuleSpatialRepresentation.text='CARTESIAN'

        top.append(Spatial)

        return ET.tostring(top, encoding='utf-8')

    def geturlType(self, data, ds_url_type):
        for ele in data:
            if ele['ds_url_type'] == ds_url_type:
                return ele['ds_url']
        return None


if __name__ == "__main__":
    ghrc = CollectionCMRXMLTags(configFilePath="/home/marouane/PycharmProjects/cmr/cmr.cfg.example")
    data= ghrc.generateCollectionXMLToIngest(ds_short_name=sys.argv[1])
    print(data)
