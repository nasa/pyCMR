'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
try:
    from future_builtins import zip
except ImportError: # not 2.6+ or is 3.x
    try:
        from itertools import izip as zip # < 2.5 or 3.x
    except ImportError:
        pass

def ComaSeperatedToListJson(pathToFile="data_example", data=None):
    #py=GHRCCatalog("configFile.cfg")

    fileLines=open(pathToFile)

    lines=[]
    listJson=[]
    print(fileLines)

    for ele in fileLines:
        if (ele=='\n'):
            continue
        ele=ele.replace("=", ",")

        lines.append(ele)
    for ele in lines:
        iterator=iter(ele.split(","))
        args=dict(izip(iterator,iterator))
        #post=py.ingestGranule(**args)
        #print post['status'], post['result']
        listJson.append(args)
    return listJson

def ComaSeperatedDataToListJson(data):
    lines = []
    listJson = []
    for ele in data:
        ele=ele[0] # the data is a list of data so I am taking the first elemnt from each
        ele = ele.replace("=", ",")
        lines.append(ele)
    for ele in lines:
        iterator = iter(ele.split(","))
        args = dict(izip(iterator, iterator))
            # post=py.ingestGranule(**args)
            # print post['status'], post['result']
        listJson.append(args)
    return listJson

class XmlListConfig(list):
    """
    Parse the xml as a python list
    """
    def __init__(self, aList):
        for element in aList:
            if len(element) > 0:
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)

class XmlDictConfig(dict):
    """
    Parse the xml as a python dict
    """
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if len(element) > 0:
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                else:
                    aDict = {element[0].tag: XmlListConfig(element)}
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})

            elif element.items():
                self.update({element.tag: dict(element.items())})
            else:
                self.update({element.tag: element.text})
