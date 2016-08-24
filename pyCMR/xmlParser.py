from itertools import izip



def ComaSeperatedToListJson(pathToFile="data_example"):
    #py=GHRCCatalog("configFile.cfg")
    fileLines=open(pathToFile)
    lines=[]
    listJson=[]
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
