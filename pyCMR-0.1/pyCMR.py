__version__ = '0.1'

import json
import requests
import re
from Result import *
from xmlParser import XmlListConfig
from xmlParser import XmlDictConfig
from xml.etree import ElementTree

granuleUrl = "https://cmr.earthdata.nasa.gov/search/granules?page_size=50&page_num={}"
granuleMetaUrl = "https://cmr.earthdata.nasa.gov/search/granules.echo10?page_size=50&page_num={}"
collectionUrl = "https://cmr.earthdata.nasa.gov/search/collections?page_size=50&page_num={}"
collectionMetaUrl = "https://cmr.earthdata.nasa.gov/search/collections.json?page_size=50&page_num={}"

headers = {"Client-Id": "servir"}

def printHello():
    """
    A test function
    :return:
    """
    print ("Hello World!")



def _searchResult(url, limit, **kwargs):
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
    result = [requests.get(url.format(pagenum), headers=headers).content
              for pagenum in xrange(1, (limit - 1) / 50 + 2)]
    # for res in result:
    #     for ref in re.findall("<reference>(.*?)</reference>", res):
    #         yield ref
    return [ref for res in result
            for ref in re.findall("<reference>(.*?)</reference>", res)]


def searchGranule(limit=100,  **kwargs):
    """
    Search the CMR granules
    :param limit: limit of the number of results
    :param kwargs: search parameters
    :return: list of results (<Instance of Result>)
    """
    print ("======== Waiting for response ========")
    metaUrl = granuleMetaUrl
    for k, v in kwargs.items():
        metaUrl += "&{}={}".format(k, v)
    
    metaResult = [requests.get(metaUrl.format(pagenum), headers=headers).content
                  for pagenum in xrange(1, (limit - 1) / 50 + 2)]

    # The first can be the error msgs
    root = ElementTree.XML(metaResult[0])
    if root.tag == "errors":
        print (" |- Error: " + str([ch.text for ch in root._children]))
        return

    metaResult = [ref for res in metaResult
                  for ref in XmlListConfig(ElementTree.XML(res))[2:]]

    return [Granule(m) for m in metaResult][:limit]


def searchCollection(limit=100, **kwargs):
    """
    Search the CMR collections
    :param limit: limit of the number of results
    :param kwargs: search parameters
    :return: list of results (<Instance of Result>)
    """
    print ("======== Waiting for response ========")
    metaUrl = collectionMetaUrl
    for k, v in kwargs.items():
        metaUrl += "&{}={}".format(k, v)

    metaResult = [requests.get(metaUrl.format(pagenum), headers=headers)
                  for pagenum in xrange(1, (limit - 1) / 50 + 2)]

    try:
        metaResult = [ref for res in metaResult
                  for ref in json.loads(res.content)['feed']['entry']]
    except KeyError:
        print ( " |- Error: " + str((json.loads(metaResult[0].content))["errors"]))
        return
    locationResult = _searchResult(collectionUrl, limit=limit, **kwargs)
    #print locationResult
    return [Collection(m, l) for m, l in zip(metaResult, locationResult)][:limit]
