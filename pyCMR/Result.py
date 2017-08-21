'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space
Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
except in compliance with the License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.

'''
import errno
import shutil
import urllib
from os import makedirs
from os.path import dirname, exists, isdir

import requests

try:
    from configparser import ConfigParser
except ImportError:
    from ConfigParser import ConfigParser

if not hasattr(urllib, 'urlretrieve'):
    urlretrieve = urllib.request.urlretrieve  # 3.0 and later
else:
    urlretrieve = urllib.urlretrieve


def mkdir_p(path):
    try:
        makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and isdir(path):
            pass
        else:
            raise


class Result(dict):
    """
    The class to structure the response xml string from the cmr API
    """
    _location = None

    def download(self, destpath=".", unm=None, pwd=None):
        """
        Download the dataset into file system
        :param destPath: use the current directory as default
        :param unm: earthexplorer username needed for ftp download
        :param pwd: earthexplorer password needed for ftp download
        :return:
        """
        url = self._location
        # Downloadable url does not exist
        if not url:
            return None
        # make dirs recursively
        destpath = destpath + "/" + url[url.find('allData'):]
        # make dirs recursively
        mkdir_p(dirname(destpath))
        # if no file exists
        if not exists(destpath):

            if url.startswith('ftp'):
                # if data is downloaded from the NRT server, need uname/pwd
                if 'nrt' in url:
                    url = url.replace('ftp://', 'ftp://' + unm + ':' + pwd + '@')
                urlretrieve(url, destpath)
            else:
                r = requests.get(url, stream=True)
                r.raw.decode_content = True

                with open(destpath + "/" + self._downloadname.replace('/', ''), 'wb') as f:
                    shutil.copyfileobj(r.raw, f)
        else:
            print('File {} already exists'.format(destpath))

    def getDownloadUrl(self):
        """
        :return:
        """
        return self._location


class Collection(Result):
    def __init__(self, metaResult, cmr_host):
        for k in metaResult:
            self[k] = metaResult[k]

        self._location = 'https://{}/search/concepts/{}.umm-json'.format(cmr_host, metaResult['concept-id'])
        self._downloadname = metaResult['Collection']['ShortName']


class Granule(Result):
    def __init__(self, metaResult):

        for k in metaResult:
            self[k] = metaResult[k]

        # Retrieve downloadable url
        try:
            if isinstance(self['Granule']['OnlineAccessURLs']['OnlineAccessURL'], dict):
                self._location = self['Granule']['OnlineAccessURLs']['OnlineAccessURL']['URL']
            elif isinstance(self['Granule']['OnlineAccessURLs']['OnlineAccessURL'], list):
                self._location = self['Granule']['OnlineAccessURLs']['OnlineAccessURL'][0]['URL']
            self._downloadname = self._location.split("/")[-1]
        except KeyError:
            self._location = None

        # Retrieve OPeNDAPUrl
        try:
            urls = self['Granule']['OnlineResources']['OnlineResource']
            self._OPeNDAPUrl = filter(lambda x: x["Type"] == "OPeNDAP", urls)[0]['URL']
        except:
            self._OPeNDAPUrl = None

    def getOPeNDAPUrl(self):
        return self._OPeNDAPUrl
