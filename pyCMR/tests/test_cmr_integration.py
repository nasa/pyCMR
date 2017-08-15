'''
Copyright 2017, United States Government, as represented by the Administrator of the National Aeronautics and Space Administration. All rights reserved.

The pyCMR platform is licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.

'''
import os
import unittest
import xml.etree.ElementTree as ET

from ..pyCMR import CMR, Collection, Granule

class TestCMRIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        configFilePath = "pyCMRConfig.cfg"


        cls.cmr = CMR(configFilePath)

        cls._test_collection_path = os.path.abspath(os.curdir) + "/pyCMR/tests/fixtures/test-collection.xml" #os.path.join(os.curdir, 'tests', 'fixtures', 'test-collection.xml')
        cls._test_granule_path = os.path.abspath(os.curdir) + "/pyCMR/tests/fixtures/test-granule.xml" #os.path.join(os.curdir, 'tests', 'fixtures', 'test-granule.xml')
        cls._test_collection_name = 'PYCMR TEST COLLECTION'
        cls._test_granule_name = 'PYCMR_TEST_GRANULE.hd5'

    def collection_search(self):
        results = self.cmr.searchCollection()
        # Make sure that the XML response was actually parsed
        self.assertTrue(isinstance(results[0], Collection))
        self.assertTrue('concept-id' in results[0].keys())
        self.assertTrue('Collection' in results[0].keys())

    def granule_search(self):
        results = self.cmr.searchGranule()
        self.assertTrue(isinstance(results[0], Granule))
        self.assertTrue('concept-id' in results[0].keys())
        self.assertTrue('Granule' in results[0].keys())

    def collection_ingest(self):
        result = self.cmr.ingestCollection(self._test_collection_path)
        # If ingest wasn't successful, the above would've thrown a 4XX error
        # But just to be sure, let's check that there was a result in the returned XML
        # Otherwise, the top-level tag would be `<errors>`
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def granule_ingest(self):
        result = self.cmr.ingestGranule(self._test_granule_path)
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def collection_update(self):
        result = self.cmr.updateCollection(self._test_collection_path)
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def granule_update(self):
        result = self.cmr.updateGranule(self._test_granule_path)
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def granule_delete(self):
        result = self.cmr.deleteGranule(self._test_granule_name)
        # Confirm that a tombstone was returned
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def collection_delete(self):
        result = self.cmr.deleteCollection(self._test_collection_name)
        parsed = ET.XML(result)
        self.assertTrue(parsed.tag == 'result')

    def test_monolith(self):
         '''
         Since these are order-sensitive integration tests,
         wrap them in a monolithic test, so that they run in the proper order
         and stop after a single failure (without having to specify `failfast`)
    
         https://stackoverflow.com/questions/5387299/python-unittest-testcase-execution-order
         '''

         for test_name in [
             'collection_search',
             'granule_search',
             'collection_ingest',
             'granule_ingest',
             'collection_update',
             'granule_update',
             'granule_delete',
             'collection_delete'
         ]:
             test = getattr(self, test_name)
             test()

    def test_search_limit(self):
         ''' Make sure that the correct number of items are returned by searches '''
         results = self.cmr.searchCollection(limit=3)
         self.assertTrue(len(results) == 3)
         results = self.cmr.searchGranule(limit=91)
         self.assertTrue(len(results) == 91)
