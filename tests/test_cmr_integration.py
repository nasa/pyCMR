import os
import unittest
import xml.etree.ElementTree as ET

from pyCMR import CMR, Collection, Granule


class TestCMRIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cmr = CMR('cmr.cfg')

        cls._test_collection_path = os.path.join('.', 'tests', 'fixtures', 'test-collection.xml')
        cls._test_granule_path = os.path.join('.', 'tests', 'fixtures', 'test-granule.xml')
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
