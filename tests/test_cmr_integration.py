import unittest

from pyCMR.pyCMR import CMR
from pyCMR.Result import Collection, Granule


class TestCMRIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cmr = CMR('cmr.cfg')

    def test_collection_search(self):
        results = self.cmr.searchCollection()
        # Make sure that the XML response was actually parsed
        self.assertTrue(isinstance(results[0], Collection))
        self.assertTrue('concept-id' in results[0].keys())
        self.assertTrue('Collection' in results[0].keys())

    def test_granule_search(self):
        results = self.cmr.searchGranule()
        self.assertTrue(isinstance(results[0], Granule))
        self.assertTrue('concept-id' in results[0].keys())
        self.assertTrue('Granule' in results[0].keys())

    def test_token_validity(self):
        pass

    def test_collection_ingest(self):
        pass

    def test_granule_ingest(self):
        pass

    def test_collection_update(self):
        pass

    def test_granule_update(self):
        pass

    def test_granule_delete(self):
        pass

    def test_collection_delete(self):
        pass


    #print cmr.searchGranule(GranuleUR="wwllnrt_20151106_daily_v1_lit.raw")

    #print cmr.searchGranule(granule_ur="AMSR_2_L2_RainOcean_R00_201508190926_A.he5")
    #print cmr.searchCollection(short_name="A2_RainOcn_NRT")
    #print cmr.getGranuleUR("granuleupdated.xml")
    #print cmr.searchGranule(granule_ur="UR_1.he13")
    #print cmr.searchGranule(granule_ur="UR_1.he15")
    #print cmr.ingestCollection("collection.xml")
    #print cmr.ingestGranule("granule.xml")
    #print cmr.ingestGranule("granuleupdated.xml")
    #print cmr.ingestGranule("/home/marouane/cmr/test-granule.xml")
    #print cmr.deleteGranule("UR_1.he11")
    #print cmr.deleteCollection("NRT AMSR2 L2B GLOBAL SWATH GSFC PROFILING ALGORITHM 2010: SURFACE PRECIPITATION, WIND SPEED OVER OCEAN, WATER VAPOR OVER OCEAN AND CLOUD LIQUID WATER OVER OCEAN V0")
