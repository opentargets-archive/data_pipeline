import unittest
import os

from mrtarget.common.ElasticsearchQuery import ESQuery, Loader
from mrtarget.modules.Uniprot import UniprotDownloader


class HarmonicSumTestCase(unittest.TestCase):
    def test_uniprot_loader(self):
        resources_path = os.path.dirname(os.path.realpath(__file__))
        loader = UniprotDownloader(Loader())
        loader.cache_human_entries(uri=resources_path + os.path.sep + "uniprot.xml.gz")
        metrics = loader.qc(ESQuery())

        self.assertEqual(metrics["uniprot.count"], 1)
