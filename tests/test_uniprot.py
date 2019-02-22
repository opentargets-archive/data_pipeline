import unittest
import os

from mrtarget.common import URLZSource
from mrtarget.modules.Uniprot import UniprotDownloader
from mrtarget.common.UniprotIO import Parser

class UniprotTestCase(unittest.TestCase):
    def test_uniprot_loader(self):
        resources_path = os.path.dirname(os.path.realpath(__file__))
        uniprot_uri = resources_path + os.path.sep + "resources" + os.path.sep + "uniprot.xml.gz"

        downloader = UniprotDownloader(None, True)
        downloader.process(uniprot_uri, True)

        self.assertEqual(downloader.total_entries, 1)
