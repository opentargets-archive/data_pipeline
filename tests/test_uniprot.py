import unittest
import os

from mrtarget.common import URLZSource
from mrtarget.modules.Uniprot import UniprotDownloader
from mrtarget.common.UniprotIO import Parser


class UniprotTestCase(unittest.TestCase):
    def test_uniprot_loader(self):
        resources_path = os.path.dirname(os.path.realpath(__file__))
        uniprot_uri = resources_path + os.path.sep + "resources" + os.path.sep + "uniprot.xml.gz"

        entries = []
        with URLZSource(uniprot_uri).open() as r_file:
            for i, xml in enumerate(UniprotDownloader._iterate_xml(r_file, UniprotDownloader.NS)):
                entries.append(xml.id)

        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0], "Q9UHF1")
