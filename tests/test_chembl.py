import logging
import unittest
from mrtarget.modules.ChEMBL import ChEMBLLookup

logger = logging.getLogger(__name__)




class TestChembl(unittest.TestCase):


    def setUp(self):
        self.handler = ChEMBLLookup()


    def test_drug_names_for_targets(self):
        self.handler.download_targets()
        self.assertGreater(len(self.handler.targets),0)
        self.handler.download_mechanisms()
        self.assertGreater(len(self.handler.mechanisms), 0)
        self.handler.download_molecules_linked_to_target()
        self.assertGreater(len(self.handler.molecule2synonyms), 0)


    def test_protein_classes(self):
        self.handler.download_protein_classification()



