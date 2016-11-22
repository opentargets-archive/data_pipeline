import logging

from modules.ChEMBL import ChEMBLLookup
from nose.tools import assert_greater

logger = logging.getLogger(__name__)




class TestChembl(object):
    @classmethod
    def setup_class(cls):
        pass

    @classmethod
    def teardown_class(cls):
        pass

    def setup(self):
        self.handler = ChEMBLLookup()

    def teardown(self):
        pass

    def test_drug_names_for_targets(self):
        handler = ChEMBLLookup()
        handler.download_targets()
        assert_greater(len(handler.targets),0)
        handler.download_mechanisms()
        assert_greater(len(handler.mechanisms), 0)
        handler.download_molecules_linked_to_target()
        assert_greater(len(handler.molecule2synonyms), 0)



