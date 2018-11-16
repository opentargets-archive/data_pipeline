import unittest
import os

import logging
from mrtarget.modules.ChEMBL import ChEMBLLookup

class ChemblTestCase(unittest.TestCase):
    def test_chembl_loader_any(self):
        _l = logging.getLogger(__name__)

        entries = {}
        molecule_set = set()
        ChEMBLLookup._populate_synonyms_for_molecule(molecule_set,entries, _l)

        self.assertEqual(len(entries), 0)

    def test_chembl_loader_some(self):
        _l = logging.getLogger(__name__)

        ch_id = "CHEMBL6020"
        mol_id = "CHEMBL17564"
        syn_name = "Methane"
        entries = {}
        molecule_set = set(ch_id)
        ChEMBLLookup._populate_synonyms_for_molecule(molecule_set, entries, _l)
        print(str(entries))

        self.assertEqual(entries[mol_id][0], syn_name)
