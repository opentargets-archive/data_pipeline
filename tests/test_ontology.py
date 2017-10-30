from mrtarget.modules.Ontology import OntologyClassReader
import logging
import unittest

class OntologyTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(OntologyTestCase, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(__name__)

    #@unittest.skip
    def test_mp_reader(self):

        self._logger.debug("test_mp_reader")
        ontology = OntologyClassReader()
        ontology.load_mp_classes()
        self.assertIsNotNone(ontology)
        self.assertIsNotNone(ontology.current_classes)
        self.assertTrue(len(ontology.current_classes) > 8000)
        self.assertIsNotNone(ontology.obsolete_classes)
        self.assertTrue(len(ontology.obsolete_classes) > 0)
        self.assertIsNotNone(ontology.top_level_classes)
        self.assertTrue(len(ontology.top_level_classes) > 10 and len(ontology.top_level_classes) < 30)

    #@unittest.skip
    def test_hpo_reader(self):

        self._logger.debug("test_hpo_reader")
        ontology = OntologyClassReader()
        ontology.load_hpo_classes()
        self.assertIsNotNone(ontology)
        self.assertIsNotNone(ontology.current_classes)
        self.assertTrue(len(ontology.current_classes) > 8000)
        self.assertIsNotNone(ontology.obsolete_classes)
        self.assertTrue(len(ontology.obsolete_classes) > 0)
        self.assertIsNotNone(ontology.top_level_classes)
        self.assertTrue(len(ontology.top_level_classes) > 10 and len(ontology.top_level_classes) < 30)

    #@unittest.skip
    def test_efo_reader(self):

        self._logger.debug("test_efo_reader")
        ontology = OntologyClassReader()
        ontology.load_open_targets_disease_ontology()
        self.assertIsNotNone(ontology)
        self.assertIsNotNone(ontology.current_classes)
        self.assertTrue(len(ontology.current_classes) > 8000)
        self.assertIsNotNone(ontology.obsolete_classes)
        self.assertTrue(len(ontology.obsolete_classes) > 0)
        self.assertIsNotNone(ontology.top_level_classes)
        self.assertTrue(len(ontology.top_level_classes) > 10)


if __name__ == '__main__':
    unittest.main()