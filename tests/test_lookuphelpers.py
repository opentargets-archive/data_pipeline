from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.connection import PipelineConnectors
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
import logging
from Settings import Config
import unittest

def init_services_connections():
    logging.info("init_services_connections")
    connectors = PipelineConnectors()
    m = connectors.init_services_connections()
    logging.debug('Attempting to establish connection to the backend... %s',
                       str(m))
    return connectors

class LookupHelpersTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LookupHelpersTestCase, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(__name__)
        self.connectors = init_services_connections()
        self.loader = Loader(self.connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = False)

    def test_mp_lookup(self):

        self._logger.debug("test_mp_lookup")

        self.assertIsNotNone(self.connectors.es)
        self.assertIsNotNone(self.connectors.r_server)

        lookup_data = LookUpDataRetriever(self.connectors.es, self.connectors.r_server,
                                          data_types=(
                                              LookUpDataType.MP,
                                          ),
                                          autoload=True,
                                          ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.mp_ontology)
        self.assertIsNotNone(lookup_data.mp_ontology.current_classes)
        self.assertIsNotNone(lookup_data.mp_ontology.obsolete_classes)
        self.assertIsNotNone(lookup_data.mp_ontology.top_level_classes)

    def test_hpo_lookup(self):
        self._logger.debug("test_hpo_lookup")

        self.assertIsNotNone(self.connectors.es)
        self.assertIsNotNone(self.connectors.r_server)

        lookup_data = LookUpDataRetriever(self.connectors.es, self.connectors.r_server,
                                      data_types=(
                                          LookUpDataType.HPO,
                                      ),
                                      autoload=True,
                                      ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.hpo_ontology)
        self.assertIsNotNone(lookup_data.hpo_ontology.current_classes)
        self.assertIsNotNone(lookup_data.hpo_ontology.obsolete_classes)
        self.assertIsNotNone(lookup_data.hpo_ontology.top_level_classes)

    def test_efo_lookup(self):
        self._logger.debug("test_hpo_lookup")

        self.assertIsNotNone(self.connectors.es)
        self.assertIsNotNone(self.connectors.r_server)

        lookup_data = LookUpDataRetriever(self.connectors.es, self.connectors.r_server,
                                      data_types=(
                                          LookUpDataType.EFO,
                                      ),
                                      autoload=True,
                                      ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.efo_ontology)
        self.assertIsNotNone(lookup_data.efo_ontology.current_classes)
        self.assertIsNotNone(lookup_data.efo_ontology.obsolete_classes)
        self.assertIsNotNone(lookup_data.efo_ontology.top_level_classes)

if __name__ == '__main__':
    unittest.main()