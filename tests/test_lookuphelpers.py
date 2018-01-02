import sys
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.common.connection import PipelineConnectors
import logging
import mock
import unittest

DRY_RUN=True

_logger = logging.getLogger(__name__)

def init_services_connections():
    '''
    Use this if you want to test against ElasticSearch
    :return: a PipelineConnectors instance
    '''
    _logger.info("init_services_connections")
    connectors = PipelineConnectors()
    m = connectors.init_services_connections()
    _logger.debug('Attempting to establish connection to the backend... %s',
                       str(m))
    return connectors

class LookupHelpersTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LookupHelpersTestCase, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(__name__)
        self.connectors = init_services_connections()
        self.loader = Loader(self.connectors.es, dry_run=DRY_RUN)

    @mock.patch('mrtarget.common.connection')
    def test_eco_lookup(self, mock_connectors):

        self._logger.debug("test_mp_lookup")

        self.assertIsNotNone(mock_connectors.es)
        self.assertIsNotNone(mock_connectors.r_server)

        lookup_data = LookUpDataRetriever(mock_connectors.es, mock_connectors.r_server,
                                          data_types=(
                                              LookUpDataType.MP,
                                          ),
                                          autoload=True,
                                          ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.mp_ontology)
        self.assertIsNotNone(lookup_data.mp_ontology.current_classes)
        self.assertTrue(len(lookup_data.mp_ontology.current_classes) > 8000)
        self.assertIsNotNone(lookup_data.mp_ontology.obsolete_classes)
        self.assertTrue(len(lookup_data.mp_ontology.obsolete_classes) > 0)
        self.assertIsNotNone(lookup_data.mp_ontology.top_level_classes)
        self.assertTrue(len(lookup_data.mp_ontology.top_level_classes) > 10 and len(lookup_data.mp_ontology.top_level_classes) < 30)

    @mock.patch('mrtarget.common.connection')
    def test_hpo_lookup(self, mock_connectors):
        self._logger.debug("test_hpo_lookup")

        self.assertIsNotNone(mock_connectors.es)
        self.assertIsNotNone(mock_connectors.r_server)

        lookup_data = LookUpDataRetriever(mock_connectors.es, mock_connectors.r_server,
                                          data_types=(
                                              LookUpDataType.HPO,
                                          ),
                                          autoload=True,
                                          ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.hpo_ontology)
        self.assertIsNotNone(lookup_data.hpo_ontology.current_classes)
        self.assertTrue(len(lookup_data.hpo_ontology.current_classes) > 8000)
        self.assertIsNotNone(lookup_data.hpo_ontology.obsolete_classes)
        self.assertTrue(len(lookup_data.hpo_ontology.obsolete_classes) > 0)
        self.assertIsNotNone(lookup_data.hpo_ontology.top_level_classes)
        self.assertTrue(
            len(lookup_data.hpo_ontology.top_level_classes) > 10 and len(lookup_data.hpo_ontology.top_level_classes) < 30)

    @mock.patch('mrtarget.common.connection')
    def test_efo_lookup(self, mock_connectors):
        self._logger.debug("test_hpo_lookup")

        self.assertIsNotNone(mock_connectors.es)
        self.assertIsNotNone(mock_connectors.r_server)

        lookup_data = LookUpDataRetriever(mock_connectors.es, mock_connectors.r_server,
                                      data_types=(
                                          LookUpDataType.EFO,
                                      ),
                                      autoload=True,
                                      ).lookup

        self.assertIsNotNone(lookup_data)
        self.assertIsNotNone(lookup_data.efo_ontology)
        self.assertIsNotNone(lookup_data.efo_ontology.current_classes)
        self.assertTrue(len(lookup_data.efo_ontology.current_classes) > 8000)
        self.assertIsNotNone(lookup_data.efo_ontology.obsolete_classes)
        self.assertTrue(len(lookup_data.efo_ontology.obsolete_classes) > 0)
        self.assertIsNotNone(lookup_data.efo_ontology.top_level_classes)
        self.assertTrue(
            len(lookup_data.efo_ontology.top_level_classes) > 10)

if __name__ == '__main__':
    unittest.main()
