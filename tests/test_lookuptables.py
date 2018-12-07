import re
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.connection import PipelineConnectors
from mrtarget.common.LookupTables import ECOLookUpTable
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

class LookupTablesTestCase(unittest.TestCase):

    def __init__(self, *args, **kwargs):
        super(LookupTablesTestCase, self).__init__(*args, **kwargs)
        self._logger = logging.getLogger(__name__)
        self.connectors = init_services_connections()
        self.loader = Loader(self.connectors.es, dry_run=DRY_RUN)

    @mock.patch('mrtarget.common.connection')
    def test_eco_get_id(self, mock_connectors):

        iri = 'http://purl.obolibrary.org/obo/SO_0000639'
        self.assertTrue(re.match('SO_0000639', ECOLookUpTable.get_ontology_code_from_url(iri)))
        iri = 'http://purl.obolibrary.org/obo/SO_0000638'
        self.assertTrue(re.match('SO_0000638', ECOLookUpTable.get_ontology_code_from_url(iri)))


if __name__ == '__main__':
    unittest.main()
