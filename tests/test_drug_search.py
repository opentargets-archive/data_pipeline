import unittest
import urllib2

from run import PipelineConnectors
import logging
from common.ElasticsearchLoader import Loader
from modules.GeneData import GeneManager
from modules.SearchObjects import SearchObjectProcess
from contextlib import closing
import shutil

class DrugSearchTestCase(unittest.TestCase):


    def test_drug_search(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        connectors = PipelineConnectors()
        connectors.init_services_connections()
        from elasticsearch_config import ElasticSearchConfiguration
        with Loader(connectors.es,
                    chunk_size=ElasticSearchConfiguration.bulk_load_chunk) as loader:

            GeneManager(loader, connectors.r_server).merge_all()

            SearchObjectProcess( loader, connectors.r_server).process_all()




if __name__ == '__main__':
    unittest.main()
