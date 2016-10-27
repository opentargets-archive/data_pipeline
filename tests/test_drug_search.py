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


    def test_analyse_publication(self):

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


    # def test_ftp_download(self):
    #     url = 'https://4.hidemyass.com/ip-1/encoded/Oi8vZnRwLmViaS5hYy51ay9wdWIvZGF0YWJhc2VzL2dlbmVuYW1lcy9uZXcvanNvbi9oZ25jX2NvbXBsZXRlX3NldC5qc29u&f=norefer'
    #
    #     with closing(urllib2.urlopen(url)) as r:
    #         with open('fileabc', 'wb') as f:
    #             shutil.copyfileobj(r, f)
    #



if __name__ == '__main__':
    unittest.main()
