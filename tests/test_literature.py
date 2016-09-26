import unittest

from modules.EvidenceString import EvidenceStringProcess
from modules.EvidencePublicationAdapter import EvidenceStringPublicationAdapter
from modules.Literature import PublicationFetcher,PublicationAnalyserSpacy, Literature
from common.ElasticsearchLoader import Loader
from elasticsearch import Elasticsearch
from redislite import Redis
from settings import Config
import logging

class LiteratureTestCase(unittest.TestCase):
#     
#         _test_pub_ids =['24523595',
#                     '26784250',
#                     '27409410',
#                     '26290144',
#                     '25787843',Open
#                     '26836588',
#                     '26781615',
#                     '26646452',
#                    '26774881',
#                     '26629442',
#                     '26371324',
#                     '24817865',
#                   PublicationAnalyserSpacy]
    
    def analyse_publication(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        r_server = Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})
        #TODO : ES mocking for unit tests - use dryrun
        es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200}],
                       maxsize=50,
                       timeout=1800,
                       sniff_on_connection_fail=True,
                       retry_on_timeout=True,
                       max_retries=10,
                       )
        loader = Loader(es=es)
        Literature(es, loader, r_server).process()

    def test_evidence_publication_adapter(self):

        logging.basicConfig(filename='output.log',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)


        r_server = Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})
        es = Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}],
                   maxsize=50,
                   timeout=1800,
                   sniff_on_connection_fail=True,
                   retry_on_timeout=True,
                   max_retries=10,
                   )
        loader = Loader(es=es)

        EvidenceStringPublicationAdapter(es, loader, r_server).process_evidence_string()







       

if __name__ == '__main__':
    unittest.main()
