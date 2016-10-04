import unittest

from modules.Literature import PublicationFetcher,PublicationAnalyserSpacy, Literature, LiteratureProcess
from common.ElasticsearchLoader import Loader
from modules.EvidenceString import EvidenceStringProcess
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
    
    def test_analyse_publication(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        r_server = Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})
        #TODO : ES mocking for unit tests?
        es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200}],
                       maxsize=50,
                       timeout=1800,
                       sniff_on_connection_fail=True,
                       retry_on_timeout=True,
                       max_retries=10,
                       )
        loader = Loader(es=es,dry_run=True)
        LiteratureProcess(es, loader, r_server).process(['europepmc'])




    def test_evidence_publication_loading(self):

        logging.basicConfig(filename='output.log',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

        r_server = Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})

        # TODO : ES mocking for unit tests - use dryrun
        es = Elasticsearch(hosts=[{'host': 'targethub-es.gdosand.aws.biogen.com', 'port': 80}],
                           maxsize=50,
                           timeout=1800,
                           sniff_on_connection_fail=True,
                           retry_on_timeout=True,
                           max_retries=10,
                           )
        loader = Loader(es=es,dry_run=True)
        EvidenceStringProcess(es, r_server).process_all(datasources=['europepmc'])



       

if __name__ == '__main__':
    unittest.main()
