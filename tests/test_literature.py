import unittest

from modules.Literature import PublicationFetcher,PublicationAnalyserSpacy, Literature
from common.ElasticsearchLoader import Loader
from elasticsearch import Elasticsearch
from redislite import Redis
from settings import Config

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
        r_server = Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})
        es = Elasticsearch(hosts = [{'host': 'localhost', 'port': 9200}],
                       maxsize=50,
                       timeout=1800,
                       sniff_on_connection_fail=True,
                       retry_on_timeout=True,
                       max_retries=10,
                       )
        loader = Loader(es=es)
        # pub_fetcher = PublicationFetcher(es, loader=loader)
        # pub_analyser = PublicationAnalyserSpacy(pub_fetcher, es, loader)
        # pub_id = '26290144'
        
        #spacy_analysed_pub = pub_analyser.analyse_publication(pub_id=pub_id)
        Literature(es, loader, r_server).process()
       

if __name__ == '__main__':
    unittest.main()
