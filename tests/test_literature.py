import unittest

from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue
from modules.Literature import PublicationFetcher,PublicationAnalyserSpacy, LiteratureProcess, MedlineRetriever, \
    PubmedFTPReaderProcess, PubmedXMLParserProcess, MEDLINE_UPDATE_PATH
from common.ElasticsearchLoader import Loader
from modules.EvidenceString import EvidenceStringProcess
from elasticsearch import Elasticsearch
from redislite import Redis
from settings import Config
import logging
from run import PipelineConnectors
import io
import gzip
import os
from collections import Counter
from modules.Literature import ftp_connect
from StringIO import StringIO



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

        es = Elasticsearch(hosts = [{'host': 'targethub-es.gdosand.aws.biogen.com', 'port': 80}],
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

        p = PipelineConnectors()
        p.init_services_connections()
        EvidenceStringProcess(p.es, p.r_server).process_all(datasources=['europepmc'],inject_literature=True)


    def test_medline_parser(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        p = PipelineConnectors()
        p.init_services_connections()
        with Loader(p.es ,chunk_size=1000) as loader:
            MedlineRetriever(p.es, loader, False, p.r_server).fetch()


    def test_medline_parser_update(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        p = PipelineConnectors()
        p.init_services_connections()
        with Loader(p.es ,chunk_size=1000) as loader:
            MedlineRetriever(p.es, loader, False, p.r_server).fetch(update=True)

    def test_medline_baseline_xml_parsing(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q,None,None)

        file_path = 'resources/test-medlinexml/medline16n0001.xml.gz'
        file_handler = io.open(file_path, 'rb')

        with io.BufferedReader(gzip.GzipFile(filename=os.path.basename(file_path),
                                             mode='rb',
                                             fileobj=file_handler,
                                             )) as f:

            for medline_record in reader.retrieve_medline_record(f):
                self.assertIsNotNone(medline_record)
                publication = parser.process((''.join(medline_record),os.path.basename(file_path)))
                '''Empty Publication in case of parsing error '''
                self.assertIsNotNone(publication)
                pmid =  publication['pmid']
                '''Test for dates < 1900, python limitation'''
                if pmid == '16691646':
                    dt = publication['pub_date']
                    self.assertEqual(str(dt), '1865-01-01')


    def test_medline_update_xml_parsing(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)


        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q, None, None)

        file_path = 'resources/test-medlinexml/medline16n0773.xml.gz'
        file_handler = io.open(file_path, 'rb')

        with io.BufferedReader(gzip.GzipFile(filename=os.path.basename(file_path),
                                             mode='rb',
                                             fileobj=file_handler,
                                             )) as f:
            for medline_record in reader.retrieve_medline_record(f):
                self.assertIsNotNone(medline_record)
                publication = parser.process((''.join(medline_record), os.path.basename(file_path)))
                self.assertIsNotNone(publication)

    def test_medline_baseline_record_no(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)


        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q, None, None)

        file_path = 'resources/test-medlinexml/medline16n0156.xml.gz'
        file_handler = io.open(file_path, 'rb')
        pmid_list = []
        with io.BufferedReader(gzip.GzipFile(filename=os.path.basename(file_path),
                                             mode='rb',
                                             fileobj=file_handler,
                                             )) as f:
            for medline_record in reader.retrieve_medline_record(f):
                self.assertIsNotNone(medline_record)
                publication = parser.process((''.join(medline_record), os.path.basename(file_path)))
                self.assertIsNotNone(publication)
                pmid_list.append(publication['pmid'])

        duplicate_pmids = [k for k, v in Counter(pmid_list).items() if v > 1]
        print duplicate_pmids

    '''To be run after each baseline load and before update files are loaded'''
    def test_loader_medline_baseline(self):
        ftp = ftp_connect()
        files = ftp.listdir(ftp.curdir)
        es_query = ESQuery()
        for file_ in files[100:300]:

            total = es_query.count_publications_for_file(file_)
            self.assertEqual(total,30000,file_)


    '''To be run after only after update files are loaded'''
    def test_loader_medline_update(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        ftp = ftp_connect(MEDLINE_UPDATE_PATH)
        files = ftp.listdir(ftp.curdir)
        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q, None, None)

        p = PipelineConnectors()
        p.init_services_connections()
        fetcher = PublicationFetcher(p.es)
        es_query = ESQuery(p.es)
        for file_ in files[0:1]:
            total_parsed = 0
            ftp_file_handler = ftp.open(file_, 'rb')
            file_handler = StringIO(ftp_file_handler.read())

            with io.BufferedReader(gzip.GzipFile(filename=file_,
                                                 mode='rb',
                                                 fileobj=file_handler,
                                                 )) as f:
                for medline_record in reader.retrieve_medline_record(f):
                    self.assertIsNotNone(medline_record)
                    publication = parser.process((''.join(medline_record), file_))
                    self.assertIsNotNone(publication)

                    total_parsed +=1

            delete_pmids = publication['delete_pmids']
            total_parsed += len(delete_pmids)
            total = es_query.count_publications_for_file(file_)
            self.assertEqual(total, total_parsed - 1, file_)

            ''' Verify deletedcitations are emptied in ES '''

            for pmid in delete_pmids:
                pubs = fetcher.get_publication(pmid)
                pub = pubs[str(pmid)]
                self.assertEqual(pub.title, '')
                self.assertEqual(pub.pub_date, None)
                self.assertEqual(pub.abstract, '')
                self.assertEqual(pub.journal, None)




if __name__ == '__main__':
    unittest.main()
