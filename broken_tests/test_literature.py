import unittest

from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisQueue
from mrtarget.modules.Literature import PublicationFetcher, MedlineRetriever, \
    PubmedFTPReaderProcess, PubmedXMLParserProcess, MEDLINE_UPDATE_PATH, Publication
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.modules.EvidenceString import EvidenceStringProcess
from elasticsearch import Elasticsearch
from redislite import Redis
from mrtarget.Settings import Config
import logging
from run import PipelineConnectors
import io
import gzip
import os

class LiteratureTestCase(unittest.TestCase):



    def test_evidence_publication_loading(self):

        logging.basicConfig(filename='output.log',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

        p = PipelineConnectors()
        p.init_services_connections()
        EvidenceStringProcess(p.es, p.r_server).process_all(datasources=['europepmc'])


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

        file_path = 'resources/test-medlinexml/test_baseline.xml.gz'
        filedir = os.path.dirname(__file__)
        file_handler = io.open(os.path.join(filedir,file_path), 'rb')

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
                if pmid == '17796445':
                    dt = publication['pub_date']
                    self.assertEqual(str(dt), '1881-04-16')

                expected_abstract = ['abc\\n', 'pqr']
                if pmid == '17832547':
                    self.assertEqual(publication['abstract'], expected_abstract)
                if pmid == '25053090':
                    dt = publication['pub_date']
                    self.assertEqual(str(dt), '2015-01-01')
                    self.assertEqual(publication['doi'],'10.1016/j.foodchem.2014.05.102')
                    mesh_terms =  [{'id': 'D000975', 'label': 'Antioxidants'}, {'id': 'D050859', 'label': 'Betacyanins'}, {'id': 'D029421', 'label': 'Cactaceae'}, {'id': 'D002851', 'label': 'Chromatography, High Pressure Liquid'}, {'id': 'D005638', 'label': 'Fruit'}, {'id': 'D009569', 'label': 'Nitric Oxide'}, {'id': 'D010545', 'label': 'Peroxides'}, {'id': 'D010936', 'label': 'Plant Extracts'}]
                    keywords = ['Antioxidant', 'Betacyanin', 'ESR', 'Hylocereus polyrhizus', 'Nitric oxide', 'Peroxyl radical']
                    journal = {'medlineAbbreviation': 'Food Chem', 'title': 'Food chemistry'}
                    chemicals = [{'registryNumber': '0', 'name': 'Antioxidants'}, {'registryNumber': '0', 'name': 'Betacyanins'}, {'registryNumber': '0', 'name': 'Peroxides'}, {'registryNumber': '0', 'name': 'Plant Extracts'}, {'registryNumber': '0', 'name': 'phyllocactin'}, {'registryNumber': '3170-83-0', 'name': 'perhydroxyl radical'}, {'registryNumber': '31C4KY9ESH', 'name': 'Nitric Oxide'}, {'registryNumber': '5YJC992ZP6', 'name': 'betanin'}]
                    authors = [{'LastName': 'Taira', 'Initials': 'J', 'ForeName': 'Junsei'}, {'LastName': 'Tsuchida', 'Initials': 'E', 'ForeName': 'Eito'}, {'LastName': 'Katoh', 'Initials': 'MC', 'ForeName': 'Megumi C'}, {'LastName': 'Uehara', 'Initials': 'M', 'ForeName': 'Masatsugu'}, {'LastName': 'Ogi', 'Initials': 'T', 'ForeName': 'Takayuki'}]
                    # authors = [{'full_name': 'Taira Junsei', 'short_name': 'Taira J', 'last_name': 'Taira'}, {'last_name': 'Tsuchida', 'short_name': 'Tsuchida E', 'full_name': 'Tsuchida Eito'}, {'last_name': 'Katoh', 'short_name': 'Katoh MC', 'full_name': 'Katoh Megumi C'}, {'last_name': 'Uehara', 'short_name': 'Uehara M', 'full_name': 'Masatsugu'}, {'last_name': 'Ogi', 'short_name': 'T', 'full_name': 'Ogi Takayuki'}]
                    self.assertEqual(publication['mesh_terms'], mesh_terms)
                    self.assertEqual(publication['journal'], journal)
                    self.assertEqual(publication['chemicals'], chemicals)
                    self.assertEqual(publication['authors'], authors)
                    self.assertEqual(publication['keywords'], keywords)

    def test_medline_update_xml_parsing(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)


        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q, None, None)

        file_path = 'resources/test-medlinexml/test_update.xml.gz'
        filedir = os.path.dirname(__file__)
        file_handler = io.open(os.path.join(filedir,file_path), 'rb')
        parsed_publications = []

        with io.BufferedReader(gzip.GzipFile(filename=os.path.basename(file_path),
                                             mode='rb',
                                             fileobj=file_handler,
                                             )) as f:
            for medline_record in reader.retrieve_medline_record(f):
                self.assertIsNotNone(medline_record)
                publication = parser.process((''.join(medline_record), os.path.basename(file_path)))
                self.assertIsNotNone(publication)
                parsed_publications.append(publication)

            self.assertEqual(parsed_publications[0]['journal'], {'medlineAbbreviation': 'Nature', 'title': 'Nature'})
            self.assertEqual(parsed_publications[0]['journal_reference'], {'volume': '526', 'pgn': '391-6', 'issue': '7573'})
            self.assertEqual(parsed_publications[0]['doi'], '10.1038/nature14655')
            self.assertEqual(str(parsed_publications[0]['first_publication_date']),'2015-10-16')
            self.assertEqual(parsed_publications[0]['title'], 'Molecular basis of ligand recognition and transport by glucose transporters.')
            self.assertEqual(publication['delete_pmids'], ['26470892','26477054'])

if __name__ == '__main__':
    unittest.main()
