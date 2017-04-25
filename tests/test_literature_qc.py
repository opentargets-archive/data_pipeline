import unittest

from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisQueue
from mrtarget.modules.Literature import PublicationFetcher, \
    PubmedFTPReaderProcess, PubmedXMLParserProcess, MEDLINE_UPDATE_PATH

import logging
from run import PipelineConnectors
import io
import gzip

from mrtarget.modules.Literature import ftp_connect

class LiteratureQCTestCase(unittest.TestCase):

    ''' To be run after each baseline load and before update files are loaded'''
    def test_loader_medline_baseline(self):
        ftp = ftp_connect()
        files = ftp.listdir(ftp.curdir)
        es_query = ESQuery()
        errors = []
        for file_ in files:

            total = es_query.count_publications_for_file(file_)
            #medline packages its records in a series of files, 
            # each with 30,000 records.
            if total != 30000:
                errors.append('ES documents {} not equal to 30k for file {}'.format(total, file_))
        logging.error(errors)
        self.assertEqual(len(errors), 0)

    '''To be run after only after update files are loaded'''

    def test_loader_medline_update(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        ftp = ftp_connect(MEDLINE_UPDATE_PATH)
        files = ftp.listdir(ftp.curdir)
        gzip_files = [i for i in files if i.endswith('.xml.gz')]
        retriever_q = RedisQueue()
        parser_q = RedisQueue()
        reader = PubmedFTPReaderProcess(retriever_q, None, None)
        parser = PubmedXMLParserProcess(parser_q, None, None)

        p = PipelineConnectors()
        p.init_services_connections()
        fetcher = PublicationFetcher(p.es)
        errors = []
        for file_ in gzip_files:
            total_parsed = 0
            pmid_list = []
            file_handler = reader.fetch_file(file_)

            with io.BufferedReader(gzip.GzipFile(filename=file_,
                                                 mode='rb',
                                                 fileobj=file_handler,
                                                 )) as f:
                for medline_record in reader.retrieve_medline_record(f):
                    self.assertIsNotNone(medline_record)
                    publication = parser.process((''.join(medline_record), file_))
                    self.assertIsNotNone(publication)
                    if 'delete_pmids' not in publication:
                        pmid_list.append(publication.get('pmid'))
                    total_parsed += 1
            pubs = fetcher.get_publication(pmid_list)

            # self.assertEqual(len(set(pmid_list)),len(pubs),'Total parsed {} not equal to ES publications {} for file {}'.format(len(set(pmid_list)), len(pubs),file_))
            if len(set(pmid_list)) != len(pubs):
                errors.append(
                    'Total parsed {} not equal to ES publications {} for file {}'.format(len(set(pmid_list)), len(pubs),
                                                                                         file_))
            ''' Verify deletedcitation records are present in ES '''
            if 'delete_pmids' in publication:
                delete_pmids = publication['delete_pmids']
                pubs = fetcher.get_publication(delete_pmids)
                # self.assertEqual(len(delete_pmids), len(pubs) ,
                #              'Deleted - Total parsed {} not equal to ES publications {} for file {}'.format(len(delete_pmids),
                #                                                                                   len(pubs), file_))
                if len(delete_pmids) != len(pubs):
                    errors.append('Deleted - Total parsed {} not equal to ES publications {} for file {}'.format(
                        len(delete_pmids),
                        len(pubs), file_))

            logging.info("Test completed for file : {} ".format(file_))
        logging.error(errors)
        self.assertEqual(len(errors), 0, 'Errors in update xml loading')


if __name__ == '__main__':
    unittest.main()
