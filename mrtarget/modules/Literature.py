#!/usr/local/bin/python
# -*- coding: UTF-8 -*-
import gzip
import io
import logging
import multiprocessing
import os
import time
from StringIO import StringIO

import ftputil as ftputil
import requests
from dateutil.parser import parse
from lxml import etree,objectify
from tqdm import tqdm 
from mrtarget.common import TqdmToLogger
from mrtarget.common.NLP import init_spacy_english_language

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess, RedisLookupTablePickle, \
    WhiteCollarWorker, RedisQueueWorkerThread
from mrtarget.common.connection import PipelineConnectors
from mrtarget.Settings import Config

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)

MAX_PUBLICATION_CHUNKS =100

class LiteratureActions(Actions):
    FETCH='fetch'
    UPDATE = 'update'


MEDLINE_BASE_PATH = 'pubmed/baseline'
MEDLINE_UPDATE_PATH = 'pubmed/updatefiles'
EXPECTED_ENTRIES_IN_MEDLINE_BASELINE_FILE = 30000


def ftp_connect(dir=MEDLINE_BASE_PATH):
    host = ftputil.FTPHost(Config.PUBMED_FTP_SERVER, 'anonymous', 'support@targetvalidation.org')
    host.chdir(dir)
    return host

# def download_file(ftp, file_path, download_attempt = 5):
#
#     attempt = 0
#     while attempt <= download_attempt:
#         logger.debug('Downloading file %s from ftp' % os.path.basename(file_path))
#         try:
#             ftp.download_if_newer(os.path.basename(file_path), file_path)
#             break
#         except Exception as e:
#             logger.debug('FAILED Downloading file %s from ftp: %s' % (os.path.basename(file_path), str(e)))
#             if attempt <= download_attempt:
#                 pass
#             else:
#                 raise e
#         attempt += 1
#     if attempt > download_attempt:
#         logger.error('File %s NOT DOWNLOADED from ftp' % os.path.basename(file_path))


# def download_file(ftp, file_path, download_attempt = 5):
#
#     attempt = 0
#     while attempt <= download_attempt:
#         try:
#             remote_file = 'ftp://anonymous:nopass@'+'/'.join([Config.PUBMED_FTP_SERVER, MEDLINE_BASE_PATH, os.path.basename(file_path)])
#             logger.debug('Downloading remote file %s ' % remote_file)
#             urllib.urlretrieve(remote_file, file_path)
#             break
#         except Exception as e:
#             logger.debug('FAILED Downloading file %s from ftp: %s' % (os.path.basename(file_path), str(e)))
#             if attempt <= download_attempt:
#                 pass
#             else:
#                 raise e
#         attempt += 1
#     if attempt > download_attempt:
#         logger.error('File %s NOT DOWNLOADED from ftp' % os.path.basename(file_path))

class PublicationFetcher(object):
    """
    Retireve data about a publication
    """
    _QUERY_BY_EXT_ID= '''http://www.ebi.ac.uk/europepmc/webservices/rest/search?pagesize=10&query=EXT_ID:{}&format=json&resulttype=core&page=1&pageSize=1000'''
    _QUERY_TEXT_MINED='''http://www.ebi.ac.uk/europepmc/webservices/rest/MED/{}/textMinedTerms//1/1000/json'''
    _QUERY_REFERENCE='''http://www.ebi.ac.uk/europepmc/webservices/rest/MED/{}/references//json'''

    #"EXT_ID:17440703 OR EXT_ID:17660818 OR EXT_ID:18092167 OR EXT_ID:18805785 OR EXT_ID:19442247 OR EXT_ID:19808788 OR EXT_ID:19849817 OR EXT_ID:20192983 OR EXT_ID:20871604 OR EXT_ID:21270825"

    def __init__(self, es = None, loader = None, dry_run = False):
        if loader is None:
            self.loader = Loader(es, dry_run=dry_run)
        else:
            self.loader=loader
        self.es_query=ESQuery(es)
        self.logger = logging.getLogger(__name__)


    def get_publication(self, pub_ids):

        if isinstance(pub_ids, (str, unicode)):
            pub_ids = [pub_ids]

        '''get from elasticsearch cache'''
        self.logger.debug( "getting pub id {}".format( pub_ids))
        pubs ={}
        try:

            for pub_source in self.es_query.get_publications_by_id(pub_ids):
                self.logger.debug( 'got pub %s from cache'%pub_source['pub_id'])
                pub = Publication()
                pub.load_json(pub_source)
                pubs[pub.pub_id] = pub
            # if len(pubs)<pub_ids:
            #     for pub_id in pub_ids:
            #         if pub_id not in pubs:
            #             self.logger.info( 'getting pub from remote {}'.format(self._QUERY_BY_EXT_ID.format(pub_id)))
            #             r=requests.get(self._QUERY_BY_EXT_ID.format(pub_id))
            #             r.raise_for_status()
            #             result = r.json()['resultList']['result'][0]
            #             self.logger.debug("Publication data --- {}" .format(result))
            #             pub = Publication(pub_id=pub_id,
            #                           title=result['title'],
            #                           abstract=result['abstractText'],
            #                           authors=result['authorList'],
            #                           year=int(result['pubYear']),
            #                           date=result["firstPublicationDate"],
            #                           journal=result['journalInfo'],
            #                           full_text=u"",
            #                           full_text_url=result['fullTextUrlList']['fullTextUrl'],
            #                           #epmc_keywords=result['keywordList']['keyword'],
            #                           doi=result['doi'],
            #                           cited_by=result['citedByCount'],
            #                           has_text_mined_terms=result['hasTextMinedTerms'] == u'Y',
            #                           has_references=result['hasReferences'] == u'Y',
            #                           is_open_access=result['isOpenAccess'] == u'Y',
            #                           pub_type=result['pubTypeList']['pubType'],
            #                           )
            #             if 'meshHeadingList' in result:
            #                 pub.mesh_headings = result['meshHeadingList']['meshHeading']
            #             if 'chemicalList' in result:
            #                 pub.chemicals = result['chemicalList']['chemical']
            #             if 'dateOfRevision' in result:
            #                 pub.date_of_revision = result["dateOfRevision"]
            #
            #             if pub.has_text_mined_entities:
            #                 self.get_epmc_text_mined_entities(pub)
            #             if pub.has_references:
            #                 self.get_epmc_ref_list(pub)
            #
            #             self.loader.put(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
            #                         Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
            #                         pub_id,
            #                         pub.to_json(),
            #                         )
            #             pubs[pub.pub_id] = pub
            #     self.loader.flush()
        except Exception, error:

            self.logger.error("Error in retrieving publication data for pmid {} ".format(pub_ids))
            pubs = None
            if error:
                self.logger.info(str(error))
            else:
                self.logger.info(Exception.message)

        return pubs

    def get_epmc_text_mined_entities(self, pub):
        r = requests.get(self._QUERY_TEXT_MINED.format(pub.pub_id))
        r.raise_for_status()
        json_response = r.json()
        if u'semanticTypeList' in json_response:
            result = json_response[u'semanticTypeList'][u'semanticType']
            pub.epmc_text_mined_entities = result
        return pub

    def get_epmc_ref_list(self, pub):
        r = requests.get(self._QUERY_REFERENCE.format(pub.pub_id))
        r.raise_for_status()
        json_response = r.json()
        if u'referenceList' in json_response:
            result = [i[u'id'] for i in json_response[u'referenceList'][u'reference'] if u'id' in i]
            pub.references=result
        return pub

    # def get_publication_with_analyzed_data(self, pub_ids):
    #     # self.logger.debug("getting publication/analyzed data for id {}".format(pub_ids))
    #     pubs = {}
    #     for parent_publication,analyzed_publication in self.es_query.get_publications_with_analyzed_data(ids=pub_ids):
    #         pub = Publication()
    #         pub.load_json(parent_publication)
    #         analyzed_pub= PublicationAnalysisSpacy(pub.pub_id)
    #         analyzed_pub.load_json(analyzed_publication)
    #         pubs[pub.pub_id] = [pub,analyzed_pub]
    #     return pubs

    def get_publications(self, pub_ids):
        pubs = {}
        for publication_doc in self.es_query.get_publications_by_id(ids=pub_ids):
            pub = Publication()
            pub.load_json(publication_doc)
            pubs[pub.id] = pub
        return pubs



class Publication(JSONSerializable):

    def __init__(self,
                 pub_id = u"",
                 title = u"",
                 abstract = u"",
                 authors = [],
                 pub_date = None,
                 date = None,
                 journal = None,
                 journal_reference=None,
                 full_text = u"",
                 keywords = [],
                 full_text_url=[],
                 doi=u'',
                 cited_by=None,
                 has_text_mined_terms=None,
                 is_open_access=None,
                 pub_type=[],
                 date_of_revision=None,
                 has_references=None,
                 references=[],
                 mesh_headings=[],
                 chemicals=[],
                 filename='',
                 text_analyzers = None,
                 delete_pmids = None
                 ):
        self.id = pub_id
        self.title = title
        self.abstract = abstract
        self.authors = authors
        self.pub_date = pub_date
        self.date = date
        self.journal = journal
        self.journal_reference=journal_reference
        self.full_text = full_text
        self.text_mined_entities = {}
        self.keywords = keywords
        self.full_text_url = full_text_url
        self.doi = doi
        self.cited_by = cited_by
        self.has_text_mined_entities = has_text_mined_terms
        self.is_open_access = is_open_access
        self.pub_type = pub_type
        self.date_of_revision = date_of_revision
        self.has_references = has_references
        self.references = references
        self.mesh_headings = mesh_headings
        self.chemicals = chemicals
        self.filename = filename
        self._text_analyzers = text_analyzers
        self._delete_pmids = delete_pmids

        if self.authors:
            self._process_authors()
        if self.abstract:
            self._sanitize_abstract()
            # self._split_sentences()
        if self.title or self.abstract:
            self._base_nlp()
        self._text_analyzers = None # to allow for object serialisation

    def __str__(self):
        return "id:%s | title:%s | abstract:%s | authors:%s | pub_date:%s | date:%s | journal:%s" \
               "| journal_reference:%s | full_text:%s | text_mined_entities:%s | keywords:%s | full_text_url:%s | doi:%s | cited_by:%s" \
               "| has_text_mined_entities:%s | is_open_access:%s | pub_type:%s | date_of_revision:%s | has_references:%s | references:%s" \
               "| mesh_headings:%s | chemicals:%s | filename:%s"%(self.pub_id,
                                                   self.title,
                                                   self.abstract,
                                                   self.authors,
                                                   self.pub_date,
                                                    self.date,
                                                    self.journal,
                                                    self.journal_reference,
                                                    self.full_text,
                                                    self.text_mined_entities,
                                                    self.keywords,
                                                    self.full_text_url,
                                                    self.doi,
                                                    self.cited_by,
                                                    self.has_text_mined_entities,
                                                    self.is_open_access,
                                                    self.pub_type,
                                                    self.date_of_revision,
                                                    self.has_references,
                                                    self.references,
                                                    self.mesh_headings,
                                                    self.chemicals,
                                                    self.filename
                                                    )
    def _process_authors(self):
        for a in self.authors:
            if 'ForeName' in a and a['LastName']:
                a['last_name'] = a['LastName']
                a['short_name'] = a['LastName']
                a['full_name'] = a['LastName']
                if 'Initials' in a and a['Initials']:
                    a['short_name'] += ' ' + a['Initials']
                    del a['Initials']
                if 'ForeName' in a and a['ForeName']:
                    a['full_name'] += ' ' + a['ForeName']
                    del a['ForeName']

                del a['LastName']

    def _split_sentences(self):
        #todo: use proper sentence detection with spacy/nltk
        abstract_sentences = Publication.split_sentences(self.abstract)
        self.abstract_sentences = [dict(order=i, value = sentence) for i, sentence in enumerate(abstract_sentences)]

    def _sanitize_abstract(self):
        if self.abstract and isinstance(self.abstract, list):
            self.abstract=' '.join(self.abstract)

    def get_text_to_analyze(self):
        if self.title and self.abstract:
            return  unicode(self.title + ' ' + self.abstract)
        elif self.title:
            return unicode(self.title)
        return u''

    @staticmethod
    def split_sentences(text):
        return text.split('. ')#todo: use spacy here

    def _base_nlp(self):
        for analyzer in self._text_analyzers:
            try:
                self.text_mined_entities[str(analyzer)]=analyzer.digest(self.get_text_to_analyze())
            except:
                logger.exception("error in nlp analysis with %s analyser for text: %s"%(str(analyzer), self.get_text_to_analyze()))
                self.text_mined_entities[str(analyzer)] = {}


class LiteratureLookUpTable(object):
    """
    A redis-based pickable literature look up table
    """

    def __init__(self,
                 es = None,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7):
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = r_server,
                                            ttl = ttl)
        if es is None:
            connector = PipelineConnectors()
            connector.init_services_connections(publication_es=True)
            es = connector.es
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        if r_server is not None:
            self._load_literature_data(r_server)
        self._logger = logging.getLogger(__name__)

    def _load_literature_data(self, r_server = None):
        # for pub_source in tqdm(self._es_query.get_all_pub_from_validated_evidence(datasources=['europepmc']),
        #                 desc='loading publications',
        #                 unit=' publication',
        #                 unit_scale=True,
        #                 leave=False,
        #                 ):
        #     pub = Publication()
        #     pub.load_json(pub_source)
        #
        #     self.set_literature(pub,self._get_r_server(
        #             r_server))# TODO can be improved by sending elements in batches
        return

    def get_literature(self, pmid, r_server = None):
        try:
            return self._table.get(pmid, r_server=self._get_r_server(r_server))
        except KeyError:
            try:
                pub = self._es_query.get_objects_by_id(pmid,
                                                          Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                                          Config.ELASTICSEARCH_PUBLICATION_DOC_NAME).next()
            except Exception as e:
                self._logger.exception('Cannot retrieve target from elasticsearch')
                raise KeyError()
            self.set_literature(pub, r_server)
            return pub

    def set_literature(self, literature, r_server = None):
        self._table.set((literature.pub_id), literature, r_server=self._get_r_server(
            r_server))

    def get_available_literature_ids(self, r_server = None):
        return self._table.keys()

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_literature(key, r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server=None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class instantiation or at the method level')
        return r_server

    def keys(self):
        return self._table.keys()





class MedlineRetriever(object):


    def __init__(self,
                 es,
                 loader,
                 dry_run=False,
                 r_server=None,
                 ):
        self.es =es
        self.loader = loader
        self.dry_run = dry_run
        self.r_server = r_server
        self.logger = logging.getLogger(__name__)
        tqdm_out = TqdmToLogger(self.logger,level=logging.INFO)

    def fetch(self,
              local_file_locn = [],
              update=False,
              force = False):
        if not self.dry_run:
            if not self.loader.es.indices.exists(Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)):
                self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME, recreate=force)
            self.loader.prepare_for_bulk_indexing(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)

        no_of_workers = Config.WORKERS_NUMBER
        ftp_readers = no_of_workers
        max_ftp_readers = no_of_workers/4 #avoid too many connections errors
        if ftp_readers >max_ftp_readers:
            ftp_readers = max_ftp_readers

        # FTP Retriever Queue
        retriever_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|medline_retriever',
                                         max_size=ftp_readers*3,
                                         job_timeout=1200)

        # Parser Queue
        parser_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|medline_parser',
                              max_size=MAX_PUBLICATION_CHUNKS*no_of_workers,
                              batch_size=1,
                              job_timeout=1200)

        # ES-Loader Queue
        loader_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|medline_loader',
                              batch_size=1,
                              serialiser='pickle',
                              max_size=MAX_PUBLICATION_CHUNKS*no_of_workers,
                              job_timeout=1200)



        q_reporter = RedisQueueStatusReporter([retriever_q,
                                               parser_q,
                                               loader_q,
                                               ],
                                              interval=30)
        q_reporter.start()


        if update == True:
            pubmed_xml_locn = Config.PUBMED_XML_UPDATE_LOCN
        else:
            pubmed_xml_locn = Config.PUBMED_XML_LOCN


        if not os.path.exists(pubmed_xml_locn):
            os.makedirs(pubmed_xml_locn)

        '''Start file-reader workers'''
        retrievers = WhiteCollarWorker(target=PubmedFTPReaderProcess,
                                       pool_size=ftp_readers,
                                       queue_in=retriever_q,
                                       redis_path=None,
                                       queue_out=parser_q,
                                       kwargs=dict(dry_run=self.dry_run)
                                       )
        retrievers.start()

        'Start xml file parser workers'
        parsers = WhiteCollarWorker(target=PubmedXMLParserProcess,
                                    pool_size=no_of_workers,
                                    queue_in=parser_q,
                                    redis_path=None,
                                    queue_out=loader_q,
                                    kwargs = dict(dry_run=self.dry_run)
                                    )
        parsers.start()

        '''Start es-loader workers'''
        '''Fixed number of workers to reduce the overhead of creating ES connections for each worker process'''
        loaders = WhiteCollarWorker(target=LiteratureLoaderProcess,
                                    pool_size=no_of_workers/2 +1,
                                    queue_in=loader_q,
                                    redis_path=None,
                                    kwargs=dict(dry_run=self.dry_run))


        loaders.start()

        # shift_downloading = 2
        if update:
            host = ftp_connect(MEDLINE_UPDATE_PATH)
        else:
            host = ftp_connect()
        files = host.listdir(host.curdir)
        #filter for update files
        gzip_files = [i for i in files if i.endswith('.xml.gz')]
        for file_ in tqdm(gzip_files,
                  desc='enqueuing remote files',
                  file=tqdm_out):
            if host.path.isfile(file_):
                # Remote name, local name, binary mode
                file_path = os.path.join(pubmed_xml_locn, file_)
                retriever_q.put(file_path)
                # time.sleep(shift_downloading*random.random())
        host.close()
        retriever_q.set_submission_finished(r_server=self.r_server)

        retrievers.join()

        parsers.join()

        loaders.join()

        self.logger.info('flushing data to index')
        if not self.dry_run:
            self.loader.es.indices.flush('%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
                wait_if_ongoing=True)

        self.logger.info("DONE")





class PubmedFTPReaderProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out,
                 dry_run=False,
                 update=False
                 ):
        super(PubmedFTPReaderProcess, self).__init__(queue_in, redis_path, queue_out)
        self.start_time = time.time()  # reset timer start
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        tqdm_out = TqdmToLogger(self.logger,level=logging.INFO)
        self.update = update
        self.es_query = ESQuery()
        if update:
            self.ftp = ftp_connect(dir=MEDLINE_UPDATE_PATH)
        else:
            self.ftp = ftp_connect()

    def process(self, data):
        file_path = data
        if self.skip_file_processing(os.path.basename(file_path)):
            self.logger.info("Skipping file {}".format(file_path))
            return
        file_handler = self.fetch_file(file_path)
        entries_in_file = 0
        with io.BufferedReader(gzip.GzipFile(filename=os.path.basename(file_path),
                           mode='rb',
                           fileobj=file_handler,
                           )) as f:
            for medline_rec in self.retrieve_medline_record(f):
                self.put_into_queue_out(('\n'.join(medline_rec), os.path.basename(file_path)))
                entries_in_file += 1

        if entries_in_file != EXPECTED_ENTRIES_IN_MEDLINE_BASELINE_FILE and not self.update:
            self.logger.info('Medline baseline file %s has a number of entries not expected: %i'%(os.path.basename(file_path),entries_in_file))

    def fetch_file(self,file_path):
        if not os.path.isfile(file_path):
            '''try to fetch via http'''
            http_url = [Config.PUBMED_HTTP_MIRROR]
            if self.update:
                http_url.append(MEDLINE_UPDATE_PATH.split('/')[1])
            else:
                http_url.append(MEDLINE_BASE_PATH.split('/')[1])
            http_url.append(os.path.basename(file_path))
            http_url='/'.join(http_url)
            try:
                response = requests.get(http_url, stream = True)
                response.raise_for_status()
                file_size = int(response.headers['content-length'])
                file_handler = StringIO()
                t = tqdm(desc = 'downloading %s via HTTP' % os.path.basename(file_path),
                         total = file_size,
                         unit = 'B',
                         unit_scale = True,
                         file=tqdm_out,
                         disable=self.logger.level == logging.DEBUG)
                for chunk in response.iter_content(chunk_size=128):
                    file_handler.write(chunk)
                    t.update(len(chunk))
                try:
                    self.logger.debug('Downloaded file %s from HTTP at %.2fMB/s' % (os.path.basename(file_path), (file_size / 1e6) / (t.last_print_t - t.start_t)))
                except ZeroDivisionError:
                    self.logger.debug('Downloaded file %s from HTTP'%os.path.basename(file_path))
                t.close()
                response.close()
                file_handler.seek(0)
            except Exception as e:
                self.logger.exception('Could not parse via HTTP, trying via FTP')

                '''try to fetch via ftp'''
                try:
                    ftp_file_handler =self.ftp.open(os.path.basename(file_path),'rb')
                    file_handler = StringIO(ftp_file_handler.read())
                except Exception as e:
                    self.logger.exception('Error fetching file: %s. %s . SKIPPED.' % ('/'.join([
                        Config.PUBMED_FTP_SERVER,
                        MEDLINE_BASE_PATH,
                        os.path.basename(file_path)
                    ]),
                    e.message))
                    return
        else:
            file_handler=io.open(file_path,'rb')
        return file_handler

    def retrieve_medline_record(self, file):

        record = []
        skip = True
        for line in file:
            line = line.strip()
            if line.startswith("<MedlineCitation ") or line.startswith("<DeleteCitation>"):
                skip = False
            if not skip:
                record.append(line)
            if line.startswith("</MedlineCitation>") or line.startswith("</DeleteCitation>"):
                rec = record
                skip = True
                record = []

                yield rec

    def skip_file_processing(self, file_name):
        try:
            total_docs = self.es_query.count_publications_for_file(file_name)
        except AttributeError:
            return False #dry run
        if self.update:
            if total_docs > 1:
                return True
        else:
            ''' baseline files fixed 30k records'''
            if total_docs == 30000:
                return True
        return False


    def close(self):
        self.ftp.close()

class PubmedXMLParserProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out,
                 dry_run=False):
        super(PubmedXMLParserProcess, self).__init__(queue_in, redis_path,queue_out)
        self.start_time = time.time()  # reset timer start
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        self._init_analyzers()
        self.processed_counter = 0

    def init(self):
        self._init_analyzers()


    def _init_analyzers(self):
        from mrtarget.modules.LiteratureNLP import NounChuncker, DocumentAnalysisSpacy
        self.nlp = init_spacy_english_language() #store it as class instance to make sure it is deleted if called again
        self.analyzers = [NounChuncker(), DocumentAnalysisSpacy(self.nlp)]

    def process(self,data):
        record, filename = data
        publication = dict()
        try:
            medline = objectify.fromstring(record)
            if medline.tag == 'MedlineCitation':

                publication['pmid'] = medline.PMID.text
                for child in medline.getchildren():
                    if child.tag == 'DateCreated':
                        first_publication_date = []
                        first_publication_date.append(child.Year.text)
                        first_publication_date.append(child.Month.text)
                        if child.Day.text:
                            first_publication_date.append(child.Day.text)
                        else:
                            first_publication_date.append('1')

                        publication['first_publication_date'] = parse(' '.join(first_publication_date)).date()

                    if child.tag == 'Article':
                        publication['journal_reference'] = {}
                        publication = self.parse_article_info(child,publication)

                    if child.tag == 'ChemicalList':
                        publication['chemicals'] = []
                        for chemical in child.getchildren():
                            chemical_dict = dict()
                            chemical_dict['name'] = chemical.NameOfSubstance.text
                            chemical_dict['name_id'] = chemical.NameOfSubstance.attrib['UI']
                            chemical_dict['registryNumber'] = chemical.RegistryNumber.text
                            publication['chemicals'].append(chemical_dict)

                    if child.tag == 'KeywordList':
                        publication['keywords'] = []
                        for keyword in child.getchildren():
                            publication['keywords'].append(keyword.text)

                    if child.tag == 'MeshHeadingList':
                        publication['mesh_terms'] = list()
                        for meshheading in child.getchildren():
                            mesh_heading = dict()
                            for label in meshheading.getchildren():
                                if label.tag == 'DescriptorName':
                                    mesh_heading['id'] = label.attrib['UI']
                                    mesh_heading['label'] = label.text
                                # if label.tag == 'QualifierName':
                                #     if 'qualifier' not in mesh_heading.keys():
                                #         mesh_heading['qualifier'] = list()
                                #     qualifier = dict()
                                #     qualifier['label'] = label.text
                                #     qualifier['id'] = label.attrib['UI']
                                #     mesh_heading['qualifier'].append(qualifier)

                            publication['mesh_terms'].append(mesh_heading)

            elif medline.tag == 'DeleteCitation':
                publication['delete_pmids'] = list()
                for deleted_pmid in medline.getchildren():
                    publication['delete_pmids'].append(deleted_pmid.text)

            publication['filename'] = filename

            publication['text_analyzers'] = self.analyzers
            return  Publication(pub_id=publication['pmid'],
                                  title=publication.get('title'),
                                  abstract=publication.get('abstract'),
                                  authors=publication.get('authors'),
                                  pub_date=publication.get('pub_date'),
                                  date=publication.get("first_publication_date"),
                                  journal=publication.get('journal'),
                                  journal_reference=publication.get("journal_reference"),
                                  full_text=u"",
                                  #full_text_url=publication['fullTextUrlList']['fullTextUrl'],
                                  keywords=publication.get('keywords'),
                                  doi=publication.get('doi'),
                                  #cited_by=publication['citedByCount'],
                                  #has_text_mined_terms=publication['hasTextMinedTerms'] == u'Y',
                                  #has_references=publication['hasReferences'] == u'Y',
                                  #is_open_access=publication['isOpenAccess'] == u'Y',
                                  pub_type=publication.get('pub_types'),
                                  filename=publication.get('filename'),
                                  mesh_headings=publication.get('mesh_terms'),
                                  chemicals=publication.get('chemicals'),
                                  text_analyzers = self.analyzers,
                                  )

        except etree.XMLSyntaxError as e:
            self.logger.error("Error parsing XML file {} - medline record {} %s".format(filename,record),e.message)
        self.processed_counter+=1
        if self.processed_counter%10000 == 0:
            #restart spacy from scratch to free up memory
            self._init_analyzers()
            logger.debug('restarting analyzers after %i docs processed in worker %s'%(self.processed_counter, self.name))


    def parse_article_info(self, article, publication):

        for e in article.iterchildren():
            if e.tag == 'ArticleTitle':
                publication['title'] = e.text

            if e.tag == 'Abstract':
                abstracts = []
                for abstractText in e.findall('AbstractText'):
                    abstracts.append(abstractText.text)
                publication['abstract'] = abstracts

            if e.tag == 'Journal':
                publication['journal'] = {}
                for child in e.getchildren():
                    if child.tag == 'Title':
                        publication['journal']['title'] = child.text
                    if child.tag == 'ISOAbbreviation':
                        publication['journal']['medlineAbbreviation'] = child.text
                    else:
                        publication['journal']['medlineAbbreviation'] = ''

                for el in e.JournalIssue.getchildren():
                    if el.tag == 'PubDate':
                        year, month, day = '1800', 'Jan', '1'
                        for pubdate in el.getchildren():
                            if pubdate.tag == 'Year':
                                year = pubdate.text
                            elif pubdate.tag == 'Month':
                                month = pubdate.text
                            elif pubdate.tag == 'Day':
                                day = pubdate.text

                        try:
                            publication['pub_date'] =  parse(' '.join((year, month, day))).date()
                        except ValueError:
                            pass
                    if el.tag == 'Volume':
                        publication['journal_reference']['volume'] = el.text
                    if el.tag == 'Issue':
                        publication['journal_reference']['issue'] = el.text

            if e.tag == 'PublicationTypeList':
                pub_types = []
                for pub_type in e.PublicationType:
                    pub_types.append(pub_type.text)
                publication['pub_types'] = pub_types

            if e.tag == 'ELocationID' and e.attrib['EIdType'] == 'doi':
                publication['doi'] = e.text

            if e.tag == 'AuthorList':
                publication['authors'] = list()
                for author in e.Author:
                    author_dict = dict()
                    for e in author.getchildren():
                        if e.tag != 'AffiliationInfo':
                            author_dict[e.tag] = e.text

                    publication['authors'].append(author_dict)

            if e.tag == 'Pagination':
                publication['journal_reference']['pgn'] = e.MedlinePgn.text


        return publication

class LiteratureLoaderProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out = None,
                 dry_run=False):
        super(LiteratureLoaderProcess, self).__init__(queue_in, redis_path, queue_out)
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)

    def init(self):
        self.loader = Loader(chunk_size=1000, dry_run=self.dry_run)
        self.es = self.loader.es
        self.es_query = ESQuery(self.es)


    def process(self, data):
        pub = data
        if pub._delete_pmids:
            for pmid in pub._delete_pmids:
                '''update parent and analyzed child publication with empty values'''
                pub = Publication(pub_id=pmid,filename=pub.filename)
                self.loader.put(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                                pmid,
                                pub,
                                create_index=False)
        else:
            try:

                self.loader.put(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                                pub.id,
                                pub,
                                create_index=False)
            except KeyError as e:
                self.logger.exception("Error creating publication object for pmid {} , filename {}, missing key: {}".format(
                    pub.id,
                    pub.filename,
                    e.message))


    def close(self):
        self.loader.close()


