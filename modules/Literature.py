#!/usr/local/bin/python
# coding: latin-1
import errno
import gzip
import logging
import multiprocessing
import os
import re
import string
import time
from collections import Counter
from ftplib import FTP,all_errors
from itertools import chain

import requests
from elasticsearch import Elasticsearch
from lxml import etree,objectify
from nltk.corpus import stopwords
from sklearn.base import TransformerMixin
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
from spacy.en import English
from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess, RedisLookupTablePickle
from settings import Config

MAX_PUBLICATION_CHUNKS =1000

class LiteratureActions(Actions):
    FETCH='fetch'
    PROCESS= 'process'

# List of symbols we don't care about
SYMBOLS = " ".join(string.punctuation).split(" ") + ["-----", "---", "...", "“", "”", "'ve"]

LABELS = {
    u'ENT': u'ENT',
    u'PERSON': u'ENT',
    u'NORP': u'ENT',
    u'FAC': u'ENT',
    u'ORG': u'ENT',
    u'GPE': u'ENT',
    u'LOC': u'ENT',
    u'LAW': u'ENT',
    u'PRODUCT': u'ENT',
    u'EVENT': u'ENT',
    u'WORK_OF_ART': u'ENT',
    u'LANGUAGE': u'ENT',
    u'DATE': u'DATE',
    u'TIME': u'TIME',
    u'PERCENT': u'PERCENT',
    u'MONEY': u'MONEY',
    u'QUANTITY': u'QUANTITY',
    u'ORDINAL': u'ORDINAL',
    u'CARDINAL': u'CARDINAL'
}


class PublicationFetcher(object):
    """
    Retireve data about a publication
    """
    _QUERY_BY_EXT_ID= '''http://www.ebi.ac.uk/europepmc/webservices/rest/search?pagesize=10&query=EXT_ID:{}&format=json&resulttype=core&page=1&pageSize=1000'''
    _QUERY_TEXT_MINED='''http://www.ebi.ac.uk/europepmc/webservices/rest/MED/{}/textMinedTerms//1/1000/json'''
    _QUERY_REFERENCE='''http://www.ebi.ac.uk/europepmc/webservices/rest/MED/{}/references//json'''

    #"EXT_ID:17440703 OR EXT_ID:17660818 OR EXT_ID:18092167 OR EXT_ID:18805785 OR EXT_ID:19442247 OR EXT_ID:19808788 OR EXT_ID:19849817 OR EXT_ID:20192983 OR EXT_ID:20871604 OR EXT_ID:21270825"

    def __init__(self, es, loader = None):
        if loader is None:
            self.loader = Loader(es)
        else:
            self.loader=loader
        self.es = es
        self.es_query=ESQuery(es)
        self.logger = logging.getLogger(__name__)


    def get_publication(self, pub_ids):

        if isinstance(pub_ids, (str, unicode)):
            pub_ids=[pub_ids]

        '''get from elasticsearch cache'''
        logging.info( "getting pub id {}".format( pub_ids))
        pubs ={}
        try:

            for pub_source in self.es_query.get_publications_by_id(pub_ids):
                logging.info( 'got pub %s from cache'%pub_source['pub_id'])
                pub = Publication()
                pub.load_json(pub_source)
                pubs[pub.pub_id] = pub
            # if len(pubs)<pub_ids:
            #     for pub_id in pub_ids:
            #         if pub_id not in pubs:
            #             logging.info( 'getting pub from remote {}'.format(self._QUERY_BY_EXT_ID.format(pub_id)))
            #             r=requests.get(self._QUERY_BY_EXT_ID.format(pub_id))
            #             r.raise_for_status()
            #             result = r.json()['resultList']['result'][0]
            #             logging.debug("Publication data --- {}" .format(result))
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
            logging.error("Error in retrieving publication data for pmid {} ".format(pub_ids))
            if error:
                logging.info(str(error))
            else:
                logging.info(Exception.message)

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

    def get_publication_with_analyzed_data(self, pub_ids):
        logging.debug("getting publication/analyzed data for id {}".format(pub_ids))
        pubs = {}
        for parent_publication,analyzed_publication in self.es_query.get_publications_with_analyzed_data(ids=pub_ids):
            pub = Publication()
            pub.load_json(parent_publication)
            analyzed_pub= PublicationAnalysisSpacy(pub.pub_id)
            analyzed_pub.load_json(analyzed_publication)
            pubs[pub.pub_id] = [pub,analyzed_pub]
        return pubs


class Publication(JSONSerializable):

    def __init__(self,
                 pub_id = u"",
                 title = u"",
                 abstract = u"",
                 authors = [],
                 year = None,
                 date = None,
                 journal = u"",
                 full_text = u"",
                 epmc_text_mined_entities = {},
                 epmc_keywords = [],
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
                 filename=''

                 ):
        self.pub_id = pub_id
        self.title = title
        self.abstract = abstract
        self.authors = authors
        self.year = year
        self.date = date
        self.journal = journal
        self.full_text = full_text
        self.epmc_text_mined_entities = epmc_text_mined_entities
        self.epmc_keywords = epmc_keywords
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

    def __str__(self):
        return "id:%s | title:%s | abstract:%s | authors:%s | year:%s | date:%s | journal:%s" \
               "| full_text:%s | epmc_text_mined_entities:%s | epmc_keywords:%s | full_text_url:%s | doi:%s | cited_by:%s" \
               "| has_text_mined_entities:%s | is_open_access:%s | pub_type:%s | date_of_revision:%s | has_references:%s | references:%s" \
               "| mesh_headings:%s | chemicals:%s | filename:%s"%(self.pub_id,
                                                   self.title,
                                                   self.abstract,
                                                   self.authors,
                                                   self.year,
                                                    self.date,
                                                    self.journal,
                                                    self.full_text,
                                                    self.epmc_text_mined_entities,
                                                    self.epmc_keywords,
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



class PublicationAnalysis(JSONSerializable):
    """
    Base class for all publication analysis
    """

    def __init__(self,
                 pub_id):
        self.pub_id = pub_id

    def get_type(self):
        '''Define the type for elasticsearch here'''
        return NotImplementedError()


class PublicationAnalysisSpacy(PublicationAnalysis):
    """
    Stores results of the analysis done by spacy
    """

    def __init__(self,
                 pub_id,
                 lemmas=(),
                 noun_chunks=(),
                 analysed_sentences_count=1,
                 ):
        super(PublicationAnalysisSpacy, self).__init__(pub_id)
        self.lemmas = lemmas
        self.noun_chunks = noun_chunks
        self.analysed_sentences_count = analysed_sentences_count


    def get_type(self):
        '''Define the type for elasticsearch here'''
        return Config.ELASTICSEARCH_PUBLICATION_DOC_ANALYSIS_SPACY_NAME



class PublicationAnalyserSpacy(object):
    def __init__(self, fetcher, es, loader = None):
        if loader is None:
            self.loader = Loader(es)
        self.fetcher = fetcher
        self.logger = logging.getLogger(__name__)
        self.parser = English()
        # A custom stoplist
        STOPLIST = set(stopwords.words('english') + ["n't", "'s", "'m", "ca","p","t"] + list(ENGLISH_STOP_WORDS))
        ALLOWED_STOPLIST=set(('non'))
        self.STOPLIST = STOPLIST - ALLOWED_STOPLIST


    def analyse_publication(self, pub_id, pub = None):

        if pub is None:
            pub = self.fetcher.get_publication(pub_ids=pub_id)
        analysed_pub = None
        if pub.title and pub.abstract:
            text_to_parse = unicode(pub.title + ' ' + pub.abstract)
            lemmas, noun_chunks, analysed_sentences_count = self._spacy_analyser(text_to_parse)
            lemmas= tuple({'value':k, "count":v} for k,v in lemmas.items())
            noun_chunks= tuple({'value':k, "count":v} for k,v in noun_chunks.items())
            analysed_pub = PublicationAnalysisSpacy(pub_id = pub_id,
                                        lemmas=lemmas,
                                        noun_chunks=noun_chunks,
                                        analysed_sentences_count=analysed_sentences_count)

        return analysed_pub


    def _spacy_analyser(self, abstract):
        # #TODO: see PriYanka notebook tests, and/or code below

        lemmas, tokens, parsedEx = self.tokenizeText(abstract)
        parsed_vector = self.transform_doc(parsedEx)
        tl = abstract.lower()
        sents_count = len(list(parsedEx.sents))
        ec = Counter()
        #    print('ENTITIES:')
        for e in parsedEx.ents:
            e_str = u' '.join(t.orth_ for t in e).encode('utf-8').lower()
            if ((not e.label_) or (e.label_ == u'ENT')) and not (e_str in self.STOPLIST) and not (e_str in SYMBOLS):
                if e_str not in ec:
                    try:
                        ec[e_str] += tl.count(e_str)
                    except:
                     logging.info(e_str)
                        #            print( e_str, e_str in STOPLIST)
                        #        print (e.label, repr(e.label_),  ' '.join(t.orth_ for t in e))
        # print('FILTERED NOUN CHUNKS:')
        # for k, v in ec.most_common(5):
        #     print k, round(float(v) / sents_count, 3)

        return lemmas, ec, sents_count


    # A custom function to tokenize the text using spaCy
    # and convert to lemmas
    def tokenizeText(self, sample):
        # get the tokens using spaCy
        tokens_all = self.parser(unicode(sample))
        #    for t in tokens_all.noun_chunks:
        #        print(t, list(t.subtree))
        # lemmatize
        lemmas = []
        for tok in tokens_all:
            lemmas.append(tok.lemma_.lower().strip() if tok.lemma_ != "-PRON-" else tok.lower_)
        tokens = lemmas

        # stoplist the tokens
        tokens = [tok for tok in tokens if tok.encode('utf-8') not in self.STOPLIST]

        # stoplist symbols
        tokens = [tok for tok in tokens if tok.encode('utf-8') not in SYMBOLS]

        # remove large strings of whitespace
        while "" in tokens:
            tokens.remove("")
        while " " in tokens:
            tokens.remove(" ")
        while "\n" in tokens:
            tokens.remove("\n")
        while "\n\n" in tokens:
            tokens.remove("\n\n")
        filtered = []
        for tok in tokens_all:
            if tok.lemma_.lower().strip() in tokens and tok.pos_ in ['PROP', 'PROPN', 'NOUN', 'ORG', 'FCA', 'PERSON']:
                filtered.append(tok)
        c = Counter([tok.lemma_.lower().strip() for tok in filtered])
        sents_count = len(list(tokens_all.sents))
        # print 'COMMON LEMMAS'
        # for i in c.most_common(5):
        #     if i[1] > 1:
        #         print i[0], round(float(i[1]) / sents_count, 3)
        return c, tokens, tokens_all

    def represent_word(self, word):
        if word.like_url:
            return '%%URL|X'
        text = re.sub(r'\s', '_', word.text)
        tag = LABELS.get(word.ent_type_, word.pos_)
        if not tag:
            tag = '?'
        return text + '|' + tag

    def transform_doc(self, doc):

        for ent in doc.ents:

            ent.merge(ent.root.tag_, ent.text, LABELS[ent.label_])

        for np in list(doc.noun_chunks):
            #        print (np, np.root.tag_, np.text, np.root.ent_type_)
            while len(np) > 1 and np[0].dep_ not in ('advmod', 'amod', 'compound'):
                np = np[1:]
            # print (np, np.root.tag_, np.text, np.root.ent_type_)

            np.merge(np.root.tag_, np.text, np.root.ent_type_)
        strings = []
        for sent in doc.sents:
            if sent.text.strip():
                strings.append(' '.join(self.represent_word(w) for w in sent if not w.is_space))
        if strings:
            return '\n'.join(strings) + '\n'
        else:
            return ''


class LiteratureProcess(object):


    def __init__(self,
                 es,
                 loader,
                 r_server=None,
                 ):
        self.es = es
        self.es_query=ESQuery(es)
        self.loader = loader
        self.r_server = r_server
        self.logger = logging.getLogger(__name__)
        if not self.es.indices.exists(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME):
            self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)


    def fetch(self,
              datasources=[],
              ):

        #TODO: load everything with a fetcher in parallel
        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)
        fetched_pub_ids=set()
        for ev in tqdm(self.es_query.get_all_pub_ids_from_evidence(),
                    desc='Reading available evidence_strings to fetch publications ids',
                    total = self.es_query.count_validated_evidence_strings(datasources= datasources),
                    unit=' evidence',
                    unit_scale=True):
            pub_id= self.get_pub_id_from_url(ev)
            if pub_id not in fetched_pub_ids:
                pubs = pub_fetcher.get_publication(pub_id)
                fetched_pub_ids.add(pub_id)


    def process(self,
                datasources=[],
                dry_run=False):
        #TODO: process everything with an analyser in parallel


        # for t in [text, text2, text3, text4, text5, text6]:

        #todo, add method to process all cached publications??

        # Literature Queue
        literature_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_analyzer_q',
                                  max_size=MAX_PUBLICATION_CHUNKS,
                                  job_timeout=120)

        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        # Start literature-analyser-worker processes
        analyzers = [LiteratureAnalyzerProcess(literature_q,
                                               self.r_server.db,
                                               dry_run,
                                               ) for i in range(no_of_workers)]

        for a in analyzers:
            a.start()

        #TODO : Use separate queue for retrieving evidences?
        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)

        for ev in tqdm(self.es_query.get_all_pub_ids_from_evidence(datasources= datasources),
                    desc='Reading available evidence_strings to analyse publications',
                    total = self.es_query.count_validated_evidence_strings(datasources= datasources),
                    unit=' evidence',
                    unit_scale=True):
            pub_id= self.get_pub_id_from_url(ev['evidence_string']['literature']['references'][0]['lit_id'])
            pubs = pub_fetcher.get_publication(pub_id)

            literature_q.put(pubs)
        literature_q.set_submission_finished(r_server=self.r_server)

            # TODO - auditing?
        #wait for all spacy workers to finish
        for a in analyzers:
                a.join()

        logging.info('flushing data to index')

        self.loader.es.indices.flush('%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
            wait_if_ongoing=True)
        logging.info("DONE")

    @staticmethod
    def get_pub_id_from_url(url):
        return url.split('/')[-1]


# Every step in a pipeline needs to be a "transformer".
# Define a custom transformer to clean text using spaCy
class CleanTextTransformer(TransformerMixin):
    """
    Convert text to cleaned text
    """

    def transform(self, X, **transform_params):
        return [cleanText(text) for text in X]

    def fit(self, X, y=None, **fit_params):
        return self

    def get_params(self, deep=True):
        return {}


# A custom function to clean the text before sending it into the vectorizer
def cleanText(text):
    # get rid of newlines
    text = text.strip().replace("\n", " ").replace("\r", " ")

    # replace twitter @mentions
    mentionFinder = re.compile(r"@[a-z0-9_]{1,15}", re.IGNORECASE)
    text = mentionFinder.sub("@MENTION", text)

    # replace HTML symbols
    text = text.replace("&amp;", "and").replace("&gt;", ">").replace("&lt;", "<")

    # lowercase
    text = text.lower()

    return text

class LiteratureLookUpTable(object):
    """
    A redis-based pickable literature look up table
    """

    def __init__(self,
                 es,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7):
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = r_server,
                                            ttl = ttl)
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        if r_server is not None:
            self._load_literature_data(r_server)

    def _load_literature_data(self, r_server = None):
        for parent_publication, analyzed_publication in tqdm(self._es_query.get_all_publications(),
                        desc='loading publications',
                        unit=' publication',
                        unit_scale=True,
                        total=self._es_query.count_all_publications(),
                        leave=False,
                        ):
            literature = parent_publication
            literature['abstract_lemmas'] = analyzed_publication['lemmas']
            literature['noun_chunks'] = analyzed_publication['noun_chunks']
            self.set_literature(literature,self._get_r_server(
                    r_server))# TODO can be improved by sending elements in batches

    def get_literature(self, pmid, r_server = None):
        return self._table.get(pmid, r_server=r_server)

    def set_literature(self, literature, r_server = None):
        self._table.set((literature['pub_id']), literature, r_server=self._get_r_server(
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




class LiteratureAnalyzerProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 chunk_size = 1e4,
                 dry_run=False
                 ):
        super(LiteratureAnalyzerProcess, self).__init__(queue_in, redis_path)
        self.queue_in = queue_in
        self.redis_path = redis_path
        self.es = Elasticsearch(Config.ELASTICSEARCH_URL)
        self.start_time = time.time()
        self.audit = list()
        self.logger = logging.getLogger(__name__)
        pub_fetcher = PublicationFetcher(self.es)
        self.chunk_size = chunk_size
        self.dry_run = dry_run
        self.pub_analyser = PublicationAnalyserSpacy(pub_fetcher, self.es)

    def process(self, data):
        publications = data
        logging.debug("In LiteratureAnalyzerProcess- {} ".format(self.name))
        try:

            for pub_id, pub in publications.items():
                spacy_analysed_pub = self.pub_analyser.analyse_publication(pub_id=pub_id,
                                                                       pub=pub)

            with Loader(self.es, chunk_size=self.chunk_size, dry_run=self.dry_run) as es_loader:
                es_loader.put(index_name=Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                            doc_type=spacy_analysed_pub.get_type(),
                            ID=pub_id,
                            body=spacy_analysed_pub.to_json(),
                            parent=pub_id,
                            )
                logging.debug("Literature data updated for pmid {}".format(pub_id))


        except Exception, error:
            logging.error("Error in loading analysed publication for pmid {}".format(pub_id))
            if error:
                logging.info(str(error))
            else:
                logging.info(Exception.message)


class PubmedLiteratureProcess(object):


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

    def fetch(self, local_file_locn = []):
        ts = time.time()
        if not self.loader.es.indices.exists(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME):
            self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)
        self.loader.prepare_for_bulk_indexing(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)

        # Literature Reader Queue
        literature_reader_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_reader_q',
                                         max_size=MAX_PUBLICATION_CHUNKS,
                                         job_timeout=120)

        # Literature Parser Queue
        literature_parser_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_parser_q',
                                  max_size=MAX_PUBLICATION_CHUNKS,
                                  job_timeout=120)

        # Literature ES-Loader Queue
        literature_loader_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_loader_q',
                                         max_size=MAX_PUBLICATION_CHUNKS,
                                         job_timeout=120)



        q_reporter = RedisQueueStatusReporter([literature_reader_q,
                                               literature_parser_q,
                                               literature_loader_q,
                                               ],
                                              interval=10)
        q_reporter.start()


        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        '''Start file-reader workers'''
        readers = [PubmedXMLReaderProcess(literature_reader_q,
                                          self.r_server.db,
                                          literature_parser_q,
                                          self.dry_run
                                          )
                   for i in range(no_of_workers)]

        for w in readers:
            w.start()



        'Start xml file parser workers'
        parsers = [PubmedXMLParserProcess(literature_parser_q,
                                     self.r_server.db,
                                     literature_loader_q,
                                     self.dry_run
                                     )
                   for i in range(no_of_workers)]

        for w in parsers:
            w.start()

        '''Start es-loader workers'''
        '''Fixed number of workers to reduce the overhead of creating ES connections for each worker process'''
        loaders = [LiteratureLoaderProcess(literature_loader_q,
                                          self.r_server.db,
                                          self.dry_run
                                          )
                   for i in range(no_of_workers)]

        for w in loaders:
            w.start()


        if not os.path.exists(Config.PUBMED_XML_LOCN):
            os.makedirs(Config.PUBMED_XML_LOCN)

        ftp = self.ftp_connect()
        files = ftp.nlst()
        max_attempts = 5

        # TODO : for testing
        # for j in range(len(files)):
        for j in range(800, 812):

            try:
                file_path = os.path.join(Config.PUBMED_XML_LOCN, files[j])

                if not os.path.isfile(file_path):
                    self.download_file(ftp, file_path)
                literature_reader_q.put(file_path)

            except all_errors as e:
                logging.error("Error downloading medline file {} from FTP server".format(files[j]), str(e))
                if max_attempts != 0:
                    ftp = self.ftp_connect()
                    max_attempts -= 1
                else:
                    break

        literature_reader_q.set_submission_finished(r_server=self.r_server)

        for r in readers:
                r.join()

        for p in parsers:
                p.join()

        for l in loaders:
                l.join()

        logging.info('flushing data to index')

        self.loader.es.indices.flush('%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
            wait_if_ongoing=True)
        te = time.time()
        logging.info("Total time taken {}".format(te-ts))
        logging.info("DONE")


    def process(self, datasources=[]):
        # Literature Queue
        literature_analyzer_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_analyzer_q',
                                  max_size=MAX_PUBLICATION_CHUNKS,
                                  job_timeout=120)

        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        # Start literature-analyser-worker processes
        analyzers = [LiteratureAnalyzerProcess(literature_analyzer_q,
                                               self.r_server.db,
                                               self.dry_run,
                                               ) for i in range(no_of_workers)]

        for a in analyzers:
            a.start()

        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)

        for ev in tqdm(self.es_query.get_all_pub_ids_from_evidence(datasources=datasources),
                       desc='Reading available evidence_strings to analyse publications',
                       total=self.es_query.count_validated_evidence_strings(datasources=datasources),
                       unit=' evidence',
                       unit_scale=True):
            pub_id = self.get_pub_id_from_url(ev['evidence_string']['literature']['references'][0]['lit_id'])
            pubs = pub_fetcher.get_publication(pub_id)

            literature_analyzer_q.put(pubs)
        literature_analyzer_q.set_submission_finished(r_server=self.r_server)

        # TODO - auditing?
        # wait for all spacy workers to finish
        for a in analyzers:
            a.join()

        logging.info('flushing data to index')

        self.loader.es.indices.flush('%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
                                     wait_if_ongoing=True)
        logging.info("DONE")


    def ftp_connect(self):
        ftp = FTP(Config.PUBMED_FTP_SERVER, timeout=30*60)
        ftp.login()
        ftp.cwd('pubmed/baseline')

        return ftp

    def download_file(self, ftp, file_path):
        handle = open(file_path, 'wb')
        ftp.retrbinary('RETR ' + os.path.basename(file_path), handle.write)
        handle.close()


class PubmedXMLReaderProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out,
                 dry_run=False):
        super(PubmedXMLReaderProcess, self).__init__(queue_in, redis_path,queue_out)
        self.start_time = time.time()  # reset timer start
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        file_path = data
        self.logger.debug("Process Name {}".format(self.name))
        self.logger.debug('Reading file %s' % file_path)
        ''' read xml file and put each medline record in the parser queue '''
        self.read_medline_xml(file_path)
        self.logger.debug("%s finished" % self.name)

    def read_medline_xml(self, file_name):

        with gzip.open(file_name, 'r') as f:
            record = []
            skip = True
            for line in f:
                if line.startswith("<MedlineCitation Owner"):
                     skip = False
                if not skip:
                     record.append(line)
                if line.startswith("</MedlineCitation>"):
                    self.queue_out.put(('\n'.join(record), file_name), self.r_server)
                    skip = True

class PubmedXMLParserProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out,
                 dry_run=False):
        super(PubmedXMLParserProcess, self).__init__(queue_in, redis_path,queue_out)
        self.es = Elasticsearch(hosts=Config.ELASTICSEARCH_URL,
                                maxsize=50,
                                timeout=1800,
                                sniff_on_connection_fail=True,
                                retry_on_timeout=True,
                                max_retries=10,
                                )
        self.start_time = time.time()  # reset timer start
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)


    def process(self,data):
        record, filename = data
        publication = dict()
        try:
            medline = objectify.fromstring(record)
            publication['pmid'] = medline.PMID.text
            for e in medline.Article.iterchildren():
                if e.tag == 'ArticleTitle':
                    publication['title'] = e.text
                if e.tag == 'Abstract':
                    publication['abstract'] = e.AbstractText.text

            publication['filename'] = filename
            self.queue_out.put(publication, self.r_server)
        except etree.XMLSyntaxError as e:
            logging.error("Error parsing XML file {} - medline record {}".format(filename, record), str(e))

        publication = dict()
        try:
            medline = objectify.fromstring(record)
            publication['pmid'] = medline.PMID.text
            for child in medline.getchildren():
                if child.tag == 'DateCreated':
                    firstPublicationDate = []
                    firstPublicationDate.append(child.Year.text)
                    firstPublicationDate.append(child.Month.text)
                    firstPublicationDate.append(child.Day.text)

                    publication['firstPublicationDate'] = '-'.join(firstPublicationDate)

                if child.tag == 'Article':
                    publication = self.parse_article_info(child,publication)

                if child.tag == 'ChemicalList':
                    publication['chemicals'] = []
                    for chemical in child.getchildren():
                        chemical_dict = dict()
                        chemical_dict['name'] = chemical.NameOfSubstance.text
                        chemical_dict['registryNumber'] = chemical.RegistryNumber.text
                        publication['chemicals'].append(chemical_dict)



                if child.tag == 'KeywordList':
                    publication['keywords'] = []
                    for keyword in child.getchildren():
                        publication['keywords'].append(keyword.text)


                if child.tag == 'MeshHeadingList':
                    publication['mesh_terms'] = list()
                    for meshheading in child.getchildren():
                        publication['mesh_terms'].append(meshheading.DescriptorName.text)

            publication['filename'] = filename
            self.queue_out.put(publication,self.r_server)
        except etree.XMLSyntaxError as e:
            logging.error("Error parsing XML file {} - medline record {}".format(filename,record),str(e))

    # Event parser not needed anymore
    # def iter_parse_medline_xml(self, record):
    #
    #
    #         context = etree.iterparse(gzip.open(file, 'rb'), events=('end',))
    #         for event, elem in context:
    #             if elem.tag == 'PMID' and elem.getparent().tag == 'MedlineCitation':
    #                     pmid = ''
    #                     mesh_terms = ''
    #                     article_list = []
    #                     keywords = ''
    #                     publication = {}
    #                     pmid = elem.text
    #                     publication['pmid'] = pmid
    #             if elem.tag == 'MeshHeadingList':
    #                     mesh_terms = self.parse_mesh_terms(elem)
    #                     publication['mesh_terms'] = mesh_terms
    #             if elem.tag == 'KeywordList':
    #                     keywords = self.parse_keywords(elem)
    #                     publication['keywords'] = keywords
    #             if elem.tag == 'ChemicalList':
    #                     chemicals = self.parse_chemicals(elem)
    #                     publication['chemicals'] = chemicals
    #             if elem.tag == 'Article':
    #                     article = self.parse_article_info(elem)
    #                     publication['article'] = article
    #                     # elem.clear()  # discard the element
    #                     # while elem.getprevious() is not None:
    #                     #     del elem.getparent()[0]
    #                     # simplejson.dump(publication, handle)
    #
    #             if elem.tag == 'MedlineCitation':
    #                     elem.clear()  # discard the element
    #                     while elem.getprevious() is not None:
    #                         del elem.getparent()[0]
    #
    #                     self.queue_out.put((publication,file),self.r_server)
    #
    #
    #         del context


    def stringify_children(self,node):

        parts = ([node.text] +
                 list(chain(*([c.text, c.tail] for c in node.getchildren()))) +
                 [node.tail])
        return ''.join(filter(None, parts))


    def parse_article_info(self, article, publication):

        for e in article.iterchildren():
            if e.tag == 'ArticleTitle':
                publication['title'] = e.text

            if e.tag == 'Abstract':
                abstracts = []
                for abstractText in e:
                    abstracts.append(abstractText.AbstractText.text)
                publication['abstract'] = abstracts

            if e.tag == 'Journal':
                publication['journal'] = {}
                for child in e.getchildren():
                    if child.tag == 'Title':
                        publication['journal']['title'] = child.text
                    if child.tag == 'ISOAbbreviation':
                        publication['journal']['medlineAbbreviation'] = child.text

                publication['pub_year'] = ''
                for pubdate in e.JournalIssue.PubDate.getchildren():
                    if pubdate.tag == 'Year':
                        publication['pub_year'] = pubdate.text

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
                        author_dict[e.tag] = e.text

                    publication['authors'].append(author_dict)

        return publication

class LiteratureLoaderProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 dry_run=False):
        super(LiteratureLoaderProcess, self).__init__(queue_in, redis_path)
        self.es = Elasticsearch(hosts=Config.ELASTICSEARCH_URL,
                                maxsize=50,
                                timeout=1800,
                                sniff_on_connection_fail=True,
                                retry_on_timeout=True,
                                max_retries=10,
                                )
        self.loader = Loader(self.es, chunk_size=1000)
        self.es_query = ESQuery(self.es)
        self.start_time = time.time()  # reset timer start
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        publication = data

        try:
            pub = Publication(pub_id=publication['pmid'],
                          title=publication['title'],
                          abstract=publication.get('abstract'),
                          authors=publication.get('authors'),
                          year=publication.get('pub_year'),
                          date=publication.get("firstPublicationDate"),
                          journal=publication.get('journal'),
                          full_text=u"",
                          #full_text_url=publication['fullTextUrlList']['fullTextUrl'],
                          epmc_keywords=publication.get('keywords'),
                          doi=publication.get('doi',''),
                          #cited_by=publication['citedByCount'],
                          #has_text_mined_terms=publication['hasTextMinedTerms'] == u'Y',
                          #has_references=publication['hasReferences'] == u'Y',
                          #is_open_access=publication['isOpenAccess'] == u'Y',
                          pub_type=publication.get('pub_types'),
                          filename=publication.get('filename'),
                          mesh_headings=publication.get('mesh_terms'),
                          chemicals=publication.get('chemicals')
                          )

        # if 'dateOfRevision' in publication:
        #     pub.date_of_revision = publication["dateOfRevision"]
        #
        # if pub.has_text_mined_entities:
        #     self.get_epmc_text_mined_entities(pub)
        # if pub.has_references:
        #     self.get_epmc_ref_list(pub)

            self.loader.put(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                            Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                            publication['pmid'],
                            pub,
                            create_index=False)
        except KeyError as e:
            logging.error("Error creating publication object for pmid {} , filename {}, missing key: {}".format(
                publication['pmid'],
                publication['filename'],
                e.message))

    def close(self):
        self.loader.close()


