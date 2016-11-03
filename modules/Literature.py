#!/usr/local/bin/python
# coding: latin-1
import logging
from pprint import pprint

import requests
from sklearn.base import TransformerMixin
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
from nltk.corpus import stopwords
import string
import re
from spacy.en import English
from collections import Counter

from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from elasticsearch import Elasticsearch
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
import multiprocessing
from settings import Config
from common.Redis import RedisQueue, RedisQueueStatusReporter, RedisQueueWorkerProcess, RedisLookupTablePickle

import pubmed_parser as pp
import json
import simplejson
from itertools import chain

import time
from lxml import etree


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
            if len(pubs)<pub_ids:
                for pub_id in pub_ids:
                    if pub_id not in pubs:
                        logging.info( 'getting pub from remote {}'.format(self._QUERY_BY_EXT_ID.format(pub_id)))
                        r=requests.get(self._QUERY_BY_EXT_ID.format(pub_id))
                        r.raise_for_status()
                        result = r.json()['resultList']['result'][0]
                        logging.debug("Publication data --- {}" .format(result))
                        pub = Publication(pub_id=pub_id,
                                      title=result['title'],
                                      abstract=result['abstractText'],
                                      authors=result['authorList'],
                                      year=int(result['pubYear']),
                                      date=result["firstPublicationDate"],
                                      journal=result['journalInfo'],
                                      full_text=u"",
                                      full_text_url=result['fullTextUrlList']['fullTextUrl'],
                                      #epmc_keywords=result['keywordList']['keyword'],
                                      doi=result['doi'],
                                      cited_by=result['citedByCount'],
                                      has_text_mined_terms=result['hasTextMinedTerms'] == u'Y',
                                      has_references=result['hasReferences'] == u'Y',
                                      is_open_access=result['isOpenAccess'] == u'Y',
                                      pub_type=result['pubTypeList']['pubType'],
                                      )
                        if 'meshHeadingList' in result:
                            pub.mesh_headings = result['meshHeadingList']['meshHeading']
                        if 'chemicalList' in result:
                            pub.chemicals = result['chemicalList']['chemical']
                        if 'dateOfRevision' in result:
                            pub.date_of_revision = result["dateOfRevision"]

                        if pub.has_text_mined_entities:
                            self.get_epmc_text_mined_entities(pub)
                        if pub.has_references:
                            self.get_epmc_ref_list(pub)

                        self.loader.put(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                    Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                                    pub_id,
                                    pub.to_json(),
                                    )
                        pubs[pub.pub_id] = pub
                self.loader.flush()
        except Exception, error:
            logging.info("Error in retrieving publication data for pmid {} ".format(pub_ids))
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
            logging.info("Error in loading analysed publication for pmid {}".format(pub_id))
            if error:
                logging.info(str(error))
            else:
                logging.info(Exception.message)


class PubmedLiteratureParser(object):

    def parse_medline_xml(self):
        path_xml = pp.list_xml_path('/Users/pwankar/git/data_pipeline/tests/resources')  # list all xml paths under directory
        pubmed_dict = pp.parse_medline_xml(path_xml[0])  # dictionary output
        with open('outfile.txt', 'w') as handle:
            json.dump(pubmed_dict, handle)

    def iter_parse_medline_xml(self):
        path_xml = pp.list_xml_path(
            '/Users/pwankar/git/data_pipeline/tests/resources')  # list all xml paths under directory
        context = etree.iterparse(path_xml[0], events=('end',))#, tag='MedlineCitation')
        publication_list = []
        with open('outfile1.txt', 'w') as handle:
            for event, elem in context:
                if elem.tag == 'PMID':
                    pmid = ''
                    mesh_terms = ''
                    article_list = []
                    keywords = ''
                    publication = {}
                    pmid = elem.text
                    publication['pmid'] = pmid
                if elem.tag == 'MeshHeadingList':
                    mesh_terms = self.parse_mesh_terms(elem)
                    publication['mesh_terms'] = mesh_terms
                if elem.tag == 'KeywordList':
                    keywords = self.parse_keywords(elem)
                    publication['keywords'] = keywords
                if elem.tag == 'ChemicalList':
                    chemicals = self.parse_chemicals(elem)
                    publication['chemicals'] = chemicals
                if elem.tag == 'Article' :
                    article =  self.parse_article_info(elem)
                    publication['article'] = article
                    elem.clear()  # discard the element
                    while elem.getprevious() is not None:
                        del elem.getparent()[0]
                    simplejson.dump(publication, handle)



        #del context




    def stringify_children(self,node):

        parts = ([node.text] +
                 list(chain(*([c.text, c.tail] for c in node.getchildren()))) +
                 [node.tail])
        return ''.join(filter(None, parts))

    def parse_mesh_terms(self,element):
        mesh_terms = []
        for descriptor in element.findall('DescriptorName'):
            mesh_terms.append(descriptor.text)

        return mesh_terms

    def parse_keywords(self,element):

        keywords = list()

        for k in element.findall('Keyword'):
            keywords.append(k.text)
        keywords = '; '.join(keywords)
        return keywords

    def parse_chemicals(self,element):
        chemicals = []
        for chemical in element.findall('Chemical'):
            chemical_data = {}
            chemical_data['name'] = chemical.find('NameOfSubstance').text
            chemical_data['registryNumber'] = chemical.find('RegistryNumber').text
            chemicals.append(chemical_data)
        return chemicals



    def parse_article_info(self,element):
        article = {}


        if element.find('ArticleTitle') is not None:
            title = self.stringify_children(element.find('ArticleTitle'))
        else:
            title = ''

        if element.find('Abstract') is not None:
            abstract = self.stringify_children(element.find('Abstract'))
        else:
            abstract = ''

        if element.find('AuthorList') is not None:
            authors = element.find('AuthorList').getchildren()
            authors_info = list()
            affiliations_info = list()
            for author in authors:
                if author.find('Initials') is not None:
                    firstname = author.find('Initials').text
                else:
                    firstname = ''
                if author.find('LastName') is not None:
                    lastname = author.find('LastName').text
                else:
                    lastname = ''
                if author.find('AffiliationInfo/Affiliation') is not None:
                    affiliation = author.find('AffiliationInfo/Affiliation').text
                else:
                    affiliation = ''
                authors_info.append(firstname + ' ' + lastname)
                affiliations_info.append(affiliation)
            affiliations_info = ' '.join([a for a in affiliations_info if a is not ''])
            authors_info = '; '.join(authors_info)
        else:
            affiliations_info = ''
            authors_info = ''

        journal = element.find('Journal')
        journal_name = ' '.join(journal.xpath('Title/text()'))
        #pubdate = date_extractor(journal, year_info_only)

        article['title']= title
        article['abstract'] = abstract
        article['journal'] = journal_name
        article['authors'] = authors_info


        return article



