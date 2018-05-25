#!/usr/local/bin/python
# -*- coding: UTF-8 -*-

'''DEPRECATED - the intention is to remove this code from the pipeline
'''


import logging
import requests
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)

MAX_PUBLICATION_CHUNKS =100

class LiteratureActions(Actions):
    FETCH='fetch'
    UPDATE = 'update'


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
                pubs[pub.id] = pub

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

    def load_json(self, data):
        super(Publication, self).load_json(data)
        if hasattr(self, 'pub_id'):
            self.id = self.pub_id
            del self.__dict__["pub_id"]

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




