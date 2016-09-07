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
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from settings import Config


class LiteratureActions(Actions):
    FETCH='fetch'
    PROCESS= 'process'

parser = English()

# A custom stoplist
STOPLIST = set(stopwords.words('english') + ["n't", "'s", "'m", "ca"] + list(ENGLISH_STOP_WORDS))
ALLOWED_STOPLIST=set(('non'))
STOPLIST = STOPLIST-ALLOWED_STOPLIST
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
        print "getting pub id ", pub_ids
        pubs ={}
        for pub_source in self.es_query.get_publications_by_id(pub_ids):
            print 'got pub %s from cache'%pub_source['pub_id']
            pub = Publication()
            pub.load_json(pub_source)
            pubs[pub.pub_id]=pub
        if len(pubs)<pub_ids:
            for pub_id in pub_ids:
                if pub_id not in pubs:
                    print 'getting pub from remote', self._QUERY_BY_EXT_ID.format(pub_id)
                    r=requests.get(self._QUERY_BY_EXT_ID.format(pub_id))
                    r.raise_for_status()
                    result = r.json()['resultList']['result'][0]
                    pub = Publication(pub_id=pub_id,
                                      title=result['title'],
                                      abstract=result['abstractText'],
                                      authors=result['authorList'],
                                      year=int(result['pubYear']),
                                      date=result["firstPublicationDate"],
                                      journal=result['journalInfo'],
                                      full_text=u"",
                                      full_text_url=result['fullTextUrlList']['fullTextUrl'],
                                      epmc_keywords=result['keywordList']['keyword'],
                                      doi=result['doi'],
                                      cited_by=result['citedByCount'],
                                      has_text_mined_terms=result['hasTextMinedTerms'] == u'Y',
                                      has_references=result['hasReferences'] == u'Y',
                                      is_open_access=result['isOpenAccess'] == u'Y',
                                      pub_type=result['pubTypeList']['pubType'],
                                      )
                    if 'meshHeadingList' in result:
                        pub. mesh_headings = result['meshHeadingList']['meshHeading']
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
                    pubs[pub.pub_id]=pub
            self.loader.flush()
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


class Publication(JSONSerializable):

    def __init__(self,
                 pub_id = u"",
                 title = u"",
                 abstract = u"",
                 authors = [],
                 year = None,
                 date = u"",
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
                 date_of_revision=u"",
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
                 lemmas={},
                 noun_chunks={},
                 analysed_sentences_count=1,
                 ):
        super(PublicationAnalysisSpacy, self).__init__(pub_id)
        self.lemmas = []
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


    def analyse_publication(self, pub_id, pub = None):

        if pub is None:
            pub = self.fetcher.get_publication(pub_id=pub_id)
        text_to_parse = unicode(pub.title + ' ' + pub.abstract)

        lemmas, noun_chunks, analysed_sentences_count = self._spacy_analyser(text_to_parse)

        analysed_pub = PublicationAnalysisSpacy(pub_id = pub_id,
                                        lemmas=lemmas,
                                        noun_chunks=noun_chunks,
                                        analysed_sentences_count=analysed_sentences_count)

        return analysed_pub


    def _spacy_analyser(self, abstract):
        print "analysing this text: \n%s"%abstract
        # #TODO: see PriYanka notebook tests, and/or code below
        print('*' * 80)
        pprint(abstract)
        lemmas, tokens, parsedEx = self.tokenizeText(abstract)
        parsed_vector = self.transform_doc(parsedEx)
        tl = abstract.lower()
        sents_count = len(list(parsedEx.sents))
        ec = Counter()
        #    print('ENTITIES:')
        for e in parsedEx.ents:
            e_str = u' '.join(t.orth_ for t in e).encode('utf-8').lower()
            if ((not e.label_) or (e.label_ == u'ENT')) and not (e_str in STOPLIST) and not (e_str in SYMBOLS):
                if e_str not in ec:
                    try:
                        ec[e_str] += tl.count(e_str)
                    except:
                        print(e_str)
                        #            print( e_str, e_str in STOPLIST)
                        #        print (e.label, repr(e.label_),  ' '.join(t.orth_ for t in e))
        print('FILTERED NOUN CHUNKS:')
        for k, v in ec.most_common(5):
            print k, round(float(v) / sents_count, 3)

        return lemmas, ec, sents_count


    # A custom function to tokenize the text using spaCy
    # and convert to lemmas
    def tokenizeText(self, sample):
        # get the tokens using spaCy
        tokens_all = parser(unicode(sample))
        #    for t in tokens_all.noun_chunks:
        #        print(t, list(t.subtree))
        # lemmatize
        lemmas = []
        for tok in tokens_all:
            lemmas.append(tok.lemma_.lower().strip() if tok.lemma_ != "-PRON-" else tok.lower_)
        tokens = lemmas

        # stoplist the tokens
        tokens = [tok for tok in tokens if tok.encode('utf-8') not in STOPLIST]

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
        print 'COMMON LEMMAS'
        for i in c.most_common(5):
            if i[1] > 1:
                print i[0], round(float(i[1]) / sents_count, 3)
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


class Literature(object):


    def __init__(self,
                 es,
                 loader,
                 ):
        self.es = es
        self.es_query=ESQuery(es)
        self.loader = loader
        self.logger = logging.getLogger(__name__)


    def fetch(self,
              datasources=[],
              ):
        if not self.es.indices.exists(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME):
            self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)

        #TODO: load everything with a fetcher in parallel
        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)
        fetched_pub_ids=set()
        for ev in tqdm(self.es_query.get_all_pub_ids_from_evidence(),
                    desc='Reading available evidence_strings',
                    total = self.es_query.count_validated_evidence_strings(datasources= datasources),
                    unit=' evidence',
                    unit_scale=True):
            pub_id= self.get_pub_id_from_url(ev)
            if pub_id not in fetched_pub_ids:
                pubs = pub_fetcher.get_publication(pub_id)
                fetched_pub_ids.add(pub_id)

                #todo remove this, just for debugging
                for pid, pub in pubs.items():
                    print pid, pub.title

    def process(self,
                datasources=[],
                ):
        #TODO: process everything with an analyser in parallel


        # for t in [text, text2, text3, text4, text5, text6]:
        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)
        pub_analyser = PublicationAnalyserSpacy(pub_fetcher, self.es, self.loader)
        for ev in tqdm(self.es_query.get_all_pub_ids_from_evidence(),
                    desc='Reading available evidence_strings',
                    total = self.es_query.count_validated_evidence_strings(datasources= datasources),
                    unit=' evidence',
                    unit_scale=True):
            pub_id= self.get_pub_id_from_url(ev)
            pubs = pub_fetcher.get_publication(pub_id)
            for pub_id, pub in pubs.items():
                spacy_analysed_pub = pub_analyser.analyse_publication(pub_id=pub_id,
                                                                      pub = pub)
                self.loader.put(index_name=Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                doc_type=spacy_analysed_pub.get_type(),
                                ID=pub_id,
                                body=spacy_analysed_pub.to_json(),
                                parent=pub_id,
                                )
        self.loader.flush()

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

