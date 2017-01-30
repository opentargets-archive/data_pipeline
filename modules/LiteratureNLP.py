#!/usr/local/bin/python
# coding: latin-1
import logging
import multiprocessing
import re
import string
import time
import json
from collections import Counter
import os

from nltk.corpus import stopwords
from sklearn.base import TransformerMixin
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
from spacy.en import English
from tqdm import tqdm

from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue, RedisQueueWorkerProcess
from modules.Literature import PublicationFetcher
from settings import Config
from lxml import etree
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher
from spacy.tokenizer import Tokenizer
from spacy.language_data import TOKENIZER_INFIXES
import spacy.util

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

SUBJECTS = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl"]
OBJECTS = ["dobj", "dative", "attr", "oprd"]

MAX_CHUNKS =100
MAX_TERM_FREQ = 200000

class LiteratureNLPProcess(object):


    def __init__(self,
                 es = None,
                 loader = None,
                 r_server=None,
                 ):
        self.es = es
        self.es_query = ESQuery(self.es)
        if loader is None:
            loader = Loader(es)
        self.loader = loader
        self.r_server = r_server
        self.logger = logging.getLogger(__name__)
        if not self.es.indices.exists(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME):
            self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)


    def process(self,
                datasources=[],
                dry_run=False):

        #todo, add method to process all cached publications??

        # Literature Queue
        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()

        literature_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|literature_analyzer_q',
                                  max_size=MAX_CHUNKS*no_of_workers,
                                  job_timeout=120)


        # Start literature-analyser-worker processes
        analyzers = [LiteratureAnalyzerProcess(literature_q,
                                               self.r_server.db,
                                               dry_run,
                                               ) for i in range(no_of_workers)]

        for a in analyzers:
            a.start()

        pub_fetcher = PublicationFetcher(self.es, loader=self.loader)

        for pub_id in tqdm(self.es_query.get_all_pub_ids_from_validated_evidence(datasources= datasources),
                    desc='Reading available evidence_strings to analyse publications',
                    total = self.es_query.count_validated_evidence_strings(datasources= datasources),
                    unit=' evidence',
                    unit_scale=True):
            pubs = pub_fetcher.get_publication(pub_id)
            if pubs:
                literature_q.put(pubs)
        literature_q.set_submission_finished(r_server=self.r_server)

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


class PublicationAnalyserSpacy(object):
    def __init__(self, fetcher, dry_run=False):

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
            text_to_parse = unicode(pub.title + ' ' + ''.join(pub.abstract))
            lemmas, noun_chunks, analysed_sentences_count = self._spacy_analyser(text_to_parse)
            lemmas= tuple({'value':k, "count":v} for k,v in lemmas.items())
            noun_chunks= tuple({'value':k, "count":v} for k,v in noun_chunks.items())
            analysed_pub = PublicationAnalysisSpacy(pub_id = pub_id,
                                        lemmas=lemmas,
                                        noun_chunks=noun_chunks,
                                        analysed_sentences_count=analysed_sentences_count)

        return analysed_pub


    def _spacy_analyser(self, abstract):

        lemmas, tokens, parsedEx = self.tokenizeText(abstract)
        parsed_vector = self.transform_doc(parsedEx)
        tl = abstract.lower()
        sents_count = len(list(parsedEx.sents))
        ec = Counter()

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
            if tok.lemma_.lower().strip() in tokens and tok.pos_ in ['PROP', 'PROPN', 'NOUN', 'ORG', 'FCA', 'PERSON', ]:
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



class LiteratureAnalyzerProcess(RedisQueueWorkerProcess):
    def __init__(self,
                 queue_in,
                 redis_path,
                 chunk_size = 1e4,
                 dry_run=False
                 ):
        super(LiteratureAnalyzerProcess, self).__init__(queue_in, redis_path)
        self.loader = Loader(chunk_size=10000, dry_run=dry_run)
        self.start_time = time.time()
        self.audit = list()
        self.logger = logging.getLogger(__name__)
        pub_fetcher = PublicationFetcher()
        self.chunk_size = chunk_size
        self.dry_run = dry_run
        self.pub_analyser = PublicationAnalyserSpacy(pub_fetcher)

    def process(self, data):
        publications = data
        logging.debug("In LiteratureAnalyzerProcess- {} ".format(self.name))
        try:

            for pub_id, pub in publications.items():
                spacy_analysed_pub = self.pub_analyser.analyse_publication(pub_id=pub_id,
                                                                       pub=pub)

                if spacy_analysed_pub:

                    self.loader.put(index_name=Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                        doc_type=spacy_analysed_pub.get_type(),
                        ID=pub_id,
                        body=spacy_analysed_pub.to_json(),
                        parent=pub_id,
                        )

        except Exception as e:
            logging.error("Error in loading analysed publication for pmid {}: {}".format(pub_id, e.message))



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


class LiteratureInfoExtractor(object):

    def __init__(self,
                 es = None,
                 loader = None,
                 r_server=None,
                 ):
        self.es = es
        self.es_query = ESQuery(self.es)
        if loader is None:
            loader = Loader(es)
        self.loader = loader
        self.r_server = r_server
        self.logger = logging.getLogger(__name__)


    def process(self,
                datasources=None,
                dry_run=False):


        if not os.path.isfile(Config.GENE_LEXICON_JSON_LOCN):
            logging.info('Generating gene matcher patterns')
            gene_lexicon_parser = LexiconParser(Config.BIOLEXICON_GENE_XML_LOCN,Config.GENE_LEXICON_JSON_LOCN,'GENE')
            gene_lexicon_parser.parse_lexicon()

        if not os.path.isfile(Config.DISEASE_LEXICON_JSON_LOCN):
            logging.info('Generating disease matcher patterns')
            disease_lexicon_parser = LexiconParser(Config.BIOLEXICON_DISEASE_XML_LOCN, Config.DISEASE_LEXICON_JSON_LOCN, 'DISEASE')
            disease_lexicon_parser.parse_lexicon()

        spacyManager = NLPManager()
        i=1
        j=0

        logging.info('Loading lexicon json')
        disease_patterns = json.load(open(Config.DISEASE_LEXICON_JSON_LOCN))
        gene_patterns = json.load(open(Config.GENE_LEXICON_JSON_LOCN))


        logging.info('Finding entity matches')
        for ev in tqdm(self.es_query.get_abstracts_from_val_ev(),
                    desc='Reading available publications for nlp information extraction',
                    total = self.es_query.count_validated_evidence_strings(),
                    unit=' evidence',
                    unit_scale=True):

            j=j+1
            if ev['literature']['title'] and ev['literature']['abstract']:
                i = i + 1
                if (i > 101):
                    break

                text_to_analyze = unicode(ev['literature']['title'] + ' ' + ''.join(ev['literature']['abstract']))
                tokens = spacyManager.tokenizeText(text_to_analyze)

                disease_matches = spacyManager.findEntityMatches(disease_patterns,tokens)
                gene_matches = spacyManager.findEntityMatches(gene_patterns,tokens)
                logging.info('Text to analyze - {} --------- Gene Matches {}'.format(text_to_analyze,gene_matches))

                #spacyManager.generateRelations(disease_matches,gene_matches)


        logging.info("Total abstracts {}".format(j))
        logging.info("DONE")

    @staticmethod
    def get_pub_id_from_url(url):
        return url.split('/')[-1]


''' base class to parse biolexicon xml files and create '''
class LexiconParser(object):

    def __init__(self,
                 lexicon_xml,
                 matcher_json,
                 lexicon_type
                 ):
        self.lexicon_xml = lexicon_xml
        self.matcher_json = matcher_json
        self.lexicon_type = lexicon_type
        self.lexicon = {}
        self.logger = logging.getLogger(__name__)

    def parse_lexicon(self):
        nlp = English()
        matcher = Matcher(vocab=nlp.vocab, )

        '''parse a list of genes from biolexicon'''

        #TODO - clear root elements to avoid memory issues while parsing
        context = etree.iterparse(open(self.lexicon_xml),
                                  # context = etree.iterparse(open("resources/test-spacy/geneProt.xml"),
                                  tag='Cluster')  # requries geneProt.xml from LexEBI
        # ftp://ftp.ebi.ac.uk/pub/software/textmining/bootstrep/termrepository/LexEBI/


        encoded_lexicon = {}
        encoded_lexicon_json_file = self.matcher_json
        for item in self.retreive_items_from_lexicon_xml(context,self.lexicon_type):
            if item.all_forms:
                matcher.add_entity(
                    item.id,  # Entity ID -- Helps you act on the match.
                    {"ent_type": item.ent_type, "label": item.label},  # Arbitrary attributes (optional)
                    on_match=self.print_matched_entities,
                    if_exists='ignore',
                )
                for form in item.all_forms:
                    token_specs = [{ORTH: token} for token in form.split()]
                    matcher.add_pattern(item.id,
                                        token_specs,
                                        label=form,
                                        )


                encoded_lexicon.update(item.to_dict())

        json.dump(encoded_lexicon,
                  open(encoded_lexicon_json_file, 'w'),
                  indent=4)

    def retreive_items_from_lexicon_xml(self, context, entity_type):
        c = 0

        for action, cluster in context:
            c += 1
            cluster_name = cluster.attrib['clsId']
            if 'UN13C' in cluster_name:
                self.logger.info('Found {} !!'.format(cluster_name))

            item = LexiconItem(cluster.attrib['clsId'], ent_type=entity_type)
            for entry in cluster.iterchildren(tag='Entry'):
                if entry.attrib['baseForm']:
                    if int(entry.attrib['mlfreq']) < MAX_TERM_FREQ:
                        if not item.label and entry.attrib['baseForm']:
                            item.label = entry.attrib['baseForm']
                        item.add_variant(entry.attrib['baseForm'],
                                         entry.attrib['mlfreq'])
                '''Synonyms'''
                for variant in entry.iterchildren(tag='Variant'):
                    if int(variant.attrib['mlfreq']) < MAX_TERM_FREQ:
                        item.add_variant(variant.attrib['writtenForm'],
                                         variant.attrib['mlfreq'])

            self.lexicon[item.id] = item
            if c % 10000 == 0:
                logging.info('parsed %i lexicon term' % c)
            yield item

    def print_matched_entities(self, matcher, doc, i, matches):
        ''' callback '''
        # '''all matches'''
        # spans = [(matcher.get_entity(ent_id), label, doc[start : end]) for ent_id, label, start, end in matches]
        # for span in spans:
        #     print span
        '''just the matched one'''
        ent_id, label, start, end = matches[i]
        print i
        span = (matcher.get_entity(ent_id), label, doc[start: end])
        print span



class LexiconItem(object):
    def __init__(self, id, label = None, ent_type ='ENT'):
        self.id = id
        self.label = label
        self.ent_type = ent_type
        '''variants identify synonymns'''
        self.variants = []
        self.all_forms = set()

    def add_variant(self, term, freq):
        self.variants.append(dict(term=term,
                                  freq=freq))
        self.all_forms.add(term)

    def __str__(self):
        string = ['ID: %s\nBASE:  %s'%(self.id,self.label)]
        for i in self.variants:
            string.append('VARIANT: %s'%i['term'])
        return '\n'.join(string)

    def to_dict(self):
        d = {self.id : [self.ent_type,
                        {"label" : self.label},
                        [[{'ORTH': token} for token in form.split()] for form in self.all_forms]]
             }
        return d

class NLPManager(object):
    def __init__(self, nlp=None):
        self.nlp = nlp

    def tokenizeText(self,text):

        def create_tokenizer(nlp):
            infix_re = spacy.util.compile_infix_regex(tuple(TOKENIZER_INFIXES + ['/',',']))
            return Tokenizer(nlp.vocab,{},nlp.tokenizer.prefix_search,nlp.tokenizer.suffix_search,
                             infix_re.finditer)

        if self.nlp is None:
            self.nlp = spacy.load('en', create_make_doc=create_tokenizer)
        custom_tokens = self.nlp(unicode(text))
        return custom_tokens


    def findEntityMatches(self,patterns, tokens):
        entities = []
        matcher = Matcher(vocab=self.nlp.vocab,
                          patterns=patterns
                          )

        matches = matcher(tokens)

        for ent_id, label, start, end in matches:
            '''doc[start: end] - actual match in document; could be a synonymn'''
            span = (matcher.get_entity(ent_id), label, tokens[start: end])
            entities.append(span)
        return entities

    def generateRelations(self,entity1,entity2,doc):
        self.extract_entity_relations_by_verb(entity1,entity2,doc)

    def getSubject(self,predicate):
        subjects = [tok for tok in predicate.lefts if tok.dep_ in SUBJECTS and tok.pos_ != "DET"]
        return subjects

    def getObject(self,predicate):
        rights = list(predicate.rights)
        objects = []
        for token in rights:
            if token.dep_ in OBJECTS:
                objects.append(token)
            elif token.dep_ == 'prep':
                rts = list(token.rights)
                objects.append(rts[0])



        #objects = [tok for tok in rights if tok.dep_ in OBJECTS ]
        return objects

    def extract_entity_relations_by_verb(self,gene_matches,disease_matches,doc):
        over_simplified_text = 'Studies have identified that ADRA1A contributes' \
                               ' to schizophrenia'
        context = ''

        relations = []
        #TODO - handle aux verbs?????
        # Auxiliary Verb List : be(am, are, is, was, were, being),can,could,do(did, does, doing),have(had, has, having),may,might,must,shall,should,will,would
        for predicate in filter(lambda w: w.pos_ == 'VERB', doc):
            subject = self.getSubject(predicate)
            object = self.getObject(predicate)
            for child in predicate.children:
                if child.dep_ == 'neg':
                    context = 'Negative'

            print 'Subject {}  Predicate {} {}  Object {}'.format(subject,context , predicate,object)







