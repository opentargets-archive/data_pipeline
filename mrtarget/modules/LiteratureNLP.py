#!/usr/local/bin/python
# -*- coding: UTF-8 -*-
import logging
import multiprocessing
import re
import string
import time
import json
from collections import Counter
import os

from mrtarget.common.NLP import init_spacy_english_language, DOMAIN_STOP_WORDS
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
import spacy
from spacy.en import English
from spacy.symbols import *
from spacy.tokens import Span
from spacy.tokens.doc import Doc
from textblob.en.inflect import singularize
from tqdm import tqdm

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisQueue, RedisQueueWorkerProcess
from mrtarget.modules.Literature import PublicationFetcher
from mrtarget.Settings import Config
from mrtarget.common.AbbreviationFinder import AbbreviationsParser

from textblob import TextBlob
from textblob.base import BaseNPExtractor
from textblob.decorators import requires_nltk_corpus
from textblob.en.np_extractors import _normalize_tags
from unidecode import unidecode

try:
    import nltk
    from nltk.corpus import stopwords as nltk_stopwords
    from nltk import Tree, pprint
except ImportError:
    logging.getLogger(__name__).warning('cannot import nltk or stopwords')

SUBJECTS = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl", "meta"]
OBJECTS = ["dobj", "dative", "attr", "oprd", "pobj", "attr", "conj", "compound"]

ANY_NOUN = SUBJECTS + OBJECTS + ['compound']
from lxml import etree
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher

import spacy.util

# List of symbols we don't care about
SYMBOLS = " ".join(string.punctuation).split(" ") + ["-----", "---", "...", "â", "â", "'ve"]

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
MAX_CHUNKS =100
MAX_TERM_FREQ = 200000


class AbstractNormalizer(object):

    greek_alphabet = {
        u'\u0391': 'Alpha',
        u'\u0392': 'Beta',
        u'\u0393': 'Gamma',
        u'\u0394': 'Delta',
        u'\u0395': 'Epsilon',
        u'\u0396': 'Zeta',
        u'\u0397': 'Eta',
        u'\u0398': 'Theta',
        u'\u0399': 'Iota',
        u'\u039A': 'Kappa',
        u'\u039B': 'Lamda',
        u'\u039C': 'Mu',
        u'\u039D': 'Nu',
        u'\u039E': 'Xi',
        u'\u039F': 'Omicron',
        u'\u03A0': 'Pi',
        u'\u03A1': 'Rho',
        u'\u03A3': 'Sigma',
        u'\u03A4': 'Tau',
        u'\u03A5': 'Upsilon',
        u'\u03A6': 'Phi',
        u'\u03A7': 'Chi',
        u'\u03A8': 'Psi',
        u'\u03A9': 'Omega',
        u'\u03B1': 'alpha',
        u'\u03B2': 'beta',
        u'\u03B3': 'gamma',
        u'\u03B4': 'delta',
        u'\u03B5': 'epsilon',
        u'\u03B6': 'zeta',
        u'\u03B7': 'eta',
        u'\u03B8': 'theta',
        u'\u03B9': 'iota',
        u'\u03BA': 'kappa',
        u'\u03BB': 'lamda',
        u'\u03BC': 'mu',
        u'\u03BD': 'nu',
        u'\u03BE': 'xi',
        u'\u03BF': 'omicron',
        u'\u03C0': 'pi',
        u'\u03C1': 'rho',
        u'\u03C3': 'sigma',
        u'\u03C4': 'tau',
        u'\u03C5': 'upsilon',
        u'\u03C6': 'phi',
        u'\u03C7': 'chi',
        u'\u03C8': 'psi',
        u'\u03C9': 'omega',
    }

    def normalize(self, text):
        for key in self.greek_alphabet:
            text = text.replace(key, self.greek_alphabet[key])
        return unidecode(text)

class PerceptronNPExtractor(BaseNPExtractor):
    '''modified from 

    http://thetokenizer.com/2013/05/09/efficient-way-to-extract-the-main-topics-of-a-sentence/
    
    To use Perceptron POS Tagger (more accurate)
    '''

    CFG = {
        ('NNP', 'NNP'): 'NNP',
        ('NNP', 'NN'): 'NNP',
        ('NN', 'NNS'): 'NNP',
        ('NNP', 'PO'): 'NNP',
        ('NN', 'IN'): 'NN',
        ('IN', 'JJ'): 'NN',
        # ('PO', 'NN'): 'NNP',
        ('NN', 'NN'): 'NNI',
        ('NNI', 'NN'): 'NNI',
        ('JJ', 'JJ'): 'JJ',
        ('JJ', 'NN'): 'NNI',
        ('NN', 'JJ'): 'NNI',

    }

    def __init__(self):
        self._trained = False

    @requires_nltk_corpus
    def train(self):
        # train_data = nltk.corpus.brown.tagged_sents(categories=['news','science_fiction'])
        self.tagger = nltk.PerceptronTagger()
        self._trained = True
        return None

    def _tokenize_sentence(self, sentence):
        '''Split the sentence into single words/tokens'''
        tokens = nltk.word_tokenize(sentence)
        return tokens

    def extract(self, sentence):
        '''Return a list of noun phrases (strings) for body of text.'''
        if not self._trained:
            self.train()
        tokens = self._tokenize_sentence(sentence)
        tagged = self.tagger.tag(tokens)
        # print tagged
        tags = _normalize_tags(tagged)
        # print tags
        merge = True
        while merge:
            merge = False
            for x in range(0, len(tags) - 1):
                t1 = tags[x]
                t2 = tags[x + 1]
                key = t1[1], t2[1]
                # print t1, t2, key
                value = self.CFG.get(key, '')
                if value:
                    merge = True
                    tags.pop(x)
                    tags.pop(x)
                    if t2[0][0].isalnum():
                        match = '%s %s' % (t1[0], t2[0])
                    else:
                        match = '%s%s' % (t1[0], t2[0])
                    pos = value
                    tags.insert(x, (match, pos))
                    break

        matches = [t[0] for t in tags if t[1] in ['NNP', 'NNI','NN']]
        # print matches
        return matches

class NounChuncker(object):

    def __init__(self):
        self.np_ex = PerceptronNPExtractor()
        self.normalizer = AbstractNormalizer()
        self.abbreviations_finder = AbbreviationsParser()

    def digest(self, text):
        from pprint import pprint
        normalized = self.normalizer.normalize(text)
        parsed = TextBlob(normalized, np_extractor=self.np_ex)
        counted_noun_phrases = parsed.noun_phrases
        abbreviations = self.abbreviations_finder.digest(parsed)
        '''make sure defined acronym are used as noun phrases '''
        for abbr in abbreviations:
            if abbr['long'].lower() not in counted_noun_phrases:
                counted_noun_phrases.append(abbr['long'].lower())
        '''improved singularisation still needs refinement'''
        # singular_counted_noun_phrases = []
        # for np in counted_noun_phrases:
        #     if not (np.endswith('sis') or np.endswith('ess')):
        #         singular_counted_noun_phrases.append(singularize(np))
        #     else:
        #         singular_counted_noun_phrases.append(np)
        # singular_counted_noun_phrases = Counter(singular_counted_noun_phrases)
        counted_noun_phrases = Counter(counted_noun_phrases)
        base_noun_phrases = counted_noun_phrases.keys()
        '''remove plurals with appended s'''
        for np in counted_noun_phrases.keys():
            if np+'s' in counted_noun_phrases:
                counted_noun_phrases[np]+=counted_noun_phrases[np+'s']
                del counted_noun_phrases[np+'s']
        '''increase count of shorter form with longer form'''
        for abbr in abbreviations:
            short = abbr['short'].lower()
            if short in counted_noun_phrases:
                counted_noun_phrases[abbr['long'].lower()] += counted_noun_phrases[short]
                del counted_noun_phrases[short]


        #count substrings occurrences as well
        for k in counted_noun_phrases:
            for s in counted_noun_phrases:
                if k != s and k in s:
                    counted_noun_phrases[k] += 1
        return dict(chunks = base_noun_phrases,
                    recurring_chunks = [i for i,k in counted_noun_phrases.items() if k >1],
                    top_chunks = [i[0] for i in counted_noun_phrases.most_common(5) if i[1]>1],
                    abbreviations = abbreviations)

    def __str__(self):
        return 'noun_phrases'


class LiteratureNLPActions(Actions):

    PROCESS= 'process'


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


class PublicationAnalysisSpacy(object):
    def __init__(self, fetcher, dry_run=False):

        self.fetcher = fetcher
        self.logger = logging.getLogger(__name__)
        self.parser = English()
        # A custom stoplist
        STOPLIST = set(nltk_stopwords.words('english') + ["n't", "'s", "'m", "ca","p","t"] + list(ENGLISH_STOP_WORDS))
        ALLOWED_STOPLIST=set(('non'))
        self.STOPLIST = STOPLIST - ALLOWED_STOPLIST


    def analyse_publication(self, pub_id, pub = None):

        if pub is None:
            pub = self.fetcher.get_publication(pub_ids=pub_id)
        analysed_pub = None
        if pub.title or pub.abstract:
            text_to_parse = pub.get_text_to_analyze()
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
        self.pub_analyser = PublicationAnalysisSpacy(pub_fetcher)

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
        self.nlp = init_spacy_english_language()


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

        i = 1
        j = 0
        spacyManager = NLPManager(self.nlp)
        logging.info('Loading lexicon json')
        disease_patterns = json.load(open(Config.DISEASE_LEXICON_JSON_LOCN))
        gene_patterns = json.load(open(Config.GENE_LEXICON_JSON_LOCN))

        # matcher = Matcher.load(path = 'gene_lexicon.json',
        #                        vocab=nlp.vocab)
        # TODO: FIXED IN SPACY MASTER BUT A BUG IN 1.5.0
        logging.info('Generating matcher patterns')
        '''load all the new patterns, do not use Matcher.load since there is a bug'''
        disease_matcher = Matcher(vocab=spacyManager.nlp.vocab,
                          patterns=disease_patterns
                          )
        gene_matcher = Matcher(vocab=spacyManager.nlp.vocab,
                                  patterns=gene_patterns
                                  )
        '''should save the new vocab now'''
        # Matcher.vocab.dump('lexeme.bin')

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

                disease_matches = spacyManager.findEntityMatches(disease_matcher,tokens)
                gene_matches = spacyManager.findEntityMatches(gene_matcher,tokens)
                logging.info('Text to analyze - {} --------- Gene Matches {} -------- Disease Matches {}'.format(text_to_analyze,gene_matches, disease_matches))

                #spacyManager.generateRelations(disease_matches,gene_matches)
        logging.info("DONE")



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
                # matcher.add_entity(
                #     item.id,  # Entity ID -- Helps you act on the match.
                #     {"ent_type": item.ent_type, "label": item.label},  # Arbitrary attributes (optional)
                #     on_match=self.print_matched_entities,
                #     if_exists='ignore',
                # )
                # for form in item.all_forms:
                #     token_specs = [{ORTH: token} for token in form.split()]
                #     matcher.add_pattern(item.id,
                #                         token_specs,
                #                         label=form,
                #                         )
                encoded_lexicon.update(item.to_dict())

        json.dump(encoded_lexicon,
                  open(encoded_lexicon_json_file, 'w'),
                  indent=4)

    def retreive_items_from_lexicon_xml(self, context, entity_type):
        c = 0

        for action, cluster in context:
            c += 1

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

class LitEntity(JSONSerializable):
    def __init__(self, id, label=None, ent_type='ENT', matched_word=None, start_pos = None, end_pos = None, doc_id = None):
        self.id = id
        self.label = label
        self.ent_type = ent_type
        self.matched_word = matched_word
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.doc_id = doc_id

# This is meant only for testing purpose
def load_entity_matches(self, loader, nlp, doc):
    matcher = Matcher(vocab=nlp.vocab,
                      patterns=json.load(open('disease_lexicon.json'))
                      )
    disease_matches = matcher(doc)
    disease_matched_list = []
    logging.info('Disease Matches!!!!!!!!!!!!!!!!!!!')
    i = 1
    for ent_id, label, start, end in disease_matches:
        i = i + 1
        span = (matcher.get_entity(ent_id), label, doc[start: end])
        litentity = LitEntity(ent_id, matcher.get_entity(ent_id)['label'], 'DISEASE', doc[start: end].text, start,
                              end, 1)
        loader.put(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME,
                   Config.ELASTICSEARCH_LITERATURE_ENTITY_DOC_NAME,
                   i,
                   litentity,
                   create_index=False)

        logging.info(span)
    disease_matched_entities = {"entities": disease_matched_list}

    matcher = Matcher(vocab=nlp.vocab,
                      patterns=json.load(open('gene_lexicon.json'))
                      )
    gene_matches = matcher(doc)
    gene_matched_list = []
    logging.info('Gene Matches!!!!!!!!!!!!!!!!!!!')
    for ent_id, label, start, end in gene_matches:
        i = i + 1
        span = (matcher.get_entity(ent_id), label, doc[start: end])
        litentity = LitEntity(ent_id, matcher.get_entity(ent_id)['label'], 'GENE', doc[start: end].text, start, end,
                              1)
        loader.put(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME,
                   Config.ELASTICSEARCH_LITERATURE_ENTITY_DOC_NAME,
                   i,
                   litentity,
                   create_index=False)
        logging.info(span)
    gene_matched_entities = {"litentity": gene_matched_list}




#####TEMPRORARY KEEPING THIS FOR REFERENCE IF NEEDED##################
class NLPManager(object):
    def __init__(self, nlp):
        self.nlp = nlp

    def tokenizeText(self,text):


        custom_tokens = self.nlp(u''+text)
        return custom_tokens


    def findEntityMatches(self,matcher, tokens):
        entities = []

        matches = matcher(tokens)

        for ent_id, label, start, end in matches:
            '''doc[start: end] - actual match in document; could be a synonymn'''
            span = (matcher.get_entity(ent_id), label, tokens[start: end])
            litentity = LitEntity(ent_id, matcher.get_entity(ent_id)['label'], 'GENE', tokens[start: end].text, start, end)
            #TODO - store entities in ES index
            entities.append(litentity)
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








class DocumentAnalysisSpacy(object):

    def __init__(self,
                 nlp,
                 normalize=True,
                 stopwords=None
                 ):

        self.logger = logging.getLogger(__name__)
        self._normalizer = AbstractNormalizer()
        self._abbreviations_finder = AbbreviationsParser()

        self.normalize = normalize
        if self.normalize:
            self._normalizer = AbstractNormalizer()
        if stopwords is None:
            self.stopwords  = set(nltk_stopwords.words('english') + ["n't", "'s", "'m", "ca", "p", "t"] + list(ENGLISH_STOP_WORDS) + DOMAIN_STOP_WORDS + list(string.punctuation))

        self.nlp = nlp



    def process(self, document):

        if isinstance(document, Doc):
            doc = document
            abbreviations = self._abbreviations_finder.digest_as_dict(doc.text)
        elif isinstance(document, unicode):
            if self.normalize:
                document = u''+self._normalizer.normalize(document)
            abbreviations = self._abbreviations_finder.digest_as_dict(document)
            # self.logger.debug('abbreviations: ' + str(abbreviations))

            if abbreviations:
                for short, long in abbreviations.items():
                    if short in document and not long in document:
                        document = document.replace(short, long)
            try:
                doc = self.nlp(document)
            except:
                self.logger.exception('Error parsing the document: %s' % document)
                return [None, {}]
        else:
            raise AttributeError('document needs to be unicode or Doc not %s' % document.__class__)

        concepts = []
        noun_phrases = []
        for sentence in doc.sents:
            try:
                analysed_sentence = SentenceAnalysisSpacy(sentence.text, self.nlp)
                analysed_sentence.analyse()
                concepts.extend(analysed_sentence.concepts)
                noun_phrases.extend(analysed_sentence.noun_phrases)
            except:
                self.logger.exception('Error parsing the sentence: %s'%sentence.text)
        # print self.noun_phrases
        noun_phrases = list(set([i.text for i in noun_phrases if i.text.lower() not in self.stopwords ]))

        # clustered_np = self.cluster_np(noun_phrases)
        noun_phrase_counter = Counter()
        lowered_text = doc.text.lower()
        for i in noun_phrases:
            lowered_np = i.lower()
            noun_phrase_counter[lowered_np]= lowered_text.count(lowered_np)
        '''remove plurals with appended s'''
        for np in noun_phrase_counter.keys():
            if np + 's' in noun_phrase_counter:
                noun_phrase_counter[np] += noun_phrase_counter[np + 's']
                del noun_phrase_counter[np + 's']
        '''increase count of shorter form with longer form'''
        for short, long in abbreviations.items():
            if short.lower() in noun_phrase_counter:
                noun_phrase_counter[long.lower()] += noun_phrase_counter[short.lower()]
                del noun_phrase_counter[short.lower()]
        noun_phrases_top = [i[0] for i in noun_phrase_counter.most_common(5) if i[1] > 1]
        noun_phrases_recurring = [i for i, k in noun_phrase_counter.items() if k > 1]

        return doc, \
               dict(chunks = noun_phrases,
                    recurring_chunks = noun_phrases_recurring,
                    top_chunks = noun_phrases_top,
                    abbreviations = [dict(short=k, long=v) for k,v in abbreviations.items()],
                    concepts = concepts)

    def digest(self, document):
        return self.process(document)[1]

    def __str__(self):
        return 'nlp'

    def cluster_np(self,noun_phrases):
        '''todo: optimise for speed'''

        clusters = {i:[i] for i in noun_phrases}
        for i in noun_phrases:
            for j in noun_phrases:
                if i!=j and i in j.split(' '):
                    # print '%s -> %s'%(i,j)
                    clusters[j].extend(clusters[i])
                    del clusters[i]
                    # elif i != j and j in i:
                    #     print '%s <- %s'%(j,i)
        # pprint(clusters)
        filtered_noun_phrases = []
        for k,v in clusters.items():
            if len(v)>1:
                longest = sorted(v, key=lambda x: len(x), reverse=True)[0]
                filtered_noun_phrases.append(longest)
            else:
                filtered_noun_phrases.append(v[0])
        # print filtered_noun_phrases
        return filtered_noun_phrases




class SentenceAnalysisSpacy(object):

    def __init__(self,
                 sentence,
                 nlp,
                 abbreviations = None,
                 normalize = True):
        self.logger = logging.getLogger(__name__)
        self._normalizer = AbstractNormalizer()
        self._abbreviations_finder = AbbreviationsParser()

        if isinstance(sentence, Doc):
            self.sentence = sentence
            self.doc = sentence
        elif isinstance(sentence, Span):
            self.sentence = sentence
            self.doc = sentence.doc
        elif isinstance(sentence, unicode):
            if not sentence.replace('\n','').strip():
                raise AttributeError('sentence cannot be empty')
            if normalize:
                sentence = u'' + self._normalizer.normalize(sentence)
            if abbreviations is None:
                self.abbreviations = self._abbreviations_finder.digest_as_dict(sentence)
                # self.logger.info('abbreviations: ' + str(self.abbreviations))

            if abbreviations:
                for short, long in abbreviations:
                    if short in sentence and not long in sentence:
                        sentence = sentence.replace(short, long)
            self.sentence = nlp(sentence)
            self.doc = self.sentence
        else:
            raise AttributeError('sentence needs to be unicode or Doc or Span not %s'%sentence.__class__)
        self.logger.debug(u'Sentence to analyse: '+self.sentence.text)




    def isNegated(self,tok):
        negations = {"no", "not", "n't", "never", "none", "false"}
        for dep in list(tok.lefts) + list(tok.rights):
            if dep.lower_ in negations:
                return True

        # alternatively look for
        #     for child in predicate.children:
        #       if child.dep_ == 'neg':
        #            context = 'Negative'
        return False

    def get_alternative_subjects(self, tok):
        '''given a token, that is a subject of a verb, extends to other possible subjects in the left part of the relation'''

        '''get objects in subtree of the subject to be related to objects in the right side of the verb #risk'''
        alt_subjects = [tok]
        allowed_pos = [NOUN, PROPN]
        for sibling in tok.head.children:
            # print sibling, sibling.pos_, sibling.dep_
            if sibling.pos == allowed_pos:
                for sub_subj in sibling.subtree:
                    if sub_subj.dep_ in ANY_NOUN and sub_subj.pos in allowed_pos:  # should check here that there is an association betweej the subj and these
                        # objects
                        alt_subjects.append(sub_subj)
                        # alt_subjects.append(parsed[sub_subj.left_edge.i : sub_subj.right_edge.i + 1].text.lower())
                '''get other subjects conjuncted to main on to be related to objects in the right side of the verb #risk'''
                for sub_subj in sibling.conjuncts:
                    # print sub_subj, sub_subj.pos_, sub_subj.dep_
                    if sub_subj.dep_ in SUBJECTS and sub_subj.pos in allowed_pos:  # should check here that there is an association betweej the
                        # subj and these objects
                        alt_subjects.append(sub_subj)
        # print alt_subjects

        # sys.exit()
        return alt_subjects


    def get_extended_verb(self, v):
        '''
        given a verb build a representative chain of verbs for the sintax tree
        :param v: 
        :return: 
        '''
        verb_modifiers = [prep,agent]
        verb_path = [v]
        verb_text = v.lemma_.lower()
        allowed_lefts_pos = [NOUN, PROPN]
        lefts = list([i for i in v.lefts if i.pos in allowed_lefts_pos and i.dep_ in SUBJECTS])
        '''get anchestor verbs if any'''
        if not v.dep_ == 'ROOT':
            for av in [i for i in v.ancestors if i.pos == VERB and i.dep not in (aux, auxpass)]:
                verb_text = av.lemma_.lower() + ' '+  v.text.lower()
                verb_path.append(av)
                lefts.extend([i for i in av.lefts if i.pos in allowed_lefts_pos and i.dep_ in SUBJECTS])
        for vchild in v.children:
            if vchild.dep in verb_modifiers:
                verb_text += ' ' + vchild.text.lower()
        return 'to '+verb_text, verb_path, lefts

    def get_verb_path_from_ancestors(self, tok):
        '''
        given a token return the chain of verbs of his ancestors
        :param tok: 
        :return: 
        '''
        return [i for i in tok.ancestors if i.pos == VERB and i.dep != aux]

    def get_extended_token(self, tok):
        '''
        given a token find a more descriptive string extending it with its chindren
        :param tok: 
        :param doc: 
        :return: 
        '''
        allowed_pos = [NOUN, ADJ, PUNCT, PROPN]
        allowed_dep = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl", "dobj",  "attr", "oprd", "pobj",# "conj",
                       "compound", "amod", "meta", "npadvmod", "nmod", "amod"]#, add "prep" to extend for "of and "in"
        extended_tokens = [i for i in tok.subtree if (i.dep_ in allowed_dep and i in tok.children) or (i == tok)]
        # just get continous tokens
        span_range = [tok.i, tok.i]
        ext_tokens_i = [i.i for i in extended_tokens]
        max_bound = max(ext_tokens_i)
        min_bound = min(ext_tokens_i)
        curr_pos = tok.i
        for cursor in range(tok.i, max_bound+1):
            if cursor in ext_tokens_i:
                if cursor == curr_pos + 1:
                    span_range[1] = cursor
                    curr_pos = cursor

        curr_pos = tok.i
        for cursor in range(tok.i, min_bound-1, -1):
            if cursor in ext_tokens_i:
                if cursor == curr_pos - 1:
                    span_range[0] = cursor
                    curr_pos = cursor
        span= Span(self.doc, span_range[0], span_range[1]+1)
        return span


    def traverse_obj_children(self, tok, verb_path):
        '''
        iterate over all the children and the conjuncts to return objects within the same chain of verbs
        :param tok: 
        :param verb_path: 
        :return: 
        '''
        for i in tok.children:
            # print i, verb_path, get_verb_path_from_ancestors(i), get_verb_path_from_ancestors(i) ==verb_path
            if i.dep_ in OBJECTS and (self.get_verb_path_from_ancestors(i) == verb_path):
                yield i
            else:
                self.traverse_obj_children(i, verb_path)
        for i in tok.conjuncts:
            # print i, verb_path, get_verb_path_from_ancestors(i), get_verb_path_from_ancestors(i) ==verb_path
            if i.dep_ in OBJECTS and (self.get_verb_path_from_ancestors(i) == verb_path):
                yield i
            else:
                self.traverse_obj_children(i, verb_path)


    def to_nltk_tree(self, node):
        def tok_format(tok):
            return " ".join(['"%s"'%tok.orth_, tok.tag_, tok.pos_, tok.dep_])

        if node.n_lefts + node.n_rights > 0:
            return Tree(tok_format(node), [self.to_nltk_tree(child) for child in node.children])
        else:
            return tok_format(node)

    def print_syntax_tree(self):
        for t in self.sentence:
            if t.dep_== 'ROOT':
                tree = self.to_nltk_tree(t)
                if isinstance(tree, Tree):
                    tree.pretty_print(stream=self)

    def get_dependent_obj(self, tok, verb_path):
        '''
        given a token find related objects for the sape chain of verbs (verb_path
        :param tok: 
        :param verb_path: 
        :return: 
        '''
        all_descendants = []
        if tok.dep_ in OBJECTS and (self.get_verb_path_from_ancestors(tok) == verb_path):
            all_descendants.append(tok)
        for i in tok.subtree:
            all_descendants.extend(list(self.traverse_obj_children(i, verb_path)))
        descendants = list(set(all_descendants))
        return_obj = [i for i in descendants]
        return return_obj

    def print_syntax_list(self):
        output = [''
                  ]
        output.append(' | '.join(('i', 'text','pos', 'dep', 'head')))
        for k, t in enumerate(self.sentence):
            output.append(' | '.join((str(t.i), '"'+t.text+'"', t.pos_, t.dep_, t.head.text)))
        self.logger.debug('\n'.join(output))


    def collapse_noun_phrases_by_punctation(self):
        '''
        this collapse needs tobe used on a single sentence, otherwise it will ocncatenate different sentences
        :param sentence: 
        :return: 
        '''
        prev_span = ''
        open_brackets = u'( { [ <'.split()
        closed_brackets = u') } ] >'.split()
        for token in self.sentence:
            try:
                if token.text in open_brackets and token.whitespace_ ==u'' :
                    next_token = token.nbor(1)
                    if any([i in next_token.text for i in closed_brackets]):
                        span = Span(self.doc, token.i, next_token.i +1)
                        # prev_span = span.text
                        yield span
                elif any([i in token.text for i in open_brackets]):
                    next_token = token.nbor(1)
                    if next_token.text in closed_brackets:
                        span = Span(self.doc, token.i, next_token.i + 1)
                        # prev_span = span.text
                        yield span

            except IndexError: #skip end of sentence
                pass


    def collapse_noun_phrases_by_syntax(self):
        allowed_conjunction_dep = [prep]
        for token in self.sentence:
            if token.pos in [NOUN ,PROPN]:
                extended = self.get_extended_token(token)
                if extended.text != token.text:
                    yield extended
                siblings = list(token.head.children)
                span_range = [token.i, token.i]
                for sibling in siblings:
                    if sibling.dep == token.dep:# or sibling.dep in allowed_conjunction_dep:
                        if sibling.i > token.i:
                            span_range[1]= sibling.i
                        elif sibling.i < token.i:
                            span_range[0] = sibling.i

                if span_range != [token.i, token.i]:
                    span = Span(self.doc, span_range[0], span_range[1] + 1)
                    yield span


    def analyse(self, verbose = False):
        '''extract concepts'''

        '''collapse noun phrases based on syntax tree'''
        noun_phrases = list(self.collapse_noun_phrases_by_punctation())
        for np in noun_phrases:
            np.merge()
        noun_phrases = list(self.collapse_noun_phrases_by_syntax())
        for np in noun_phrases:
            np.merge()
        if verbose:
            self.print_syntax_list()
            self.print_syntax_tree()


        self.concepts = []
        noun_phrases = []
        verbs = [tok for tok in self.sentence if tok.pos == VERB and tok.dep not in (aux, auxpass)]
        for v in verbs:
            verb_text, verb_path, subjects = self.get_extended_verb(v)
            rights = list([i for i in v.rights if i.pos != VERB])
            # print v, subjects, rights
            for subject in subjects:
                for any_subject in self.get_alternative_subjects(subject):
                    noun_phrases.append(any_subject)
                    for r in rights:
                        dependend_objects = self.get_dependent_obj(r, verb_path)
                        for do in dependend_objects:
                            noun_phrases.append(do)
                            self.concepts.append(dict(
                                                    subject=any_subject.text,
                                                    object=do.text,
                                                    verb=verb_text,
                                                    verb_path = [i.text for i in verb_path],
                                                    verb_subtree = self.doc[v.left_edge.i : v.right_edge.i + 1].text,
                                                    subj_ver= '%s -> %s' %(any_subject.text, verb_text),
                                                    ver_obj =  '%s -> %s' %(verb_text, do.text),
                                                    concept= '%s -> %s -> %s'%(any_subject.text, verb_text, do.text),
                                                    negated = self.isNegated(v) or self.isNegated(any_subject) or \
                                                              self.isNegated(do)
                            ))
        self.noun_phrases=list(set(noun_phrases))
        # self.logger.info(self.noun_phrases)
        # for c in self.concepts:
        #     self.logger.info(c['concept'])


    def __str__(self):
        return self.sentence.text

    def write(self, message):
        '''needed to print nltk graph to logging'''
        if message != '\n':
            self.logger.debug(message)

