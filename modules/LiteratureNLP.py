#!/usr/local/bin/python
# coding: UTF-8
import logging
import multiprocessing
import re
import string
import time
from collections import Counter

from nltk.corpus import stopwords
from sklearn.feature_extraction.stop_words import ENGLISH_STOP_WORDS
from spacy.en import English
from tqdm import tqdm
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import Loader
from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisQueue, RedisQueueWorkerProcess
from modules.Literature import PublicationFetcher
from settings import Config
import nltk
from textblob import TextBlob
from textblob.base import BaseNPExtractor
from textblob.decorators import requires_nltk_corpus
from textblob.en.np_extractors import _normalize_tags
from unidecode import unidecode

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

class FastNPExtractor2(BaseNPExtractor):
    '''A fast and simple noun phrase extractor.

    Credit to Shlomi Babluk. Link to original blog post:

        http://thetokenizer.com/2013/05/09/efficient-way-to-extract-the-main-topics-of-a-sentence/
    '''

    CFG = {
        ('NNP', 'NNP'): 'NNP',
        ('NN', 'NN'): 'NNI',
        ('NNI', 'NN'): 'NNI',
        ('JJ', 'JJ'): 'JJ',
        ('JJ', 'NN'): 'NNI',
    }

    def __init__(self):
        self._trained = False

    @requires_nltk_corpus
    def train(self):
        nltk.download(['brown','punkt'])
        train_data = nltk.corpus.brown.tagged_sents(categories=['science_fiction'])
        regexp_tagger = nltk.RegexpTagger([
            (r'^-?[0-9]+(.[0-9]+)?$', 'CD'),
            (r'(-|:|;)$', ':'),
            (r'\'*$', 'MD'),
            (r'(The|the|A|a|An|an)$', 'AT'),
            (r'.*able$', 'JJ'),
            (r'^[A-Z].*$', 'NNP'),
            (r'.*ness$', 'NN'),
            (r'.*ly$', 'RB'),
            (r".*'s$", 'POS'),
            (r'.*s$', 'NNS'),
            (r'.*ing$', 'VBG'),
            (r'.*ed$', 'VBD'),
            (r'.*', 'NN'),
        ])
        unigram_tagger = nltk.UnigramTagger(train_data, backoff=regexp_tagger)
        self.tagger = nltk.BigramTagger(train_data, backoff=unigram_tagger)
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
        tags = _normalize_tags(tagged)
        merge = True
        while merge:
            merge = False
            for x in range(0, len(tags) - 1):
                t1 = tags[x]
                t2 = tags[x + 1]
                key = t1[1], t2[1]
                value = self.CFG.get(key, '')
                if value:
                    merge = True
                    tags.pop(x)
                    tags.pop(x)
                    match = '%s %s' % (t1[0], t2[0])
                    pos = value
                    tags.insert(x, (match, pos))
                    break

        matches = [t[0] for t in tags if t[1] in ['NNP', 'NNI']]
        return matches

class NounChuncker(object):

    def __init__(self):
        self.np_ex = FastNPExtractor2()
        self.normalizer = AbstractNormalizer()

    def digest(self, text):
        normalized = self.normalizer.normalize(text)
        parsed = TextBlob(normalized, np_extractor=self.np_ex)
        return list(parsed.noun_phrases)


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
        STOPLIST = set(stopwords.words('english') + ["n't", "'s", "'m", "ca","p","t"] + list(ENGLISH_STOP_WORDS))
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


