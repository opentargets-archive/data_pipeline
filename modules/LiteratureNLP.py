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
from lxml import etree
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher
from spacy.tokenizer import Tokenizer
from spacy.language_data import TOKENIZER_INFIXES
import spacy.util
from spacy.tokens.doc import Doc


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
                 entities=None
                 ):
        super(PublicationAnalysisSpacy, self).__init__(pub_id)
        self.lemmas = lemmas
        self.noun_chunks = noun_chunks
        self.analysed_sentences_count = analysed_sentences_count
        self.entities = entities


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
                dry_run=False,
                force=False):


        if not os.path.isfile(Config.GENE_LEXICON_JSON_LOCN):
            logging.info('Generating gene matcher patterns')
            gene_lexicon_parser = LexiconParser(Config.BIOLEXICON_GENE_XML_LOCN,Config.GENE_LEXICON_JSON_LOCN,'GENE')
            gene_lexicon_parser.parse_lexicon()

        if not os.path.isfile(Config.DISEASE_LEXICON_JSON_LOCN):
            logging.info('Generating disease matcher patterns')
            disease_lexicon_parser = LexiconParser(Config.BIOLEXICON_DISEASE_XML_LOCN, Config.DISEASE_LEXICON_JSON_LOCN, 'DISEASE')
            disease_lexicon_parser.parse_lexicon()

        i = 1
        if not self.loader.es.indices.exists(Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)):
            self.loader.create_new_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME, recreate=force)
        self.loader.prepare_for_bulk_indexing(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME)

        no_of_workers = Config.WORKERS_NUMBER or multiprocessing.cpu_count()
        #no_of_workers = 2

        # Gene Matcher Queue
        tokenizer_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|tokenizer',
                            max_size=MAX_CHUNKS * no_of_workers,
                            job_timeout=1200)

        # Gene Matcher Queue
        gene_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|gene_matcher',
                                 max_size=MAX_CHUNKS * no_of_workers,
                                 job_timeout=1200)

        # Disease Matcher Queue
        disease_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|disease_matcher',
                              max_size=MAX_CHUNKS * no_of_workers,
                              job_timeout=120)

        # ES-Loader Queue
        loader_q = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|publication_loader',
                              max_size=MAX_CHUNKS * no_of_workers,
                              job_timeout=120)

        tokenizers = [NLPTokenizerProcess(tokenizer_q,
                                                self.r_server.db,
                                                gene_q
                                                )
                         for i in range(no_of_workers/2)]
        for w in tokenizers:
            w.start()

        gene_matchers = [GeneRecognitionProcess(gene_q,
                                             self.r_server.db,
                                             disease_q
                                             )
                      for i in range(no_of_workers/2)]

        for w in gene_matchers:
            w.start()

        disease_matchers = [DiseaseRecognitionProcess(disease_q,
                                             self.r_server.db,
                                             loader_q
                                             )
                      for i in range(no_of_workers/2)]

        for w in disease_matchers:
            w.start()

        loaders = [PublicationLoaderProcess(loader_q,
                                           self.r_server.db,
                                           dry_run
                                           )
                   for i in range(no_of_workers / 2 )]

        for w in loaders:
            w.start()
        # loader = PublicationLoaderProcess(loader_q,self.r_server.db,dry_run)
        # loader.start()

        logging.info('Finding entity matches')
        # for ev in tqdm(self.es_query.get_abstracts_from_val_ev(),
        #             desc='Reading available publications for nlp information extraction',
        #             total = self.es_query.count_validated_evidence_strings(),
        #             unit=' evidence',
        #             unit_scale=True):
        for pub in tqdm(self.es_query.get_all_publications(),
                           desc='Reading available publications for nlp information extraction',
                           total=self.es_query.count_validated_evidence_strings(),
                           unit=' evidence',
                           unit_scale=True):


            if pub['title'] and pub['abstract']:
                i = i + 1
                if (i > 101):
                    break

                text_to_analyze = unicode(pub['title'] + ' ' + ''.join(pub['abstract']))
                tokenizer_q.put((pub['pub_id'], text_to_analyze))
                    # spacyManager.generateRelations(disease_matches,gene_matches)
        tokenizer_q.set_submission_finished(r_server=self.r_server)

        for t in tokenizers:
            t.join()

        for g in gene_matchers:
            g.join()

        for d in disease_matchers:
            d.join()

        for l in loaders:
            l.join()

        logging.info('flushing data to index')

        self.loader.es.indices.flush(
            '%s*' % Loader.get_versioned_index(Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME),
            wait_if_ongoing=True)

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
    def __init__(self, id, label=None, ent_type='ENT', matched_word=None, start_pos = None, end_pos = None):
        self.id = id
        self.label = label
        self.ent_type = ent_type
        self.matched_word = matched_word
        self.start_pos = start_pos
        self.end_pos = end_pos

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash((self.matched_word, self.label))

    def __str__(self):
        string = 'Matched Word {}, Lex_Label {}'.format(self.matched_word,self.label)
        return string


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


class NLPManager(object):
    def __init__(self, nlp=None):
        def create_tokenizer(nlp):
            infix_re = spacy.util.compile_infix_regex(tuple(TOKENIZER_INFIXES + ['/', ',']))
            return Tokenizer(nlp.vocab, {}, nlp.tokenizer.prefix_search, nlp.tokenizer.suffix_search,
                             infix_re.finditer)
        if nlp is None:
           if nlp is None:
                    self.nlp = spacy.load('en', create_make_doc=create_tokenizer)
        else:
            self.nlp = nlp

    def tokenizeText(self,text):


        custom_tokens = self.nlp(unicode(text))
        return custom_tokens


    def findEntityMatches(self,matcher, tokens):
        entities = set()

        matches = matcher(tokens)

        for ent_id, label, start, end in matches:
            '''doc[start: end] - actual match in document; could be a synonymn'''
            span = (matcher.get_entity(ent_id), label, tokens[start: end])
            litentity = LitEntity(ent_id, matcher.get_entity(ent_id)['label'], 'GENE', tokens[start: end].text, start, end)
            entities.add(litentity)
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


class NLPTokenizerProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out):
        super(NLPTokenizerProcess, self).__init__(queue_in, redis_path, queue_out)
        #self.nlp_manager = NLPManager()
        nlp = spacy.load('en')
        self.nlp_manager = NLPManager(nlp)
        self.spacyDoc = Doc(self.nlp_manager.nlp.vocab)
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        pub_id, text = data

        tokens = self.nlp_manager.tokenizeText(text)

        return (pub_id,tokens.to_bytes())


class GeneRecognitionProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out):
        super(GeneRecognitionProcess, self).__init__(queue_in, redis_path, queue_out)
        nlp = spacy.load('en')
        self.nlp_manager = NLPManager(nlp)

        gene_patterns = json.load(open(Config.GENE_LEXICON_JSON_LOCN))
        self.gene_match_patterns = Matcher(vocab=self.nlp_manager.nlp.vocab,
                                  patterns=gene_patterns
                                  )
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        pub_id, tokens = data
        spacyDoc = Doc(self.nlp_manager.nlp.vocab)
        genes = self.nlp_manager.findEntityMatches(self.gene_match_patterns,spacyDoc.from_bytes(tokens))
        analysed_pub = PublicationAnalysisSpacy(pub_id=pub_id,
                                                entities=list(genes)
                                                )
        return (analysed_pub, tokens)



class DiseaseRecognitionProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 queue_out):
        super(DiseaseRecognitionProcess, self).__init__(queue_in, redis_path,queue_out)
        #self.nlp_manager = NLPManager()
        nlp = spacy.load('en')
        self.nlp_manager = NLPManager(nlp)

        disease_patterns = json.load(open(Config.DISEASE_LEXICON_JSON_LOCN))
        self.disease_match_patterns = Matcher(vocab=self.nlp_manager.nlp.vocab,
                                           patterns=disease_patterns
                                           )
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)


    def process(self, data):
        analysed_pub, tokens = data
        spacyDoc = Doc(self.nlp_manager.nlp.vocab)
        diseases = self.nlp_manager.findEntityMatches(self.disease_match_patterns,spacyDoc.from_bytes(tokens))
        genes = analysed_pub.entities
        analysed_pub.entities = genes + list(diseases)
        return analysed_pub


class PublicationLoaderProcess(RedisQueueWorkerProcess):

    def __init__(self,
                 queue_in,
                 redis_path,
                 dry_run=False):
        super(PublicationLoaderProcess, self).__init__(queue_in, redis_path)
        self.loader = Loader(chunk_size=10000, dry_run=dry_run)
        self.start_time = time.time()  # reset timer start
        self.logger = logging.getLogger(__name__)


    def process(self, data):

        analysed_pub = data
        logging.info('Pub Id - {} '.format(analysed_pub.pub_id))

        # doc_body = json.dumps({'doc':analysed_pub.to_json(),'doc_as_upsert':True})
        self.loader.put(index_name=Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                        doc_type=analysed_pub.get_type(),
                        ID=analysed_pub.pub_id,
                        body=analysed_pub,
                        # operation='update',
                        parent=analysed_pub.pub_id,
                        )




    def close(self):
        self.loader.close()





