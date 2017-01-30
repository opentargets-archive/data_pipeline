import json
import unittest

import spacy
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher

from modules.LiteratureNLP import LiteratureNLPProcess,LiteratureInfoExtractor,LexiconParser
from run import PipelineConnectors

import logging
from lxml import etree
from spacy.tokenizer import Tokenizer
from spacy.language_data import TOKENIZER_INFIXES
import  spacy.util
from common.ElasticsearchLoader import Loader
from settings import Config
from common.DataStructure import JSONSerializable
from common.ElasticsearchQuery import ESQuery


SUBJECTS = ["nsubj", "nsubjpass", "csubj", "csubjpass", "agent", "expl"]
OBJECTS = ["dobj", "dative", "attr", "oprd"]

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




class LiteratureNLPTestCase(unittest.TestCase):



    def analyse_publication(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        p = PipelineConnectors()
        p.init_services_connections()

        LiteratureNLPProcess(p.es, r_server=p.r_server).process(['europepmc'])

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

    def test_text_analysis(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)






        nlp = spacy.en.English()
        matcher = Matcher(nlp.vocab)
        matcher.add_entity(
            "Chr 8p",  # Entity ID -- Helps you act on the match.
            {"ent_type": "BIO", "label": "this is a chromosome"},  # Arbitrary attributes (optional)
            on_match=self.print_matched_entities
        )

        matcher.add_pattern(
            "Chr 8p",  # Entity ID -- Created if doesn't exist.
            [  # The pattern is a list of *Token Specifiers*.
                {
                    ORTH: "Chromosome"
                },
                {
                    ORTH: "8p"
                }
            ],
            label='Chromosome 8p'  # Can associate a label to the pattern-match, to handle it better.
        )

        matcher.add_entity(
            "schizophrenia",  # Entity ID -- Helps you act on the match.
            {"ent_type": "DISEASE", "label": "schizophrenia"},  # Arbitrary attributes (optional)
            on_match=self.print_matched_entities
        )

        matcher.add_pattern(
            "schizophrenia",  # Entity ID -- Created if doesn't exist.
            [  # The pattern is a list of *Token Specifiers*.
                {
                    LEMMA: u"schizophrenia" # match case insensitive https://www.reddit.com/r/spacynlp/comments/4oclbi/nlp_matcher_add_caseinsensitive_patterns/
                },

            ],
            label='Schizophrenia'  # Can associate a label to the pattern-match, to handle it better.
        )

        doc = nlp(u'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: implications for schizophrenia, autism and cancer.')
        print("PRIVATE")
        matches = matcher(doc)# matches are printed in the callback


        '''
        quick way to store a list of entities/patterns to pass to the matcher
        can be loaded from json, we should encode the lexicon like this and pass it to spacy
        see test_biolexicon_parser below
        '''
        patterns = {
            'JS': ['PRODUCT', {}, [[{'ORTH': 'JavaScript'}]]],
            'GoogleNow': ['PRODUCT', {}, [[{'ORTH': 'Google'}, {'ORTH': 'Now'}]]],
            'Java': ['PRODUCT', {}, [[{'LOWER': 'java'}]]],
        }

        # '''using phrase matcher'''
        # def test_phrase_matcher():
        #     vocab = Vocab(lex_attr_getters=English.Defaults.lex_attr_getters)
        #     matcher = PhraseMatcher(vocab, [Doc(vocab, words='Google Now'.split())])
        #     doc = Doc(vocab, words=['I', 'like', 'Google', 'Now', 'best'])
        #     assert len(matcher(doc)) == 1


    def retreive_items_from_lexicon_xml(self,context,entity_type):
        c = 0
        lexicon = {}
        for action, cluster in context:
            c += 1
            cluster_id = cluster.attrib['clsId']
            #if cluster_id.endswith('HUMAN'):
            item = LexiconItem(cluster.attrib['clsId'], ent_type=entity_type)
            for entry in cluster.iterchildren(tag='Entry'):
                if entry.attrib['baseForm']:
                    term_frequency = entry.attrib['mlfreq']
                    if int(term_frequency) < 200000:
                        if not item.label and entry.attrib['baseForm']:
                            item.label = entry.attrib['baseForm']

                        item.add_variant(entry.attrib['baseForm'],
                                         entry.attrib['mlfreq'])
                '''Synonyms'''
                for variant in entry.iterchildren(tag='Variant'):
                    term_frequency = variant.attrib['mlfreq']
                    if int(term_frequency) < 200000:
                        item.add_variant(variant.attrib['writtenForm'],
                                         term_frequency)

            lexicon[item.id] = item
            if c % 10000 == 0:
                logging.info('parsed %i lexicon term' % c)
            yield item

    def parse_lexicon(self,xml_file, lexicon_type,json_file):

        nlp = spacy.en.English()
        matcher = Matcher(vocab=nlp.vocab, )
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        '''parse a list of genes from biolexicon'''

        context = etree.iterparse(open(xml_file),
                                  # context = etree.iterparse(open("resources/test-spacy/geneProt.xml"),
                                  tag='Cluster')  # requries geneProt.xml from LexEBI
        # ftp://ftp.ebi.ac.uk/pub/software/textmining/bootstrep/termrepository/LexEBI/


        encoded_lexicon = {}
        encoded_lexicon_json_file = json_file
        for item in self.retreive_items_from_lexicon_xml(context,lexicon_type):
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



    def test_nlp_gene_matcher(self):


        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        gene_file = '/Users/pwankar/Downloads/geneProt (1).xml'
        self.parse_lexicon(gene_file,'GENE','gene_lexicon.json')

        nlp = spacy.en.English()
        # matcher = Matcher.load(path = 'gene_lexicon.json',
        #                        vocab=nlp.vocab)
        # FIXED IN SPACY MASTER BUT A BUG IN 1.5.0

        '''load all the new patterns, do not use Matcher.load since there is a bug'''
        matcher = Matcher(vocab=nlp.vocab,
                          patterns=json.load(open('gene_lexicon.json'))
                          )
        self.assertTrue(matcher.has_entity('UNIPR_ROA1_PANTR'))
        '''should save the new vocab now'''
        # Matcher.vocab.dump('lexeme.bin')
        '''test real abstract'''
        pmid = '19204725'
        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF17, FGF20, FGFR1, FZD3, LDL, NAT2, NEF3, NRG1, PCM1, PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               '' \
               '' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer. In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'

        doc = nlp(unicode(text))
        matches = matcher(doc)
        logging.info('Found matches!!!!!!')
        for ent_id, label, start, end in matches:
            '''doc[start: end] - actual match in document; could be a synonymn'''
            span = (matcher.get_entity(ent_id), label, doc[start: end])
            logging.info(span)


    def test_nlp_disease_matcher(self):


        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)

        # disease_file = '/Users/pwankar/Downloads/umlsDisease.xml'
        # self.parse_lexicon(disease_file, 'DISEASE', 'disease_lexicon.json')

        nlp = spacy.en.English()
        # matcher = Matcher.load(path = 'gene_lexicon.json',
        #                        vocab=nlp.vocab)
        # FIXED IN SPACY MASTER BUT A BUG IN 1.5.0

        '''load all the new patterns, do not use Matcher.load since there is a bug'''
        matcher = Matcher(vocab=nlp.vocab,
                          patterns=json.load(open('disease_lexicon.json'))
                          )
        '''should save the new vocab now'''
        # Matcher.vocab.dump('lexeme.bin')
        '''test real abstract'''
        pmid = '19204725'
        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF17, FGF20, FGFR1, FZD3, LDL, NAT2, NEF3, NRG1, PCM1, PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer . Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               '' \
               '' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer . In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'

        doc = nlp(unicode(text))
        matches = matcher(doc)
        logging.info('Found matches!!!!!!')
        for ent_id, label, start, end in matches:
            '''doc[start: end] - actual match in document; could be a synonymn'''
            span = (matcher.get_entity(ent_id), label, doc[start: end])
            logging.info(span)


    def test_match_from_gene_lexicon(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        nlp = spacy.en.English()
        # matcher = Matcher.load(path = 'gene_lexicon.json',
        #                        vocab=nlp.vocab)
        # FIXED IN SPACY MASTER BUT A BUG IN 1.5.0

        '''load all the new patterns, do not use Matcher.load since there is a bug'''
        matcher = Matcher(vocab = nlp.vocab,
                          patterns= json.load(open('gene_lexicon.json'))
                          )

        self.assertTrue(matcher.has_entity('UNIPR_ROA1_PANTR'))
        '''should save the new vocab now'''
        # Matcher.vocab.dump('lexeme.bin')
        #TODO


        '''test real abstract'''
        pmid = '19204725'
        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF20, FGFR1, FZD3, LDL, NAT2, nef3, NRG1, PCM1,PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1 ) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               '' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer. In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'

        doc = nlp(unicode(text))
        matches = matcher(doc)
        for ent_id, label, start, end in matches:
            span = (matcher.get_entity(ent_id), label, doc[start: end])
            logging.info(span)
            # Observations = VMAT1/SLC18A1 both are synonymns, neither is recognized
            # False positives for as,to,for,by,and ... etc. his,act
            # Words in brackets are recognized
            # ({u'label': u'Bladder cancer-associated protein'}, 154084, human) - 'human' is associated to <Cluster clsId="UNIPR_BLCAP_RAT" semType="geneProt">
            # <Entry entryId="UNIPR_BLCAP_RAT_1" baseForm="Bladder cancer-associated protein" - Is this valid
            # PCM1,PLAT, - Both PCM1 and PLAT are not recognized in this case, but they are recognized if separated by space and comma viz. PCM1, PLAT,
            #nef3 - not found - biolexicon contains it in the form NEF3,Nef3. As lower case is not present in the lexicon- it is ignored by pattern matcher
            #baseForm="" empty baseform results in null labels , is this a bug, how to handle this?



    def test_spacy_custom_tokenizer(self):
        logging.info("Results with classId = HUMAN check!!")


        def create_tokenizer(nlp):
            infix_re = spacy.util.compile_infix_regex(tuple(TOKENIZER_INFIXES + ['/',',']))
            #infix_re = re.compile('/')

            return Tokenizer(nlp.vocab,{},nlp.tokenizer.prefix_search,nlp.tokenizer.suffix_search,
                             infix_re.finditer)

        gene_file = 'resources/test-spacy/geneProt.xml'
        self.parse_lexicon(gene_file, 'GENE', 'gene_lexicon-test.json')
        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF20, FGFR1, FZD3, LDL, NAT2, nef3, NRG1, PCM1,PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1 ) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               '' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer. In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'

        text_blcap = 'Bladder cancer-associated protein (BLCAP) is downregulated in cancer and has been identified ' \
                     'as a prognostic biomarker for human cancer. We previously reported that BLCAP mRNA is decreased' \
                     ' in cervical cancer tissues, and overexpression of BLCAP was found to inhibit cell growth and induce apoptosis ' \
                     'in the human cervical cancer HeLa cell line To investigate the BLCAP protein expression in cervical cancer ' \
                     'and its potential clinical indications, we developed a polyclonal antibody against human BLCAP to assess the ' \
                     'BLCAP protein expression in 30 cervical cancer tissues and 30 non-tumor cervical tissues from patients. ' \
                     'Western blotting data showed that a single band of recombinant protein was probed by antiserum of BLCAP and no ' \
                     'band was probed by pre-immune serum. BLCAP expression was significantly downregulated in cervical carcinoma tissues ' \
                     'compared with its expression in the non-tumor cervical tissues. Moreover, cervical carcinoma tissues from ' \
                     'patients with stage III-IV had significantly lower BLCAP expression percentage compared with stage I-II. ' \
                     'Similarly, a significantly lower BLCAP expression percentage was observed in moderately/poorly differentiated ' \
                     'tumor tissues and in the tumor tissues from patients with lymphatic metastasis (LM) compared with ' \
                     'well-differentiated tumor tissues and non-LM patients, respectively. Our results suggest that decreased BLCAP ' \
                     'protein expression is associated with poor prognosis and it could be a potential bio-index to predict cervical ' \
                     'tumor patient outcome.'

        text_genes = 'Mutational landscape of gingivo-buccal oral squamous cell carcinoma reveals new recurrently-mutated genes' \
                     ' and molecular subgroups. Gingivo-buccal oral squamous cell carcinoma (OSCC-GB), an anatomical and clinical' \
                     ' subtype of head and neck squamous cell carcinoma (HNSCC), is prevalent in regions where tobacco-chewing is common.' \
                     ' Exome sequencing (n=50) and recurrence testing (n=60) reveals that some significantly and frequently altered genes' \
                     ' are specific to OSCC-GB (USP9X, MLL4, ARID2, UNC13C and TRPM3), while some others are shared with HNSCC ' \
                     '(for example, TP53, FAT1, CASP8, HRAS and NOTCH1). We also find new genes with recurrent amplifications ' \
                     '(for example, DROSHA, YAP1) or homozygous deletions (for example, DDX3X) in OSCC-GB. We find a high proportion of' \
                     ' C>G transversions among tobacco users with high numbers of mutations. Many pathways that are enriched for genomic alterations ' \
                     'are specific to OSCC-GB. Our work reveals molecular subtypes with distinctive mutational profiles such as patients predominantly' \
                     ' harbouring mutations in CASP8 with or without mutations in FAT1. Mean duration of disease-free survival is significantly elevated' \
                     ' in some molecular subgroups. These findings open new avenues for biological characterization and exploration of therapies. '
                     #old_tokens = nlp(unicode(text))

        nlp = spacy.load('en', create_make_doc=create_tokenizer)

        custom_tokens = nlp(unicode(text_genes))
        #
        #
        matcher = Matcher(vocab=nlp.vocab,
                          patterns=json.load(open('gene_lexicon-test.json'))
                          )
        matches = matcher(custom_tokens)
        for ent_id, label, start, end in matches:
            span = (matcher.get_entity(ent_id), label, custom_tokens[start: end])
            logging.info(span)

        # ''' BLCAP Test'''
        # custom_tokens = nlp(unicode(text_blcap))
        # logging.info("BLCAP text - tokens")
        # for i, token in enumerate(custom_tokens):
        #     print("custom tokens:", token.orth, token.orth_)

        # logging.info("BLCAP Gene Matches")


        # for i, token in enumerate(custom_tokens):
        #     print("custom tokens:", token.orth, token.orth_)

        # matcher = Matcher(vocab=nlp.vocab,
        #                   patterns=json.load(open('gene_lexicon.json'))
        #                   )
        # matches = matcher(custom_tokens)
        # for ent_id, label, start, end in matches:
        #     span = (matcher.get_entity(ent_id), label, custom_tokens[start: end])
        #     logging.info(span)
        #
        # logging.info("BLCAP Disease Matches")
        #
        # disease_file = '/Users/pwankar/Downloads/umlsDisease.xml'
        # self.parse_lexicon(disease_file, 'DISEASE', 'disease_lexicon.json')
        # matcher = Matcher(vocab=nlp.vocab,
        #                   patterns=json.load(open('disease_lexicon.json'))
        #                   )
        # matches = matcher(custom_tokens)
        # for ent_id, label, start, end in matches:
        #     span = (matcher.get_entity(ent_id), label, custom_tokens[start: end])
        #     logging.info(span)

    def test_lit_info_extractor(self):
        p = PipelineConnectors()
        p.init_services_connections()

        LiteratureInfoExtractor(p.es, r_server=p.r_server).process()

    def test_disease_patterns(self):
        disease_lexicon_parser = LexiconParser('resources/test-spacy/disease.xml', 'disease-lexicon-test.json',
                                               'DISEASE')
        disease_lexicon_parser.parse_lexicon()

    def extract_entity_relations(self,gene_matches,disease_matches,doc):
        over_simplified_text = 'Studies have identified that ADRA1A contributes' \
                               ' to schizophrenia'

        relations = []
        #for gene in filter(lambda w: w.orth_ in gene_matches, doc):
        for  token in doc:
            #print token.orth_, token.dep_, token.head.orth_, [t.orth_ for t in token.lefts], [t.orth_ for t in token.rights]
            if token.orth_ in gene_matches:
                gene = token
                if gene.dep_ in ('attr', 'dobj'):
                    subject = [w for w in gene.head.lefts if w.dep_ == 'nsubj']
                    if subject and subject in disease_matches:
                        subject = subject[0]
                        relations.append((subject, gene.head, gene))
                elif gene.dep_ == 'pobj' and gene.head.dep_ == 'prep':
                    relations.append((gene.head.head, gene))


        return relations

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




    def load_entity_matches(self,loader, nlp, doc):


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

    def test_entity_relations(self):


        p = PipelineConnectors()
        p.init_services_connections()
        loader = Loader(p.es, chunk_size=1000)


        if not loader.es.indices.exists(Loader.get_versioned_index(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME)):
            loader.create_new_index(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME)

        def create_tokenizer(nlp):
            infix_re = spacy.util.compile_infix_regex(tuple(TOKENIZER_INFIXES + ['/',',']))
            #infix_re = re.compile('/')

            return Tokenizer(nlp.vocab,{},nlp.tokenizer.prefix_search,nlp.tokenizer.suffix_search,
                             infix_re.finditer)




        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF20, FGFR1, FZD3, LDL, NAT2, nef3, NRG1, PCM1,PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1 ) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               '' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer. In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'

        over_simplified_text = 'Molecular genetics and developmental studies have identified that ADRA1A , ARHGEF10 do not contribute' \
                               ' to schizophrenia'


        simplified_text = 'Molecular genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF20, FGFR1, FZD3, LDL, NAT2, nef3, NRG1, PCM1,PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1 ) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer.'

        nlp = spacy.load('en', create_make_doc=create_tokenizer)
        doc = nlp(unicode(over_simplified_text))

        # '''Simple dependency parsing'''
        # doc = nlp(unicode(over_simplified_text))
        # sentences = list(doc.sents)
        # subj = ''
        # obj = ''
        # for sentence in sentences:
        #     root_token= sentence.root
        #     for child in root_token.children:
        #         if child.dep_ == 'nsubj':
        #             subj = child
        #         if child.dep_ == 'dobj':
        #             obj = child
        #     print 'Original sentence {}'.format(sentence)
        #     print 'Subject {} Predicate {} Object {} '.format(subj,root_token ,obj)

        '''Navigating the dependency parse tree'''
        es_query = ESQuery()
        gene_matched_entities = []
        for gene in es_query.get_lit_entities_for_type('GENE'):
            gene_matched_entities.append(gene)
        disease_matched_entities = []
        for disease in es_query.get_lit_entities_for_type('DISEASE'):
            disease_matched_entities.append(disease)

        print 'Gene Disease Relations'
        gene_matched_labels = list(map(lambda x: x['matched_word'], gene_matched_entities))
        disease_matched_labels = list(map(lambda x: x['matched_word'], disease_matched_entities))
        relations = self.extract_entity_relations_by_verb(disease_matched_labels, gene_matched_labels, doc)
        # for r1, r2, r3 in relations:
        #     print(r1.text, r2.text,  r3.text)


if __name__ == '__main__':
    unittest.main()
