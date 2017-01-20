import json
import unittest

import sys

import spacy
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher

from modules.LiteratureNLP import LiteratureNLPProcess
from run import PipelineConnectors

import logging
from lxml import etree
from spacy.tokenizer import Tokenizer
from spacy.language_data import TOKENIZER_INFIXES
import  spacy.util
import re

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
            item = LexiconItem(cluster.attrib['clsId'], ent_type=entity_type)
            for entry in cluster.iterchildren(tag='Entry'):
                if entry.attrib['baseForm']:
                    if not item.label and entry.attrib['baseForm']:
                        item.label = entry.attrib['baseForm']
                    item.add_variant(entry.attrib['baseForm'],
                                     entry.attrib['mlfreq'])
                '''Synonyms'''
                for variant in entry.iterchildren(tag='Variant'):
                    item.add_variant(variant.attrib['writtenForm'],
                                     variant.attrib['mlfreq'])

            lexicon[item.id] = item
            if c % 10000 == 0:
                logging.info('parsed %i lexicon term' % c)
            yield item



    def parse_lexicon(self,xml_file,lexicon_type,json_file):

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
            # False positives for as,to,for,by,and ... etc.
            # Words in brackets are recognized
            # ({u'label': u'Bladder cancer-associated protein'}, 154084, human) - 'human' is associated to <Cluster clsId="UNIPR_BLCAP_RAT" semType="geneProt">
            # <Entry entryId="UNIPR_BLCAP_RAT_1" baseForm="Bladder cancer-associated protein" - Is this valid
            # PCM1,PLAT, - Both PCM1 and PLAT are not recognized in this case, but they are recognized if separated by space and comma viz. PCM1, PLAT,
            #nef3 - not found - biolexicon contains it in the form NEF3,Nef3. As lower case is not present in the lexicon- it is ignored by pattern matcher
            #baseForm="" empty baseform results in null labels , is this a bug, how to handle this?



    def test_spacy_custom_tokenizer(self):


        def create_tokenizer(nlp):
            infix_re = spacy.util.compile_infix_regex(tuple(TOKENIZER_INFIXES + ['/',',']))
            #infix_re = re.compile('/')

            return Tokenizer(nlp.vocab,{},nlp.tokenizer.prefix_search,nlp.tokenizer.suffix_search,
                             infix_re.finditer)

        nlp = spacy.en.English()
        nlp.tokenizer
        text = 'However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               '' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF20, FGFR1, FZD3, LDL, NAT2, nef3, NRG1, PCM1,PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1 ) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. '
        old_tokens = nlp(unicode(text))

        nlp = spacy.load('en', create_make_doc=create_tokenizer)
        nlp.tokenizer
        custom_tokens = nlp(unicode(text))

        for i, token in enumerate(custom_tokens):
            print("custom tokens:", token.orth, token.orth_)



if __name__ == '__main__':
    unittest.main()
