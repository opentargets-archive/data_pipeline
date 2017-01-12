import json
import unittest

import sys

import spacy
from spacy.attrs import ORTH, TAG, LEMMA
from spacy.matcher import Matcher

from modules.LiteratureNLP import PublicationFetcher,PublicationAnalyserSpacy, LiteratureNLPProcess
from run import PipelineConnectors

import logging
from lxml import etree

class LexiconItem(object):
    def __init__(self, id, label = None, ent_type ='ENT'):
        self.id = id
        self.label = label
        self.ent_type = ent_type
        self.variants = []
        self.all_forms = set()

    def add_variant(self, term, freq):
        self.variants.append(dict(term=term,
                                  freq=freq))
        self.all_forms.add(term)

    def __str__(self):
        string = ['ID: %s\nBASE:  %s'%(self.id,self.base)]
        for i in self.variants:
            string.append('VARIANT: %s'%i['term'])
        return '\n'.join(string)

    def to_dict(self):
        d = {self.id : [self.ent_type, {}, [[{'ORTH': i} for i in self.all_forms]]]}
        return d



class LiteratureNLPTestCase(unittest.TestCase):

    def analyse_publication(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)
        p = PipelineConnectors()
        p.init_services_connections()

        LiteratureNLPProcess(p.es, r_server=p.r_server).process(['europepmc'])

    def test_text_analysis(self):

        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)




        def print_matched_entities(matcher, doc, i, matches):
            ''' callback '''
            # '''all matches'''
            # spans = [(matcher.get_entity(ent_id), label, doc[start : end]) for ent_id, label, start, end in matches]
            # for span in spans:
            #     print span
            '''just the matched one'''
            ent_id, label, start, end = matches[i]
            span = (matcher.get_entity(ent_id), label, doc[start: end])
            print span

        nlp = spacy.en.English()
        matcher = Matcher(nlp.vocab)
        matcher.add_entity(
            "Chr 8p",  # Entity ID -- Helps you act on the match.
            {"ent_type": "BIO", "label": "this is a chromosome"},  # Arbitrary attributes (optional)
            on_match=print_matched_entities
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
            on_match=print_matched_entities
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




    def test_lexicon_parsing(self):

        '''parse a list of genes from biolexicon'''
        context = etree.iterparse(open("/Users/andreap/Downloads/geneProt.xml"),
                                  tag='Cluster')  # requries geneProt.xml from LexEBI
        # ftp://ftp.ebi.ac.uk/pub/software/textmining/bootstrep/termrepository/LexEBI/

        encoded_lexicon = {}
        encoded_lexicon_json_file = 'gene_lexicon.json'
        nlp = spacy.en.English()
        matcher = Matcher(vocab=nlp.vocab,)
        c = 0
        lexicon = {}
        for action, cluster in context:
            c += 1
            item = LexiconItem(cluster.attrib['clsId'], ent_type='GENE')
            for entry in cluster.iterchildren(tag='Entry'):
                if entry.attrib['baseForm']:
                    if not item.label and entry.attrib['baseForm']:
                        item.label = entry.attrib['baseForm']
                    item.add_variant(entry.attrib['baseForm'],
                                     entry.attrib['mlfreq'])
                for variant in entry.iterchildren(tag='Variant'):
                    item.add_variant(variant.attrib['writtenForm'],
                                     variant.attrib['mlfreq'])
            if item.all_forms:
                token_specs = [{ORTH: i} for i in item.all_forms]
                matcher.add_pattern(entity_key = item.id,
                                    token_specs = token_specs,
                                    label = item.label,
                                    )
                encoded_lexicon.update(item.to_dict())
            lexicon[item.id] = item
            if c % 10000 == 0:
                print 'parsed %i lexicon term' % c

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
        print matches


        json.dump(encoded_lexicon,
                  open(encoded_lexicon_json_file,'w'),
                  indent=4)

    def test_match_from_gene_lexicon(self):
        nlp = spacy.en.English()
        # matcher = Matcher.load(path = 'gene_lexicon.json',
        #                        vocab=nlp.vocab)

        '''load all the new patterns, do not use Matcher.load since there is a bug'''
        matcher = Matcher(vocab = nlp.vocab,
                          patterns= json.load(open('gene_lexicon.json'))
                          )
        # matcher.add_pattern(
        #     "UNIPR_DKK4_HUMAN",  # Entity ID -- Created if doesn't exist.
        #     [  # The pattern is a list of *Token Specifiers*.
        #         {
        #             ORTH: u"DKK4"
        #         # match case insensitive
        #             # https://www.reddit.com/r/spacynlp/comments/4oclbi/nlp_matcher_add_caseinsensitive_patterns/
        #         },
        #
        #     ],
        #     label='DKK4'  # Can associate a label to the pattern-match, to handle it better.
        # )
        self.assertTrue(matcher.has_entity('UNIPR_DKK4_HUMAN'))
        '''should save the new vocab now'''
        # Matcher.vocab.dump('lexeme.bin')
        #TODO

        # '''test short sentence'''
        # text = u'Chromosome DKK4 as a potential hub for developmental neuropsychiatric disorders:'
        # doc = nlp(
        #     u'Chromosome DKK4 as a potential hub for developmental neuropsychiatric disorders.')
        # print("PRIVATE")
        # matches = matcher(doc)
        # print matches

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
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF17, FGF20, FGFR1, FZD3, LDL, NAT2, NEF3, NRG1, PCM1, PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1) that are most likely to contribute to neuropsychiatric disorders ' \
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
        print matches



if __name__ == '__main__':
    unittest.main()
