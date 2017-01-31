import json
import unittest

import spacy
from spacy.attrs import ORTH, LEMMA
from spacy.matcher import Matcher

from modules.LiteratureNLP import LiteratureNLPProcess,LiteratureInfoExtractor,LexiconParser, NLPManager,LitEntity
from run import PipelineConnectors

import logging
import os
import  spacy.util
from common.ElasticsearchLoader import Loader
from settings import Config
from common.DataStructure import JSONSerializable
from common.ElasticsearchQuery import ESQuery

from nose.tools.nontrivial import with_setup



logger = logging.getLogger(__name__)

def my_setup_function():
    if not os.path.isfile(Config.GENE_LEXICON_JSON_LOCN):
        logging.info('Generating gene matcher patterns')
        gene_lexicon_parser = LexiconParser(Config.BIOLEXICON_GENE_XML_LOCN, Config.GENE_LEXICON_JSON_LOCN, 'GENE')
        gene_lexicon_parser.parse_lexicon()

    if not os.path.isfile(Config.DISEASE_LEXICON_JSON_LOCN):
        logging.info('Generating disease matcher patterns')
        disease_lexicon_parser = LexiconParser(Config.BIOLEXICON_DISEASE_XML_LOCN, Config.DISEASE_LEXICON_JSON_LOCN,
                                               'DISEASE')
        disease_lexicon_parser.parse_lexicon()



def my_teardown_function():
    logger.info("my_teardown_function")



class LiteratureNLPTestCase(unittest.TestCase):



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





    @with_setup(my_setup_function, my_teardown_function)
    def test_disease_matcher(self):


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

        spacyManager = NLPManager()
        disease_patterns = json.load(open(Config.DISEASE_LEXICON_JSON_LOCN))
        doc = spacyManager.tokenizeText(text)

        disease_matcher = Matcher(vocab=self.nlp.vocab,
                               patterns=disease_patterns
                               )
        matches = spacyManager.findEntityMatches(disease_matcher, doc)
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
        blcap_tokens = spacyManager.tokenizeText(text_blcap)
        matches = spacyManager.findEntityMatches(disease_matcher, blcap_tokens)

    @with_setup(my_setup_function, my_teardown_function)
    def test_gene_matcher(self):
        logging.basicConfig(filename='output.log',
                            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                            level=logging.INFO)



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

        spacyManager = NLPManager()
        gene_patterns = json.load(open(Config.GENE_LEXICON_JSON_LOCN))
        doc = spacyManager.tokenizeText(text)

        gene_matcher = Matcher(vocab=self.nlp.vocab,
                               patterns=gene_patterns
                               )
        gene_matches = spacyManager.findEntityMatches(gene_matcher,doc)

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
        blcap_tokens = spacyManager.tokenizeText(text_blcap)
        matches = spacyManager.findEntityMatches(gene_matcher, blcap_tokens)

            # Observations = VMAT1/SLC18A1 both are synonymns, neither is recognized
            # False positives for as,to,for,by,and ... etc. his,act
            # Words in brackets are recognized
            # ({u'label': u'Bladder cancer-associated protein'}, 154084, human) - 'human' is associated to <Cluster clsId="UNIPR_BLCAP_RAT" semType="geneProt">
            # <Entry entryId="UNIPR_BLCAP_RAT_1" baseForm="Bladder cancer-associated protein" - Is this valid
            # PCM1,PLAT, - Both PCM1 and PLAT are not recognized in this case, but they are recognized if separated by space and comma viz. PCM1, PLAT,
            #nef3 - not found - biolexicon contains it in the form NEF3,Nef3. As lower case is not present in the lexicon- it is ignored by pattern matcher
            #baseForm="" empty baseform results in null labels , is this a bug, how to handle this?

    @with_setup(my_setup_function, my_teardown_function)
    def test_spacy_custom_tokenizer(self):

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

        spacyManager = NLPManager()

        doc = spacyManager.tokenizeText(text)
        tokens = [tok.orth_ for tok in doc]
        self.assertIn('ADRA1A',tokens)
        self.assertIn('VMAT1', tokens,'tokens separated by /')
        self.assertIn('SLC18A1', tokens,'tokens separated by /')
        self.assertIn('Fgf17', tokens,'words in brackets')
        self.assertIn('disorders', tokens,'words followed by semicolon')
        self.assertIn('PCM1', tokens, 'words separate by comma but without spaces - PCM1,PLAT,')
        self.assertIn('PLAT', tokens, 'words separate by comma but without spaces - PCM1,PLAT,')


    def test_lit_info_extractor(self):
        p = PipelineConnectors()
        p.init_services_connections()
        LiteratureInfoExtractor(p.es, r_server=p.r_server).process()

    def test_gene_pattern_generation(self):
        disease_lexicon_parser = LexiconParser('resources/test-spacy/geneProt.xml', 'gene_lexicon.json',
                                               'GENE')
        disease_lexicon_parser.parse_lexicon()

    def test_disease_pattern_generation(self):
        disease_lexicon_parser = LexiconParser('resources/test-spacy/disease.xml', 'disease-lexicon-test.json',
                                               'DISEASE')
        disease_lexicon_parser.parse_lexicon()

    @with_setup(my_setup_function, my_teardown_function)
    def test_entity_relations(self):


        p = PipelineConnectors()
        p.init_services_connections()
        loader = Loader(p.es, chunk_size=1000)


        if not loader.es.indices.exists(Loader.get_versioned_index(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME)):
            loader.create_new_index(Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME)

        spacyManager = NLPManager()

        disease_patterns = json.load(open(Config.DISEASE_LEXICON_JSON_LOCN))
        gene_patterns = json.load(open(Config.GENE_LEXICON_JSON_LOCN))


        # over_simplified_text = 'Molecular genetics and developmental studies have identified that ADRA1A , ARHGEF10 contribute' \
        #                        ' to schizophrenia'

        over_simplified_text = 'The frequency of ADRA1A was 10-fold greater in African-Americans than in Caucasians, ' \
                              'but was not associated with schizophrenia'



        doc = spacyManager.tokenizeText(over_simplified_text)
        # disease_matches = spacyManager.findEntityMatches(disease_patterns, doc)
        # gene_matches = spacyManager.findEntityMatches(gene_patterns, doc)
        # spacyManager.extract_entity_relations_by_verb(disease_matched_labels, gene_matched_labels,doc)


        '''Navigating the dependency parse tree'''
        #Only for testing purpose
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
        relations = spacyManager.extract_entity_relations_by_verb(disease_matched_labels, gene_matched_labels, doc)



if __name__ == '__main__':
    unittest.main()
