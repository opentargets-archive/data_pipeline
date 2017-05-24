#!/usr/local/bin/python
# encoding: UTF-8
import os
import unittest

import spacy

from modules.LiteratureNLP import PublicationFetcher, PublicationAnalysisSpacy, LiteratureNLPProcess, \
    SentenceAnalysisSpacy, DocumentAnalysisSpacy
from run import PipelineConnectors

import logging


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

        analyser = PublicationAnalysisSpacy(None)
        pmid = '19204725'
        text = 'Chromosome 8p as a potential hub for developmental neuropsychiatric disorders: ' \
               'implications for schizophrenia, autism and cancer. ' \
               'Defects in genetic and developmental processes are thought to contribute susceptibility to autism ' \
               'and schizophrenia. Presumably, owing to etiological complexity identifying susceptibility genes ' \
               'and abnormalities in the development has been difficult. However, the importance of genes within ' \
               'chromosomal 8p region for neuropsychiatric disorders and cancer is well established. There are 484 ' \
               'annotated genes located on 8p; many are most likely oncogenes and tumor-suppressor genes. Molecular ' \
               'genetics and developmental studies have identified 21 genes in this region (ADRA1A, ARHGEF10, CHRNA2, ' \
               'CHRNA6, CHRNB3, DKK4, DPYSL2, EGR3, FGF17, FGF20, FGFR1, FZD3, LDL, NAT2, NEF3, NRG1, PCM1, PLAT, ' \
               'PPP3CC, SFRP1 and VMAT1/SLC18A1) that are most likely to contribute to neuropsychiatric disorders ' \
               '(schizophrenia, autism, bipolar disorder and depression), neurodegenerative disorders (Parkinson\'s' \
               ' and Alzheimer\'s disease) and cancer. Furthermore, at least seven nonprotein-coding RNAs (microRNAs) ' \
               'are located at 8p. Structural variants on 8p, such as copy number variants, microdeletions or ' \
               'microduplications, might also contribute to autism, schizophrenia and other human diseases including' \
               ' cancer. In this review, we consider the current state of evidence from cytogenetic, linkage, ' \
               'association, gene expression and endophenotyping studies for the role of these 8p genes in ' \
               'neuropsychiatric disease. We also describe how a mutation in an 8p gene (Fgf17) results in a ' \
               'mouse with deficits in specific components of social behavior and a reduction in its dorsomedial ' \
               'prefrontal cortex. We finish by discussing the biological connections of 8p with respect to ' \
               'neuropsychiatric disorders and cancer, despite the shortcomings of this evidence.'
        lemmas, noun_chunks, analysed_sentences_count = analyser._spacy_analyser(text)
        logging.info('PMID: %s'%pmid)
        logging.info('LEMMAS: '+str( lemmas.most_common(100)))
        logging.info('NOUN CHUNKS '+str(noun_chunks.most_common(100)))
        self.assertIn('ppp3cc', lemmas)
        self.assertIn('alzheimer', lemmas)
        self.assertIn('micrornas', lemmas)
        self.assertIn('neuropsychiatric disorders', noun_chunks)
        self.assertIn('dorsomedial prefrontal cortex', noun_chunks)
        self.assertIn('tumor-suppressor genes', noun_chunks)
        # self.assertIn('chromosome 8p', noun_chunks)# failing
        # self.assertIn('nonprotein-coding RNAs', noun_chunks)# failing

        pmid = '15316618'
        text = 'Limb girdle muscular dystrophies. ' \
               'Limb girdle muscular dystrophies (LGMDs) are a genetically heterogeneous group of primary myopathies ' \
               'involving progressive weakness and wasting of the muscles in the hip and shoulder girdles, with distal ' \
               'spread to the bulbar or respiratory musculature in rare cases. Depending on the mode of genetic ' \
               'transmission, six autosomal dominant forms (LGMD1A-F, 10-25%) and ten autosomal recessive forms ' \
               '(LGMD2A-J, 75-90%) are currently known. The prevalence of LGMDs is 0.8/100,000. These conditions ' \
               'are caused by mutations in genes encoding for myotilin (5q31, LGMD1A), lamin A/C (1q11-q21.2, LGMD1B), ' \
               'caveolin-3 (3p25, LGMD1C), unknown proteins (7q, LGMD1D, 6q23, LGMD1E, 7q32.1-32.2., LGMD1F), ' \
               'calpain-3 (15q15.1-21.1, LGMD2A), dysferlin (2p13.3-13.1, LGMD2B), gamma-sarcoglycan (13q12, LGMD2C), ' \
               'alpha-sarcoglycan, also known as adhalin (17q12-q21.3, LGMD2D), beta-sarcoglycan (4q12, LGMD2E), ' \
               'delta-sarcoglycan (5q33-q34, LGMD2F), telethonin (17q11-q12, LGMD2G), E3-ubiquitin ligase (9q31-q34.1,' \
               ' LGMD2H), fukutin-related protein (19q13.3, LGMD2I), and titin (2q31, LGMD2J). Cardiac involvement has ' \
               'been described for LGMD1B-E, LGMD2C-G, and LGMD2I. The time of onset varies between early childhood ' \
               'and middle age. There is no male or female preponderance. Disease progression and life expectancy ' \
               'vary widely, even among different members of the same family. The diagnosis is based primarily ' \
               'on DNA analysis. The history, clinical neurological examinations, blood chemistry investigations, ' \
               'electromyography, and muscle biopsy also provide information that is helpful for the diagnosis. No ' \
               'causal therapy is currently available.'
        lemmas, noun_chunks, analysed_sentences_count = analyser._spacy_analyser(text)
        logging.info('PMID: %s' % pmid)
        logging.info('LEMMAS: ' + str(lemmas.most_common(100)))
        logging.info('NOUN CHUNKS ' + str(noun_chunks.most_common(100)))
        self.assertIn('sarcoglycan', lemmas)
        self.assertIn('girdle', lemmas)
        self.assertIn('limb', lemmas)
        self.assertIn('lgmds', lemmas)
        self.assertIn('17q12-q21.3', lemmas)
        self.assertIn('muscular dystrophies', noun_chunks)
        # self.assertIn('E3-ubiquitin ligase', noun_chunks)# failing




class SpacySentenceNLPTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nlp = spacy.load('en_core_web_md')

    def _concept_exists(self,
                        subject,
                        verb,
                        object,
                        concept_list,
                        ignore_case = False):

        for c in concept_list:
            if ignore_case:
                if c['subject'].lower() == subject.lower() and \
                    c['verb'].lower() == verb.lower()  and \
                    c['object'].lower() == object.lower():
                    return True
            else:
                if c['subject'] == subject and \
                    c['verb'] == verb and \
                    c['object'] == object:
                    return True
        return False

    def test_doc(self):
        text =  u'Asthma is a chronic disease characterized by airway inflammation, obstruction and hyperresponsiveness.'

        doc = self.nlp(text)
        sentence =  SentenceAnalysisSpacy(doc)
        sentence.analyse()
        self.assertTrue(self._concept_exists(subject=u'Asthma',
                                             verb=u'to be',
                                             object=u'chronic disease',
                                             concept_list=sentence.concepts))

    def test_span(self):
        text = u'Asthma is a chronic disease characterized by airway inflammation, obstruction and hyperresponsiveness. ' \
               u'Severe asthma affects a small proportion of subjects but results in most of the morbidity, costs and mortality ' \
               u'associated with the disease.'

        doc = self.nlp(text)
        for span in doc.sents:
            sentence = SentenceAnalysisSpacy(span)
            sentence.analyse()
            self.assertTrue(self._concept_exists(subject=u'Asthma',
                                                 verb=u'to be',
                                                 object=u'chronic disease',
                                                 concept_list=sentence.concepts))
            break

    def test_asthma(self):
        text =  u'Asthma is a chronic disease characterized by airway inflammation, obstruction and hyperresponsiveness.'
        expected_noun_phrases = set(['chronic disease', 'airway inflammation, obstruction', 'Asthma', 'hyperresponsiveness'])

        sentence = SentenceAnalysisSpacy(text, self.nlp)
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])

        self.assertTrue(self._concept_exists(subject=u'Asthma',
                                             verb=u'to be',
                                             object=u'chronic disease',
                                             concept_list=sentence.concepts))
        self.assertTrue(self._concept_exists(subject=u'Asthma',
                                             verb=u'to be characterized by',
                                             object=u'hyperresponsiveness',
                                             concept_list=sentence.concepts))
        self.assertTrue(self._concept_exists(subject=u'Asthma',
                                             verb=u'to be characterized by',
                                             object=u'airway inflammation, obstruction',
                                             concept_list=sentence.concepts))

        self.assertEqual(noun_phrases, expected_noun_phrases)

    def test_serum_level(self):
        '''test verb descriptor to be collected'''
        text =  u'The serum levels of CA125, CA15.3, and HE4 were significantly higher in the TTF-1-positive group than in the TTF-1-negative group (p<0.05).'
        expected_noun_phrases = set(['TTF-1-negative group', 'serum levels', 'TTF-1-positive group'])
        sentence = SentenceAnalysisSpacy(text, self.nlp)
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])
        self.assertEqual(noun_phrases, expected_noun_phrases)
        self.assertTrue(self._concept_exists(subject=u'serum levels',
                                             verb=u'to be higher',
                                             object=u'TTF-1-positive group',
                                             concept_list=sentence.concepts))
        self.assertTrue(self._concept_exists(subject=u'serum levels',
                                             verb=u'to be higher than',
                                             object=u'TTF-1-negative group',
                                             concept_list=sentence.concepts))

    def test_hyphen_token(self):
        text =  u'Here we report that the Polo-like kinase PLK1, an essential mitotic kinase regulator, is an important downstream effector of c-ABL in regulating the growth of cervical cancer.'

        sentence = SentenceAnalysisSpacy(text, self.nlp)
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])
        self.assertIn(u'Polo-like kinase PLK1', noun_phrases)
        self.assertIn(u'c-ABL', noun_phrases)

        self.assertTrue(self._concept_exists(subject=u'Polo-like kinase PLK1',
                                             verb=u'to report is',
                                             object=u'important downstream effector of cABL',
                                             concept_list=sentence.concepts)
                                             )

        self.assertTrue(self._concept_exists(subject=u'Polo-like kinase PLK1',
                                             verb=u'to report regulating',
                                             object=u'cervical cancer',
                                             concept_list=sentence.concepts)
                        )
        self.assertTrue(self._concept_exists(subject=u'Polo-like kinase PLK1',
                                             verb=u'to report regulating',
                                             object=u'growt',
                                             concept_list=sentence.concepts)
                        )

    def test_Schistosoma(self):
        text =  u'Studies have suggested that Schistosoma mansoni infection reduces the severity of asthma and prevent atopy.'

        sentence = SentenceAnalysisSpacy(text, self.nlp)
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])
        self.assertIn(u'Schistosoma mansoni infection', noun_phrases)

        self.assertTrue(self._concept_exists(subject=u'Schistosoma mansoni infection',
                                             verb=u'to suggest reduces',
                                             object=u'asthma',
                                             concept_list=sentence.concepts)
                                             )

        self.assertTrue(self._concept_exists(subject=u'Schistosoma mansoni infection',
                                             verb=u'to suggest prevent',
                                             object=u'atopy',
                                             concept_list=sentence.concepts)
                        )

    def test_Fanconi(self):
        text =  u'Fanconi anemia (FA) is a genetic disease characterized by bone marrow failure and increased cancer risk.'
        expected_noun_phrases = set(['bone marrow failure', 'Fanconi anemia', 'cancer risk', 'genetic disease',])
        sentence = SentenceAnalysisSpacy(text, self.nlp)
        self.assertIn(u'FA', sentence.abbreviations)
        self.assertEqual(u'Fanconi anemia', sentence.abbreviations[u'FA'])
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])
        self.assertEqual(noun_phrases, expected_noun_phrases)

        self.assertTrue(self._concept_exists(subject=u'Fanconi anemia',
                                             verb=u'to be characterized by',
                                             object=u'cancer risk',
                                             concept_list=sentence.concepts)
                        )
        self.assertTrue(self._concept_exists(subject=u'genetic disease',
                                             verb=u'to be characterized by',
                                             object=u'cancer risk',
                                             concept_list=sentence.concepts)
                        )

        self.assertTrue(self._concept_exists(subject=u'Fanconi anemia',
                                             verb=u'to be characterized by',
                                             object=u'bone marrow failure',
                                             concept_list=sentence.concepts)
                                             )
        self.assertTrue(self._concept_exists(subject=u'genetic disease',
                                             verb=u'to be characterized by',
                                             object=u'bone marrow failure',
                                             concept_list=sentence.concepts)
                        )
    def alpha_syn(self):
        text =  u'Deubiquitinase Usp8 regulates α-synuclein clearance and modifies its toxicity in Lewy body disease.'
        expected_noun_phrases = set(['Usp8', 'Lewy body disease', 'alpha-synuclein clearance', 'toxicity'])
        sentence = SentenceAnalysisSpacy(text, self.nlp)
        sentence.analyse()
        noun_phrases = set([i.text for i in sentence.noun_phrases])
        self.assertEqual(noun_phrases, expected_noun_phrases)

        self.assertTrue(self._concept_exists(subject=u'Usp8',
                                             verb=u'to regulate',
                                             object=u'alpha-synuclein clearance',
                                             concept_list=sentence.concepts)
                        )
        self.assertTrue(self._concept_exists(subject=u'Usp8',
                                             verb=u'to regulate modifies',
                                             object=u'Lewy body disease',
                                             concept_list=sentence.concepts)
                        )
        self.assertTrue(self._concept_exists(subject=u'Usp8',
                                             verb=u'to regulate modifies',
                                             object=u'toxicity',
                                             concept_list=sentence.concepts)
                        )




class SpacyDocumentNLPTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.nlp = spacy.load('en_core_web_md')


    def test_analyse_all_abstracts(self):
        file_path = 'resources/test_abstract_nlp.txt'
        filedir = os.path.dirname(__file__)
        for abstract in file(os.path.join(filedir, file_path)):
            print abstract.strip()
            parsed_abstract = DocumentAnalysisSpacy(u''+abstract, nlp = self.nlp)
            parsed_abstract.digest()
            # print parsed_abstract.noun_phrase_counter
            print 'Top Noun Phrases:',len(parsed_abstract.noun_phrases_top), parsed_abstract.noun_phrases_top
            print 'Noun Phrases:',len(parsed_abstract.noun_phrases)
            print 'Concepts:',len(parsed_abstract.concepts)
            self.assertLess(len(parsed_abstract.noun_phrases_top), len(parsed_abstract.noun_phrases))



if __name__ == '__main__':
    unittest.main()
