import sys
import httplib
import time
import optparse
import logging
import os
import json
import re
import hashlib
import datetime
from common import Actions
from settings import Config
from EvidenceValidation import EvidenceValidationFileChecker
import elasticsearch
from elasticsearch import Elasticsearch, helpers
from SPARQLWrapper import SPARQLWrapper, JSON
import cttv.model.core as cttv
import cttv.model.bioentity as bioentity
import cttv.model.evidence.phenotype as evidence_phenotype
import cttv.model.evidence.core as evidence_core
import cttv.model.evidence.linkout as evidence_linkout
import cttv.model.evidence.association_score as association_score
import cttv.model.evidence.mutation as evidence_mutation

__author__ = "Gautier Koscielny"
__copyright__ = "Copyright 2014-2016, Open Targets"
__credits__ = ["Gautier Koscielny", "David Tamborero"]
__license__ = "Apache 2.0"
__version__ = "1.2.2"
__maintainer__ = "Gautier Koscielny"
__email__ = "gautierk@targetvalidation.org"
__status__ = "Production"

INTOGEN_RELEASE_DATE = ''
INTOGEN_FILENAME = 'C:\Users\gk680303\github\data_pipeline\resources\intogen_opentargets.tsv'
INTOGEN_SCORE_MAP = { 'A' : 0.75, 'B': 0.5, 'C': 0.25 }
INTOGEN_SCORE_DOC = {
    'A' : 'the gene exhibits several signals of positive selection in the tumor type',
    'B' : 'the gene is already described as a cancer gene and exhibits a signal of positive selection in the tumor type',
    'C' : 'the gene exhibits a signal of positive selection and is functionally connected to the genes with evidence A or B in the tumor type'
}
INTOGEN_ROLE_MAP = {
    'Act' : 'http://identifiers.org/cttv.activity/gain_of_function',
    'LoF' : 'http://identifiers.org/cttv.activity/loss_of_function',
    'Ambiguous': 'http://identifiers.org/cttv.activity/unknown'
}
INTOGEN_TUMOR_TYPE_EFO_MAP = {
    'ALL' : { 'uri' :'http://www.ebi.ac.uk/efo/EFO_0000220', 'label': 'acute lymphoblastic leukemia' },
    'AML' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000222', 'label' : 'acute myeloid leukemia' },
    'BLCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000292', 'label' : 'bladder carcinoma' },
    'BRCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000305', 'label' : 'breast carcinoma' },
    'CLL' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000095', 'label' : 'chronic lymphocytic leukemia'},
    'CM' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000389', 'label' : 'cutaneous melanoma'},
    'COREAD' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000365', 'label' : 'colorectal adenocarcinoma' },
    'DLBC': { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000403', 'label' : 'diffuse large B-cell lymphoma' },
    'ESCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002916', 'label' : 'esophageal carcinoma' },
    'GBM' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000519', 'label' : 'glioblastoma multiforme' },
    'HC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000182', 'label' : 'hepatocellular carcinoma' },
    'HNSC' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000181', 'label' : 'head and neck squamous cell carcinoma' },
    'LGG' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0005543', 'label' : 'brain glioma' },
    'LUAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000571', 'label' : 'lung adenocarcinoma' },
    'LUSC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000708', 'label' : 'squamous cell lung carcinoma' },
    'MB' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002939', 'label' : 'medulloblastoma' },
    'MM' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0001378', 'label' : 'multiple myeloma' },
    'NB' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000621', 'label' : 'neuroblastoma' },
    'NSCLC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0003060' , 'label' : 'non-small cell lung carcinoma' },
    'OV' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002917', 'label' : 'ovarian serous adenocarcinoma' },
    'PA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000272', 'label' : 'astrocytoma' },
    'PAAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_1000044', 'label' : 'pancreatic adenocarcinoma' },
    'PRAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000673', 'label' : 'prostate adenocarcinoma' },
    'RCC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000349', 'label' : 'clear cell renal carcinoma' },
    'SCLC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000702', 'label' : 'small cell lung carcinoma' },
    'STAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000503', 'label' : 'stomach adenocarcinoma' },
    'THCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002892', 'label' : 'thyroid carcinoma' },
    'UCEC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000466', 'label' : 'endometrioid carcinoma' }
}

''' cancer acronyms '''
INTOGEN_TUMOR_TYPE_MAP = {
    'ALL' : 'acute lymphocytic leukemia',
    'AML' : 'acute myeloid leukemia',
    'BLCA' : 'bladder carcinoma',
    'BRCA' : 'breast carcinoma',
    'CLL' : 'chronic lymphocytic leukemia',
    'CM' : 'cutaneous melanoma',
    'COREAD' : 'colorectal adenocarcinoma',
    'DLBC': 'diffuse large B cell lymphoma',
    'ESCA' : 'esophageal carcinoma',
    'GBM' : 'glioblastoma multiforme',
    'HC' : 'hepatocarcinoma',
    'HNSC' : 'head and neck squamous cell carcinoma',
    'LGG' : 'lower grade glioma',
    'LUAD' : 'lung adenocarcinoma',
    'LUSC' : 'lung squamous cell carcinoma',
    'MB' : 'medulloblastoma',
    'MM' : 'multiple myeloma',
    'NB' : 'neuroblastoma',
    'NSCLC' : 'non small cell lung carcinoma',
    'OV' : 'serous ovarian adenocarcinoma',
    'PA' : 'pylocytic astrocytoma',
    'PAAD' : 'pancreas adenocarcinoma',
    'PRAD' : 'prostate adenocarcinoma',
    'RCC' : 'renal clear cell carcinoma',
    'SCLC' : 'small cell lung carcinoma',
    'STAD' : 'stomach adenocarcinoma',
    'THCA' : 'thyroid carcinoma',
    'UCEC' : 'uterine corpus endometrioid carcinoma'
}

class IntOGenActions(Actions):
    GENERATE_EVIDENCE = 'generateevidence'

class IntOGen():

    def __init__(self, es, sparql):
        self.es = es
        self.sparql = sparql
        self.ev = EvidenceValidationFileChecker(self.adapter, self.es, self.sparql)
        self.cache = {}
        self.counter = 0
        self.mmGenes = {}
        self.OMIMmap = {}
        self.hgnc2mgis = {}
        self.mgi2mouse_models = {}
        self.mouse_model2diseases = {}
        self.disease_gene_locus = {}
        self.mouse_models = {}
        self.diseases = {}
        self.hashkeys = {}

    def read_intogen(self):

        records = []

        now = datetime.datetime.now()
        provenance_type= evidence_core.BaseProvenance_Type(database=evidence_core.BaseDatabase(id="IntOGen", version='current'))

        with open(INTOGEN_FILENAME, 'r') as intogen_file:
            n = 0
            for line in intogen_file:
                n +=1
                if n>1:

                    (Symbol,Ensg,Tumor_Type,Evidence,Role) = tuple(line.split('\t'))

                    resource_score = association_score.Probability(type="probability", method= association_score.Method(description ="Pharmaprojects database"), value=INTOGEN_SCORE_MAP[Evidence])

                    evidenceString = cttv.Literature_Curated()
                    evidenceString.validated_against_schema_version = '1.2.2'
                    evidenceString.access_level = "public"
                    evidenceString.type = "somatic_mutation"
                    evidenceString.sourceID = "intogen"
                    evidenceString.unique_association_fields = {}
                    evidenceString.unique_association_fields['projectName'] = 'IntOGen'
                    evidenceString.unique_association_fields['symbol'] = Symbol
                    evidenceString.unique_association_fields['tumor_type_acronym'] = Tumor_Type
                    evidenceString.unique_association_fields['tumor_type'] = INTOGEN_TUMOR_TYPE_MAP[Tumor_Type]
                    evidenceString.unique_association_fields['evidence_level'] = Evidence
                    evidenceString.unique_association_fields['role'] = Role
                    evidenceString.unique_association_fields['role_description'] = INTOGEN_SCORE_DOC[Role]
                    evidenceString.unique_association_fields['method'] = 'OncodriveROLE'
                    evidenceString.unique_association_fields['method_description'] = 'Classifying cancer driver genes into Loss of Function and Activating roles'

                    target_type = 'http://identifiers.org/cttv.target/gene_evidence'

                    ''' target information '''
                    evidenceString.target = bioentity.Target(
                                            id=["http://identifiers.org/ensembl/{0}".format(Ensg)],
                                            target_name = Symbol,
                                            activity=INTOGEN_ROLE_MAP[Role],
                                            )

                    ''' disease information '''
                    evidenceString.disease = bioentity.Disease(
                                            id = [INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['uri']],
                                            name=[INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['label']]
                                            )

                    ''' evidence '''
                    evidenceString.evidence = evidence_core.Literature_Curated()
                    evidenceString.evidence.date_asserted = now.isoformat()
                    evidenceString.evidence.is_associated = True
                    evidenceString.evidence.evidence_codes = [ "http://purl.obolibrary.org/obo/ECO_0000053" ]
                    evidenceString.evidence.provenance_type = provenance_type,
                    evidenceString.evidence.resource_score = resource_score,

                    linkout = evidence_linkout.Linkout(
                        url = 'https://www.intogen.org/search?gene=%s&cancer=%s'%(Symbol, Tumor_Type),
                        nice_name = 'IntOGen -  %s gene cancer mutations in %s (%s)'%(Symbol, INTOGEN_TUMOR_TYPE_MAP[Tumor_Type], Tumor_Type)
                    )

                    evidenceString.evidence.urls = [ linkout ]

                    ''' gene_variant '''
                    mutation = evidence_mutation.Mutation(
                        functional_consequence = 'http://purl.obolibrary.org/obo/SO_0001564',
                        preferred_name = '%s_%s'%(Symbol, Tumor_Type))

                    evidenceString.evidence.known_mutations = [ mutation ]

                    error = evidenceString.validate(logging)
                    if error > 0:
                        logging.error(evidenceString.to_JSON(indentation=4))
                        sys.exit(1)

                    self.evidence_strings.append(evidenceString)


            logging.info(n)


        intogen_file.close()

def main():

    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

    logging.info("Load IntOGen data")
    itg = IntOGen()
    itg.read_intogen('')
    #pp.write_evidence_strings(options.pharmaprojects_json)

if __name__ == "__main__":
    main()