import sys
import logging
import datetime
from mrtarget.common import Actions
from mrtarget.Settings import Config, file_or_resource
import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
#import opentargets.model.evidence.mutation as evidence_mutation
from mrtarget.common.ElasticsearchQuery import ESQuery

__copyright__ = "Copyright 2014-2017, Open Targets"
__credits__   = ["Francesco Iorio", "Andrea Pierleoni", "ChuangKee Ong"]
__license__   = "Apache 2.0"
__version__   = "1.2.7"
__maintainer__= "ChuangKee Ong"
__email__     = ["ckong@ebi.ac.uk"]
__status__    = "Production"

SLAPENRICH_FILENAME = file_or_resource('slapenrich_opentargets.tsv')
SLAPENRICH_EVIDENCE_FILENAME = '/Users/ckong/Desktop/cttv001_slapenrich-22-09-2017.json'

#INTOGEN_ROLE_MAP = {
#    'Act' : 'http://identifiers.org/cttv.activity/gain_of_function',
#    'LoF' : 'http://identifiers.org/cttv.activity/loss_of_function',
#    'Ambiguous': 'http://identifiers.org/cttv.activity/unknown',
#    'ambiguous': 'http://identifiers.org/cttv.activity/unknown'
#}

TUMOR_TYPE_EFO_MAP = {
    'ALL' : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000220', 'label': 'acute lymphoblastic leukemia'},
    'BLCA': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000292', 'label': 'bladder carcinoma'},
    'BRCA': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000305', 'label': 'breast carcinoma'},
    'CLL' : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000095', 'label': 'chronic lymphocytic leukemia'},
    'DLBC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000403', 'label': 'diffuse large B-cell lymphoma'},
    'ESCA': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0002916', 'label': 'esophageal carcinoma'},
    'GBM' : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000519', 'label': 'glioblastoma multiforme'},
    'HNSC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000181', 'label': 'head and neck squamous cell carcinoma'},
    'KIRC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000349', 'label': 'clear cell renal carcinoma'},
    'LAML': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000222', 'label': 'acute myeloid leukemia'},
    'LGG' : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0005543', 'label': 'brain glioma'},
    'LIHC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000182', 'label': 'hepatocellular carcinoma'},
    'LUAD': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000571', 'label': 'lung adenocarcinoma'},
    'LUSC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000708', 'label': 'squamous cell lung carcinoma'},
    'MB'  : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0002939', 'label': 'medulloblastoma'},
    'MM'  : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0001378', 'label': 'multiple myeloma'},
    'NB'  : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000621', 'label': 'neuroblastoma'},
    'OV'  : {'uri': 'http://www.ebi.ac.uk/efo/EFO_0002917', 'label': 'ovarian serous adenocarcinoma'},
    'PAAD': {'uri': 'http://www.ebi.ac.uk/efo/EFO_1000044', 'label': 'pancreatic adenocarcinoma'},
    'PRAD': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000673', 'label': 'prostate adenocarcinoma'},
    'SCLC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000702', 'label': 'small cell lung carcinoma'},
    'SKCM': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000389', 'label': 'cutaneous melanoma'},
    'STAD': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0000503', 'label': 'stomach adenocarcinoma'},
    'THCA': {'uri': 'http://www.ebi.ac.uk/efo/EFO_0002892', 'label': 'thyroid carcinoma'},
    'UCEC': {'uri': 'http://www.ebi.ac.uk/efo/EFO_1000233', 'label': 'endometrial endometrioid adenocarcinoma'}
}

''' cancer acronyms '''
TUMOR_TYPE_MAP = {
    'ALL' : 'acute lymphocytic leukemia',
    'BLCA': 'bladder carcinoma',
    'BRCA': 'breast carcinoma',
    'CLL' : 'chronic lymphocytic leukemia',
    'DLBC': 'diffuse large B cell lymphoma',
    'ESCA': 'esophageal carcinoma',
    'GBM' : 'glioblastoma multiforme',
    'HNSC': 'head and neck squamous cell carcinoma',
    'KIRC': 'clear cell renal carcinoma',
    'LAML': 'acute myeloid leukemia',
    'LGG' : 'lower grade glioma',
    'LIHC': 'hepatocellular carcinoma',
    'LUAD': 'lung adenocarcinoma',
    'LUSC': 'lung squamous cell carcinoma',
    'MB'  : 'medulloblastoma',
    'MM'  : 'multiple myeloma',
    'NB'  : 'neuroblastoma',
    'OV'  : 'serous ovarian adenocarcinoma',
    'PAAD': 'pancreas adenocarcinoma',
    'PRAD': 'prostate adenocarcinoma',
    'SCLC': 'small cell lung carcinoma',
    'SKCM' : 'cutaneous melanoma',
    'STAD': 'stomach adenocarcinoma',
    'THCA': 'thyroid carcinoma',
    'UCEC': 'endometrial endometrioid adenocarcinoma'
}

SYMBOL_MAPPING = {
    # TODO These symbols does not has Ensembl ID mappings, need alternative mapping
    '''
    i.e 'C15orf55': 'NUTM1',

    ADRBK1
    ADRBK2
    BRE
    C10orf2
    CASC5
    CECR1
    CSRP2BP
    DEFB131
    ERBB2IP
    FAM175A
    FAM175B
    FIGF
    FYB
    GYLTL1B
    IKBKAP
    INADL
    KIAA0101
    KIRREL
    MINA
    MRE11A
    NGFRAP1
    PARK2
    PTRF
    RQCD1
    SEPP1
    SHFM1
    TCEB1
    TCEB2
    TCEB3
    TCEB3B
    TCEB3C
    TCEB3CL
    UFD1L
    VPRBP
    WBSCR17
    WHSC1
    WHSC1L1
    '''
}

class SLAPEnrichActions(Actions):
    GENERATE_EVIDENCE = 'generateevidence'

class SLAPEnrich():
    def __init__(self, es=None, r_server=None):
        self.es = es
        self.r_server = r_server
        self.esquery  = ESQuery(self.es)
        self.evidence_strings = list()
        self.ensembl_current = {}
        self.symbols = {}
        self.logger = logging.getLogger(__name__)

    def load_Ensembl(self):

        self.logger.debug("Loading ES Ensembl {0} assembly genes and non reference assembly".format(
            Config.EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY))

        for row in self.esquery.get_all_ensembl_genes():

            self.ensembl_current[row["id"]] = row
            # put the ensembl_id in symbols too
            display_name = row["display_name"]
            if display_name not in self.symbols:
                self.symbols[display_name] = {}
                self.symbols[display_name]["assembly_name"] = row["assembly_name"]
                self.symbols[display_name]["ensembl_release"] = row["ensembl_release"]
            if row["is_reference"]:
                self.symbols[display_name]["ensembl_primary_id"] = row["id"]
            else:
                if "ensembl_secondary_id" not in self.symbols[display_name] or row["id"] < \
                        self.symbols[display_name]["ensembl_secondary_id"]:
                    self.symbols[display_name]["ensembl_secondary_id"] = row["id"]
                if "ensembl_secondary_ids" not in self.symbols[display_name]:
                    self.symbols[display_name]["ensembl_secondary_ids"] = []
                self.symbols[display_name]["ensembl_secondary_ids"].append(row["id"])

        self.logger.debug("Loading ES Ensembl finished")

    def process_slapenrich(self, infile=SLAPENRICH_FILENAME, outfile=SLAPENRICH_EVIDENCE_FILENAME):
        self.load_Ensembl()
        self.build_evidence(filename=infile)
        self.write_evidence(filename=outfile)

    def build_evidence(self, filename=SLAPENRICH_FILENAME):

        now = datetime.datetime.now()

        '''
            build evidence.provenance_type object
        '''
        provenance_type = evidence_core.BaseProvenance_Type(
            database=evidence_core.BaseDatabase(
                id="SLAPEnrich",
                version='2017.08',
                dbxref=evidence_core.BaseDbxref(url="https://saezlab.github.io/SLAPenrich/", id="SLAPEnrich analysis of TCGA tumor types", version="2017.08")),
            literature = evidence_core.BaseLiterature(
                references = [evidence_core.Single_Lit_Reference(lit_id="http://europepmc.org/abstract/MED/28179366")]
            )
        )
        error = provenance_type.validate(logging)

        if error > 0:
            self.logger.error(provenance_type.to_JSON(indentation=4))
            sys.exit(1)

        with open(filename, 'r') as slapenrich_input:
            n = 0

            for line in slapenrich_input:
                n +=1
                if n>1:
                    '''
                        pval    => pvalue of the SLAPenrichment of the pathway indicated in the cancer/tumor type
                        fdr     => FDR percentage of the SLAPenrichment of the pathway in the cancer/tumor type
                        logOdds => log10 odd ratio (number of patients with mutations in the pathway / number of expected patients with mutations in the pathway)
                        exeeco  => exclusive coverage of the pathway = number patients with mutations in exactly 1 gene in the pathway / number of patients with mutations in at least one gene in the pathway.
                    '''
                    (tumor_type, gene_symbol, mutFreq_dataset, pathway_id, mutFreq_pathway, pval, fdr, logodds, excco) = tuple(line.rstrip().split('\t'))

                    pathway = pathway_id.split(":")
                    pathway_id = pathway[0].rstrip()
                    pathway_desc = pathway[1].rstrip()
                    '''
                        build evidence.resource_score object
                    '''
                    resource_score = association_score.Pvalue(
                        type="pvalue",
                        method=association_score.Method(
                            description="SLAPEnrich analysis of TCGA tumor types as described in Brammeld J et al (2017)",
                            reference  ="http://europepmc.org/abstract/MED/28179366",
                            url="https://saezlab.github.io/SLAPenrich"
                        ),
                        value=float(pval)
                    )

                    evidenceString = opentargets.Literature_Curated()
                    evidenceString.validated_against_schema_version = Config.EVIDENCEVALIDATION_SCHEMA
                    evidenceString.access_level = "public"
                    evidenceString.type = "affected_pathway"
                    evidenceString.sourceID = "slapenrich"
                    '''
                        build unique_association_field object
                    '''
                    evidenceString.unique_association_fields = {}
                    evidenceString.unique_association_fields['symbol'] = gene_symbol
                    evidenceString.unique_association_fields['tumor_type_acronym'] = tumor_type
                    evidenceString.unique_association_fields['tumor_type'] = TUMOR_TYPE_MAP[tumor_type]
                    evidenceString.unique_association_fields['pathway_id'] = 'http://www.reactome.org/PathwayBrowser/#%s' % (pathway_id)
                    evidenceString.unique_association_fields['efo_id'] = TUMOR_TYPE_EFO_MAP[tumor_type]['uri']

                    target_type = 'http://identifiers.org/cttv.target/gene_evidence'
                    ensembl_gene_id = None

                    if gene_symbol in SYMBOL_MAPPING:
                        gene_symbol = SYMBOL_MAPPING[gene_symbol]

                    '''
                        build target object,
                    '''
                    if gene_symbol in self.symbols:
                        record = self.symbols
                        # u'GNB3': {'assembly_name': u'GRCh38', 'ensembl_release': 89,'ensembl_primary_id': u'ENSG00000111664'},
                        if "ensembl_primary_id" in record[gene_symbol]:
                            ensembl_gene_id = record[gene_symbol]["ensembl_primary_id"]
                        elif "ensembl_secondary_ids" in record:
                            ensembl_gene_id = record[gene_symbol]["ensembl_secondary_ids"][0]
                        else:
                            self.logger.error("%s is in Ensembl but cound not find its ensembl_gene_id" %gene_symbol)
                            continue

                        evidenceString.target = bioentity.Target(
                            id="http://identifiers.org/ensembl/{0}".format(ensembl_gene_id),
                            target_name=gene_symbol,
                            #TODO activity is a required field in target object, currently set as unknown
                            activity="http://identifiers.org/cttv.activity/unknown",
                            target_type=target_type
                        )

                        '''
                            build disease object
                        '''
                        evidenceString.disease = bioentity.Disease(
                            id=TUMOR_TYPE_EFO_MAP[tumor_type]['uri'],
                            name=TUMOR_TYPE_EFO_MAP[tumor_type]['label']
                        )

                        '''
                            build evidence object
                        '''
                        evidenceString.evidence = evidence_core.Literature_Curated()
                        evidenceString.evidence.date_asserted = now.isoformat()
                        evidenceString.evidence.is_associated = True
                        #TODO check is this the correct evidence code "computational combinatorial evidence"
                        evidenceString.evidence.evidence_codes = ["http://purl.obolibrary.org/obo/ECO_0000053"]
                        evidenceString.evidence.provenance_type = provenance_type
                        evidenceString.evidence.resource_score = resource_score

                        '''
                            build evidence.url object
                        '''
                        linkout = evidence_linkout.Linkout (
                            url='http://www.reactome.org/PathwayBrowser/#%s'%(pathway_id),
                            nice_name='%s'%(pathway_desc)
                        )

                        evidenceString.evidence.urls = [linkout]

                        error = evidenceString.validate(logging)

                        if error > 0:
                            self.logger.error(evidenceString.to_JSON())
                            sys.exit(1)

                        self.evidence_strings.append(evidenceString)

                    else:
                        self.logger.error("%s is not found in Ensembl" % gene_symbol)

            self.logger.info("%s evidence parsed"%(n-1))
            self.logger.info("%s evidence created"%len(self.evidence_strings))

        slapenrich_input.close()

    def write_evidence(self, filename=SLAPENRICH_EVIDENCE_FILENAME):
        self.logger.info("Writing SLAPEnrich evidence strings")
        with open(filename, 'w') as slapenrich_output:
            n = 0
            for evidence_string in self.evidence_strings:
                n += 1
                self.logger.info(evidence_string.disease.id[0])
                # get max_phase_for_all_diseases
                error = evidence_string.validate(logging)
                if error == 0:
                    slapenrich_output.write(evidence_string.to_JSON(indentation=None)+"\n")
                else:
                    self.logger.error("REPORTING ERROR %i" %n)
                    self.logger.error(evidence_string.to_JSON(indentation=4))
            slapenrich_output.close()

'''
def main():
    import logging
    logger = logging.getLogger(__name__)

if __name__ == "__main__":
    main()
'''
