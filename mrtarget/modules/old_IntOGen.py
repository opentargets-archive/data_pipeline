import sys
import logging
import datetime
from mrtarget.common import Actions
from mrtarget.Settings import Config, file_or_resource
#from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
import opentargets.model.evidence.mutation as evidence_mutation
from mrtarget.common.ElasticsearchQuery import ESQuery

__copyright__  = "Copyright 2014-2017, Open Targets"
__credits__    = ["Gautier Koscielny", "David Tamborero"]
__license__    = "Apache 2.0"
__version__    = "1.2.7"
__maintainer__ = "ChuangKee Ong"
__email__      = ["gautierk@targetvalidation.org", "ckong@ebi.ac.uk"]
__status__     = "Production"

INTOGEN_RELEASE_DATE = ''
INTOGEN_FILENAME = file_or_resource('intogen_opentargets.tsv')
INTOGEN_EVIDENCE_FILENAME = '/Users/ckong/Desktop/cttv001_intogen-29-08-2017.json'
INTOGEN_SCORE_MAP = { 'A' : 0.75, 'B': 0.5, 'C': 0.25 }
INTOGEN_SCORE_DOC = {
    'A' : 'the gene exhibits several signals of positive selection in the tumor type',
    'B' : 'the gene is already described as a cancer gene and exhibits a signal of positive selection in the tumor type',
    'C' : 'the gene exhibits a signal of positive selection and is functionally connected to the genes with evidence A or B in the tumor type'
}
INTOGEN_ROLE_MAP = {
    'Act' : 'http://identifiers.org/cttv.activity/gain_of_function',
    'LoF' : 'http://identifiers.org/cttv.activity/loss_of_function',
    'Ambiguous': 'http://identifiers.org/cttv.activity/unknown',
    'ambiguous': 'http://identifiers.org/cttv.activity/unknown'
}
INTOGEN_TUMOR_TYPE_EFO_MAP = {
    'ALL' : { 'uri' :'http://www.ebi.ac.uk/efo/EFO_0000220', 'label': 'acute lymphoblastic leukemia' },
    'AML' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000222', 'label' : 'acute myeloid leukemia' },
    'BLCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000292', 'label' : 'bladder carcinoma' },
    'BRCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000305', 'label' : 'breast carcinoma' },
    'CLL' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000095', 'label' : 'chronic lymphocytic leukemia'},
    'CM' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000389', 'label' : 'cutaneous melanoma'},
    'COREAD' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000365', 'label' : 'colorectal adenocarcinoma' },
    'DLBCL': { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000403', 'label' : 'diffuse large B-cell lymphoma' },
    'ESCA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002916', 'label' : 'esophageal carcinoma' },
    'GBM' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000519', 'label' : 'glioblastoma multiforme' },
    'HC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000182', 'label' : 'hepatocellular carcinoma' },
    'HNSC' : { 'uri': 'http://www.ebi.ac.uk/efo/EFO_0000181', 'label' : 'head and neck squamous cell carcinoma' },
    'LGG' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0005543', 'label' : 'brain glioma' },
    'LUAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000571', 'label' : 'lung adenocarcinoma' },
    'LUSC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000708', 'label' : 'squamous cell lung carcinoma' },
    'MB' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002939', 'label' : 'medulloblastoma' },
    'MEN' : { 'uri' : 'http://purl.obolibrary.org/obo/HP_0002858', 'label' : 'Meningioma' },
    'MM' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0001378', 'label' : 'multiple myeloma' },
    'NB' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000621', 'label' : 'neuroblastoma' },
    'NSCLC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0003060' , 'label' : 'non-small cell lung carcinoma' },
    'OV' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0002917', 'label' : 'ovarian serous adenocarcinoma' },
    'PIA' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000272', 'label' : 'astrocytoma' },
    'PAAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_1000044', 'label' : 'pancreatic adenocarcinoma' },
    'PRAD' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000673', 'label' : 'prostate adenocarcinoma' },
    'RCCC' : { 'uri' : 'http://www.ebi.ac.uk/efo/EFO_0000349', 'label' : 'clear cell renal carcinoma' },
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
    'DLBCL': 'diffuse large B cell lymphoma',
    'ESCA' : 'esophageal carcinoma',
    'GBM' : 'glioblastoma multiforme',
    'HC' : 'hepatocarcinoma',
    'HNSC' : 'head and neck squamous cell carcinoma',
    'LGG' : 'lower grade glioma',
    'LUAD' : 'lung adenocarcinoma',
    'LUSC' : 'lung squamous cell carcinoma',
    'MB' : 'medulloblastoma',
    'MEN' : 'meningioma',
    'MM' : 'multiple myeloma',
    'NB' : 'neuroblastoma',
    'NSCLC' : 'non small cell lung carcinoma',
    'OV' : 'serous ovarian adenocarcinoma',
    'PIA' : 'pylocytic astrocytoma',
    'PAAD' : 'pancreas adenocarcinoma',
    'PRAD' : 'prostate adenocarcinoma',
    'RCCC' : 'renal clear cell carcinoma',
    'SCLC' : 'small cell lung carcinoma',
    'STAD' : 'stomach adenocarcinoma',
    'THCA' : 'thyroid carcinoma',
    'UCEC' : 'uterine corpus endometrioid carcinoma'
}

INTOGEN_SYMBOL_MAPPING = {
    'C15orf55' : 'NUTM1',
    'CSDA' : 'YBX3',
    'EIF2C3' : 'AGO3',
    'ERBB2IP' : 'ERBIN',
    'FAM123B' : 'AMER1',
    'HNRPDL' : 'HNRNPDL',
    'MLL' : 'KMT2A',
    'MLL2' : 'KMT2D',
    'MLL3' : 'KMT2C',
    'RQCD1' : 'CNOT9'
}

class IntOGenActions(Actions):
    GENERATE_EVIDENCE = 'generateevidence'

class IntOGen():

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
                    self.symbols[display_name]["ensembl_secondary_id"] = row["id"];
                if "ensembl_secondary_ids" not in self.symbols[display_name]:
                    self.symbols[display_name]["ensembl_secondary_ids"] = []
                self.symbols[display_name]["ensembl_secondary_ids"].append(row["id"])

        self.logger.debug("Loading ES Ensembl finished")

    def process_intogen(self, infile=INTOGEN_FILENAME, outfile=INTOGEN_EVIDENCE_FILENAME):
        self.load_Ensembl()
        self.read_intogen(filename=infile)
        self.write_evidence_strings(filename=outfile)

    def read_intogen(self, filename=INTOGEN_FILENAME):

        records = []

        # the database was created in 2014
        #now = datetime.datetime.now()
        now = datetime.datetime(2014, 12, 1, 8, 30)
        provenance_type = evidence_core.BaseProvenance_Type(
            database=evidence_core.BaseDatabase(
                id="IntOGen Cancer Drivers Database",
                version='2014.12',
                dbxref=evidence_core.BaseDbxref(url="https://www.intogen.org/search", id="IntOGen Cancer Drivers Database", version="2014.12")),
            literature = evidence_core.BaseLiterature(
                references = [ evidence_core.Single_Lit_Reference(lit_id = "http://europepmc.org/abstract/MED/25759023") ]
            )
        )
        error = provenance_type.validate(logging)
        if error > 0:
            self.logger.error(provenance_type.to_JSON(indentation=4))
            sys.exit(1)

        with open(filename, 'r') as intogen_file:
            n = 0
            for line in intogen_file:
                n +=1
                if n>1:

                    (Symbol,Ensg,Tumor_Type,Evidence,Role) = tuple(line.rstrip().split('\t'))

                    resource_score = association_score.Probability(
                        type="probability",
                        method= association_score.Method(
                            description ="IntOGen Driver identification methods as described in Rubio-Perez, C., Tamborero, D., Schroeder, MP., Antolin, AA., Deu-Pons,J., Perez-Llamas, C., Mestres, J., Gonzalez-Perez, A., Lopez-Bigas, N. In silico prescription of anticancer drugs to cohorts of 28 tumor types reveals novel targeting opportunities. Cancer Cell 27 (2015), pp. 382-396",
                            reference = "http://europepmc.org/abstract/MED/25759023",
                            url = "https://www.intogen.org/about"),
                        value=INTOGEN_SCORE_MAP[Evidence])

                    evidenceString = opentargets.Literature_Curated()
                    evidenceString.validated_against_schema_version = Config.EVIDENCEVALIDATION_SCHEMA
                    evidenceString.access_level = "public"
                    evidenceString.type = "somatic_mutation"
                    evidenceString.sourceID = "intogen"
                    evidenceString.unique_association_fields = {}
                    evidenceString.unique_association_fields['projectName'] = 'IntOGen Cancer Drivers Database'
                    evidenceString.unique_association_fields['symbol'] = Symbol
                    evidenceString.unique_association_fields['tumor_type_acronym'] = Tumor_Type
                    evidenceString.unique_association_fields['tumor_type'] = INTOGEN_TUMOR_TYPE_MAP[Tumor_Type]
                    evidenceString.unique_association_fields['evidence_level'] = Evidence
                    evidenceString.unique_association_fields['role'] = Role
                    evidenceString.unique_association_fields['role_description'] = INTOGEN_ROLE_MAP[Role]
                    evidenceString.unique_association_fields['method'] = 'OncodriveROLE'
                    evidenceString.unique_association_fields['method_description'] = 'Classifying cancer driver genes into Loss of Function and Activating roles'

                    target_type = 'http://identifiers.org/cttv.target/gene_evidence'

                    '''
                        target information (root.target.target_type is required)
                        get the ensembl gene id from the symbol (mapping from 2014 won't work)
                    '''
                    ensembl_gene_id = None
                    if Symbol in INTOGEN_SYMBOL_MAPPING:
                        Symbol = INTOGEN_SYMBOL_MAPPING[Symbol]

                    if Symbol in self.symbols:
                        record = self.symbols[Symbol]
                        if "ensembl_primary_id" in record:
                            ensembl_gene_id = record["ensembl_primary_id"]
                        elif "ensembl_secondary_ids" in record:
                            ensembl_gene_id = record["ensembl_secondary_ids"][0]
                        else:
                            self.logger.error("%s is in Ensembl but could not find it"%Symbol)
                            continue
                    else:
                        self.logger.error("%s is not found in Ensembl" % Symbol)
                        continue

                    # id = [ "http://identifiers.org/ensembl/{0}".format(ensembl_gene_id) ], #["http://identifiers.org/ensembl/{0}".format(Ensg)],
                    evidenceString.target = bioentity.Target(
                        id= "http://identifiers.org/ensembl/{0}".format(ensembl_gene_id),
                                            target_name = Symbol,
                                            activity=INTOGEN_ROLE_MAP[Role],
                                            target_type='http://identifiers.org/cttv.target/gene_evidence'
                                            )

#                    id = [INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['uri']],
#                    name = [INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['label']]
                    ''' disease information '''
                    evidenceString.disease = bioentity.Disease(
                                            id = INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['uri'],
                                            name=INTOGEN_TUMOR_TYPE_EFO_MAP[Tumor_Type]['label']
                                            )

                    ''' evidence '''
                    evidenceString.evidence = evidence_core.Literature_Curated()
                    evidenceString.evidence.date_asserted = now.isoformat()
                    evidenceString.evidence.is_associated = True
                    evidenceString.evidence.evidence_codes = [ "http://purl.obolibrary.org/obo/ECO_0000053" ]
                    evidenceString.evidence.provenance_type = provenance_type
                    evidenceString.evidence.resource_score = resource_score

                    linkout = evidence_linkout.Linkout(
                        url = 'https://www.intogen.org/search?gene=%s&cancer=%s'%(Symbol, Tumor_Type),
                        nice_name = 'IntOGen -  %s gene cancer mutations in %s (%s)'%(Symbol, INTOGEN_TUMOR_TYPE_MAP[Tumor_Type], Tumor_Type)
                    )

                    evidenceString.evidence.urls = [ linkout ]

                    # gain_of_function would become "Dominant", loss_of_function would be Recessive
                    inheritance_pattern = 'unknown'
                    if Role == 'Act':
                        inheritance_pattern = 'dominant'
                    elif Role == 'LoF':
                        inheritance_pattern = 'recessive'

                    ''' gene_variant '''
                    mutation = evidence_mutation.Mutation(
                        functional_consequence = 'http://purl.obolibrary.org/obo/SO_0001564',
                        preferred_name = 'gene_variant',
                        inheritance_pattern = inheritance_pattern)

                    evidenceString.evidence.known_mutations = [ mutation ]

                    error = evidenceString.validate(logging)
                    if error > 0:
                        self.logger.error(evidenceString.to_JSON())
                        sys.exit(1)

                    self.evidence_strings.append(evidenceString)


            self.logger.info("%s evidence parsed"%(n-1))
            self.logger.info("%s evidence created"%len(self.evidence_strings))

        intogen_file.close()

    def write_evidence_strings(self, filename=INTOGEN_EVIDENCE_FILENAME):
        self.logger.info("Writing IntOGen evidence strings")
        with open(filename, 'w') as tp_file:
            n = 0
            for evidence_string in self.evidence_strings:
                n+=1
                self.logger.info(evidence_string.disease.id[0])
                # get max_phase_for_all_diseases
                error = evidence_string.validate(logging)
                if error == 0:
                    tp_file.write(evidence_string.to_JSON(indentation=None)+"\n")
                else:
                    self.logger.error("REPORTING ERROR %i" % n)
                    self.logger.error(evidence_string.to_JSON(indentation=4))
                    #sys.exit(1)
        tp_file.close()

def main():
    logger = logging.getLogger(__name__)
    logger.info("Load IntOGen data")

if __name__ == "__main__":
    main()
