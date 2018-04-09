from yapsy.IPlugin import IPlugin
from mrtarget.Settings import Config
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

class CancerBiomarkers(IPlugin):

    '''
      Initiate CancerBiomarker object
    '''
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.loader = None
        self.r_server = None
        self.esquery = None
        self.ensembl_current = {}
        self.symbols = {}
        self.cancerbiomarkers = {}
        self.tqdm_out = None

    def print_name(self):
        self._logger.info("Cancer Biomarkers plugin")

    def merge_data(self, genes, loader, r_server, tqdm_out):

        self.loader = loader
        self.r_server = r_server
        self.tqdm_out = tqdm_out

        try:
            '''
             Parse cancer biomarker data into self.cancerbiomarkers
            '''
            self.build_json(filename=Config.BIOMARKER_FILENAME)

            '''
             Testing: Find out which genes in self.cancerbiomarkers cannot be mapped
            '''
            #for gene in self.cancerbiomarkers:
            #    print(gene)

            '''
             Iterate through all genes and add cancer biomarkers data if gene symbol is present
            '''
            for gene_id, gene in tqdm(genes.iterate(),
                                      desc='Adding Cancer Biomarker data',
                                      unit=' gene',
                                      file=self.tqdm_out):
                '''
                   extend gene with related Cancer Biomarker data
                '''
                if gene.approved_symbol in self.cancerbiomarkers:
                        gene.cancerbiomarkers = dict()
                        self._logger.info("Adding Cancer Biomarker data to gene %s" % (gene.approved_symbol))
                        gene.cancerbiomarkers = self.cancerbiomarkers[gene.approved_symbol]

        except Exception as ex:
            tb = traceback.format_exc()
            self._logger.error(tb)
            self._logger.error('Error %s' % ex)
            raise ex

    def build_json(self, filename=Config.BIOMARKER_FILENAME):

        with open(filename, 'r') as input:
            n = 0
            for row in input:
                n += 1
                (Alteration, AlterationType, AssayType, Association, Biomarker, Comments, CurationDate, Curator, Drug, DrugFamily, DrugFullName, DrugStatus, EvidenceLevel, Gene, MetastaticTumorType, PrimaryTumorAcronym, PrimaryTumorTypeFullName, Source, TCGIincluded, Targeting, cDNA, gDNA, IndividualMutation, Info, Region, Strand, Transcript, PrimaryTumorType) = tuple(row.rstrip().split('\t'))
                '''
                 Split Primary Tumor Acronym, Gene and Source
                 to separate out multiple entries in these
                '''
                mPrimaryTumorAcronym = PrimaryTumorAcronym.split(";")
                mGene = Gene.split(";")
                mSource = Source.split(";")

                '''
                 Iterate through tumor types, genes and sources
                '''
                for singleTumor in mPrimaryTumorAcronym:
                    for singleGene in mGene:
                        '''
                         If gene has not appeared in biomarker list yet,
                         initialise self.cancerbiomarkers with an empty hash table
                        '''
                        if Gene not in self.cancerbiomarkers:
                            self.cancerbiomarkers[Gene] = dict()
                        for singleSource in mSource:
                            # print(Biomarker + " | "  + DrugFullName + " | " + Association + " | " + singleTumor + " | " + singleGene + " | " + singleSource)
                            '''
                             Assign values to PMID or Reference field(s)
                            '''
                            currPMID = ""
                            currReference = ""
                            # TODO currReference link needs putting together
                            currReferenceLink = ""

                            if "PMID" in singleSource:
                                currPMID = singleSource[5:]
                            else:
                                currReference = singleSource
                            '''
                             Put together infor for each biomarker
                            '''
                            #TODO tumor acroynm needs mapping to EFO
                            line = {
                                "gene": singleGene,
                                "biomarker": Biomarker,
                                "individualbiomarker": IndividualMutation,
                                "association": Association,
                                "drug": Drug,
                                "drugfamily": DrugFamily,
                                "drugfullname": DrugFullName,
                                "disease": PrimaryTumorTypeFullName,
                                "diseaseID": singleTumor,
                                "evidencelevel": EvidenceLevel,
                                "PMID": currPMID,
                                "reference": currReference,
                                "referencelink": currReferenceLink
                            }

                            '''
                             Add data for current Tumor, Gene, Source combianation
                             to self.cancerbiomarkers
                            '''
                            try:
                               self.cancerbiomarkers[Gene]["cancer_biomarkers"].append(line)
                            except KeyError:
                               self.cancerbiomarkers[Gene]["cancer_biomarkers"] = list()
                               self.cancerbiomarkers[Gene]["cancer_biomarkers"].append(line)




