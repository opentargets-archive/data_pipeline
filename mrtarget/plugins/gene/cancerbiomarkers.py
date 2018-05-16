from yapsy.IPlugin import IPlugin
from mrtarget.Settings import Config
from tqdm import tqdm
import traceback
import logging
logging.basicConfig(level=logging.DEBUG)

BIOMARKER_SOURCE_MAPPINGS = {
    "AACR 2012" : { 'url' : "http://cancerres.aacrjournals.org/content/72/8_Supplement", 'label' : "American Association for Cancer Research Annual Meeting 2012" },
    "AACR 2013" : { 'url' : "http://cancerres.aacrjournals.org/content/73/8_Supplement", 'label' : "American Association for Cancer Research Annual Meeting 2013" },
    "AACR 2014" : { 'url' : "http://cancerres.aacrjournals.org/content/74/19_Supplement", 'label' : "American Association for Cancer Research Annual Meeting 2014" },
    "AACR 2015" : { 'url' : "http://cancerres.aacrjournals.org/content/75/15_Supplement", 'label' : "American Association for Cancer Research Annual Meeting 2015" },
    "AACR 2016" : { 'url' : "http://cancerres.aacrjournals.org/content/76/14_Supplement", 'label' : "American Association for Cancer Research Annual Meeting 2016" },
    "AACR 2017" : { 'url' : "http://www.abstractsonline.com/pp8/#!/4292", 'label' : "American Association for Cancer Research Annual Meeting 2017" },
    "ASCO 2006" : { 'url' : "https://www.uicc.org/2006-american-society-clinical-oncology-asco-annual-meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2006" },
    "ASCO 2011" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2011%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2011" },
    "ASCO 2012" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2012%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2012" },
    "ASCO 2013" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2013%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2013" },
    "ASCO 2014" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2014%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2014" },
    "ASCO 2015" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2015%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2015" },
    "ASCO 2016" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2016%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2016" },
    "ASCO 2017" : { 'url' : "https://meetinglibrary.asco.org/browse-meetings/2017%20ASCO%20Annual%20Meeting", 'label' : "American Society of Clinical Oncology Annual Meeting 2017" },
    "ASCO GI 2015" : { 'url' : "https://meetinglibrary.asco.org/results/Meeting:%222015%20Gastrointestinal%20Cancers%20Symposium%22", 'label' : "American Society of Clinical Oncology - Gastrointestinal Cancers Symposium 2015" },
    "Caris molecular intelligence" : { 'url' : "https://www.carismolecularintelligence.com", 'label' : "Caris Molecule Intelligence" },
    "EBCC10" : { 'url' : "https://www.ecco-org.eu/Events/Past-conferences/EBCC10", 'label' : "Official 10th European Breast Cancer Conference" },
    "ENA 2012" : { 'url' : "https://www.ecco-org.eu/Events/Past-conferences/EORTC_NCI_AACR-2012", 'label' : "EORTC-NCI-AACR Symposium on Molecular Targets and Cancer Therapeutics 2012" },
    "ENA 2014" : { 'url' : "https://www.ecco-org.eu/Events/Past-conferences/EORTC_NCI_AACR_2014", 'label' : "EORTC-NCI-AACR Symposium on Molecular Targets and Cancer Therapeutics 2014" },
    "ENA 2015" : { 'url' : "http://www.aacr.org/Meetings/Pages/MeetingDetail.aspx?EventItemID=52#.WuhHxmbMxTY", 'label' : "EORTC-NCI-AACR Symposium on Molecular Targets and Cancer Therapeutics 2015" },
    "ESMO 2012" : { 'url' : "http://www.esmo.org/Conferences/Past-Conferences/ESMO-2012-Congress", 'label' : "European Society for Medical Oncology 2012 Congress" },
    "ESMO 2013" : { 'url' : "http://www.esmo.org/Conferences/Past-Conferences/European-Cancer-Congress-2013", 'label' : "European Society for Medical Oncology 2013 Congress" },
    "ESMO 2014" : { 'url' : "http://www.esmo.org/Conferences/Past-Conferences/ESMO-2014-Congress", 'label' : "European Society for Medical Oncology 2014 Congress" },
    "ESMO 2015" : { 'url' : "http://www.esmo.org/Conferences/Past-Conferences/European-Cancer-Congress-2015", 'label' : "European Society for Medical Oncology 2015 Congress" },
    "FDA" : { 'url' : "https://www.fda.gov/RegulatoryInformation/Guidances/default.htm", 'label' : "Food and Drug Administration guidelines" },
    "FDA guidelines" : { 'url' : "https://www.fda.gov/RegulatoryInformation/Guidances/default.htm", 'label' : "Food and Drug Administration guidelines" },
    "NCCN" : { 'url' : "https://www.nccn.org/professionals/physician_gls/default.aspx", 'label' : "National Comprehensive Cancer Network guidelines" },
    "NCCN guidelines" : { 'url' : "https://www.nccn.org/professionals/physician_gls/default.aspx", 'label' : "National Comprehensive Cancer Network guidelines" },
    "ASH 2012 (abstr 673)" : { 'url' : "http://www.bloodjournal.org/content/120/21/673", 'label' : "American Society of Hematology Annual Meeting 2012" },
    "ASH 2012 (abstr 48)" : { 'url' : "http://www.bloodjournal.org/content/120/21/48", 'label' : "American Society of Hematology Annual Meeting 2012" },
    "ASH 2015 (Blood 2015 126:2975)" : { 'url' : "http://www.bloodjournal.org/content/126/23/2975", 'label' : "American Society of Hematology Annual Meeting 2015" },
    "EMA" : { 'url' : "http://www.ema.europa.eu/docs/es_ES/document_library/EPAR_-_Product_Information/human/000406/WC500022207.pdf", 'label' : "European Medicines Agency Product Info" },
    "ASH 2014" : { 'url' : "https://ash.confex.com/ash/2014/webprogram/Paper71027.html", 'label' : "American Society of Hematology Annual Meeting 2014" },
    "Neuro Oncology Oct 2016" : { 'url' : "https://academic.oup.com/neuro-oncology/article-abstract/18/suppl_4/iv50/2222864/P08-41-Development-of-a-novel-TERT-targeting?cited-by=yes&legid=neuonc;18/suppl_4/iv50-b", 'label' : "Neuro-Oncology, Volume 18, Issue suppl_4, 1 October 2016, Pages iv50, https://doi.org/10.1093/neuonc/now188.174" },
    "JCO Precision Oncology (PO.16.00054)" : { 'url' : "http://ascopubs.org/doi/abs/10.1200/PO.16.00054", 'label' : "DOI: 10.1200/PO.16.00054 JCO Precision Oncology (published online June 23, 2017)" },
    "JCO Precision Oncology (PO.16.00055)" : { 'url' : "http://ascopubs.org/doi/full/10.1200/PO.16.00055", 'label' : "DOI: 10.1200/PO.16.00055 JCO Precision Oncology (published online June 27, 2017)" }
}

class CancerBiomarkers(IPlugin):

    # Initiate CancerBiomarker object
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
            # Parse cancer biomarker data into self.cancerbiomarkers
            self.build_json(filename=Config.BIOMARKER_FILENAME)

            # Iterate through all genes and add cancer biomarkers data if gene symbol is present
            self._logger.info("Generating Cancer Biomarker data injection")
            for gene_id, gene in tqdm(genes.iterate(),
                                      desc='Adding Cancer Biomarker data',
                                      unit=' gene',
                                      file=self.tqdm_out):
                # Extend gene with related Cancer Biomarker data
                if gene.approved_symbol in self.cancerbiomarkers:
                    gene.cancerbiomarkers = list()
                    self._logger.debug("Adding Cancer Biomarker data to gene %s", gene.approved_symbol)
                    gene.cancerbiomarkers.append(self.cancerbiomarkers[gene.approved_symbol])

        except Exception as ex:
            self._logger.exception(str(ex), exc_info=1)
            raise ex

    def build_json(self, filename=Config.BIOMARKER_FILENAME):

        with open(filename, 'r') as input:
            for n, row in enumerate(input, start=1):

                (Alteration, AlterationType, AssayType, Association, Biomarker, Comments, CurationDate,
                 Curator, Drug, DrugFamily, DrugFullName, DrugStatus, EvidenceLevel,
                 Gene, MetastaticTumorType, PrimaryTumorAcronym, PrimaryTumorTypeFullName,
                 Source, TCGIincluded, Targeting, cDNA, gDNA, IndividualMutation, Info,
                 Region, Strand, Transcript, PrimaryTumorType) = \
                    tuple(row.rstrip().split('\t'))

                # Split Primary Tumor Acronym, Gene and Source to separate out multiple entries
                mSource = map(str.strip, Source.split(";"))
                geneList = list(map(str.strip, Gene.split(";")))
                # If the two genes are identical, only keep one copy to prevent duplication of current biomarker
                if len(geneList)>1:
                    if geneList[0] == geneList[1]:
                        geneList = [geneList[0]]

                # Iterate through genes and sources
                for singleGene in geneList:

                    # If gene has not appeared in biomarker list yet,
                    # initialise self.cancerbiomarkers with an empty list
                    if singleGene not in self.cancerbiomarkers:
                        self.cancerbiomarkers[singleGene] = []

                    # Create empty dictionaries for PMIDs and other references
                    pubmed = []
                    other = []

                    # Go through the references/sources
                    for singleSource in mSource:
                        if "PMID" in singleSource: # If the source is a PMID
                            currPMID = singleSource[5:] # Remove 'PMID:' if necessary
                            pubmed.append({'pmid': currPMID})
                        else: # Else: the source is either a clinical trial or a conference abstract
                            if 'NCT' in singleSource:
                                other.append({'name' : singleSource, 'link' : 'https://clinicaltrials.gov/ct2/show/' + singleSource, 'description': 'Clinical Trial'})
                            elif singleSource.split(" (")[0] in BIOMARKER_SOURCE_MAPPINGS:
                                other.append({'name': singleSource, 'link': BIOMARKER_SOURCE_MAPPINGS[singleSource.split(" (")[0]]['url'], 'description': BIOMARKER_SOURCE_MAPPINGS[singleSource.split(" (")[0]]['label']})

                    # Put the reference info together for each biomarker
                    myReferences = {"pubmed": pubmed, "other": other}

                    #TODO tumor acronym needs mapping to EFO
                    line = {
                        "gene": singleGene,
                        "biomarker": Biomarker,
                        "individualbiomarker": IndividualMutation,
                        "association": Association,
                        "drug": Drug,
                        "drugfamily": DrugFamily,
                        "drugfullname": DrugFullName,
                        "disease": PrimaryTumorTypeFullName,
                        "diseaseID": PrimaryTumorAcronym,
                        "evidencelevel": EvidenceLevel,
                        "references": myReferences
                    }

                    # Add data for current biomarker to self.cancerbiomarkers
                    self.cancerbiomarkers[singleGene].append(line)
