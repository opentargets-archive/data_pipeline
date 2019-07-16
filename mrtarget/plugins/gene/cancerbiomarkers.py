from builtins import str
from builtins import map
from yapsy.IPlugin import IPlugin
from opentargets_urlzsource import URLZSource
import traceback
import logging
import csv
import configargparse

# Not used at the moment, but good to have here to check that the column names are correct
cancerbiomarker_columns = ("Alteration", "AlterationType", "AssayType", "Association", "Biomarker", "Comments",
                           "CurationDate", "Curator", "Drug", "DrugFamily", "DrugFullName", "DrugStatus",
                           "EvidenceLevel", "Gene", "MetastaticTumorType", "PrimaryTumorAcronym",
                           "PrimaryTumorTypeFullName", "Source", "TCGIincluded", "Targeting", "cDNA", "gDNA",
                           "IndividualMutation", "Info", "Region", "Strand", "Transcript", "PrimaryTumorType")

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

BIOMARKER_DISEASE_MAPPINGS = {
    "Acute_lymphoblastic_leukemia" : { 'label' : "Acute lymphoblastic leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000220" },
    "Acute_myeloid_leukemia" : { 'label' : "Acute myeloid leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000222" },
    "Acute_promyelocytic_leukemia" : { 'label' : "Acute promyelocytic leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000224" },
    "Adenoid_cystic_carcinoma" : { 'label' : "Adenoid cystic carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000231" },
    "Anaplastic_oligodendroglioma" : { 'label' : "Anaplastic oligodendroglioma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002501" },
    "Angiosarcoma" : { 'label' : "Angiosarcoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0003968" },
    "Basal_cell_carcinoma" : { 'label' : "Basal cell carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0004193" },
    "Breast_adenocarcinoma" : { 'label' : "Breast adenocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000304" },
    "Cholangiocarcinoma" : { 'label' : "Cholangiocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005221" },
    "Chronic_lymphocytic_leukemia" : { 'label' : "Chronic lymphocytic leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000095" },
    "Chronic_myeloid_leukemia" : { 'label' : "Chronic myeloid leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000339" },
    "Colorectal_adenocarcinoma" : { 'label' : "Colorectal adenocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000365" },
    "Cutaneous_melanoma" : { 'label' : "Cutaneous melanoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000389" },
    "Glioblastoma_multiforme" : { 'label' : "Glioblastoma multiforme", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000519" },
    "Glioma" : { 'label' : "Glioma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005543" },
    "Leukemia" : { 'label' : "Leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000565" },
    "Liposarcoma" : { 'label' : "Liposarcoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000569" },
    "Lung_adenocarcinoma" : { 'label' : "Lung adenocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000571" },
    "Lymphoma" : { 'label' : "Lymphoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000574" },
    "Medulloblastoma" : { 'label' : "Medulloblastoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002939" },
    "Mesothelioma" : { 'label' : "Mesothelioma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000588" },
    "Multiple_myeloma" : { 'label' : "Multiple myeloma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0001378" },
    "Neuroblastoma" : { 'label' : "Neuroblastoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000621" },
    "Neurofibroma" : { 'label' : "Neurofibroma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000622" },
    "Osteosarcoma" : { 'label' : "Osteosarcoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000637" },
    "Pancreas_adenocarcinoma" : { 'label' : "Pancreatic adenocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000044" },
    "Plexiform_neurofibroma" : { 'label' : "Plexiform neurofibroma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000658" },
    "Prostate_adenocarcinoma" : { 'label' : "Prostate adenocarcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000673" },
    "Rhabdomyosarcoma" : { 'label' : "Rhabdomyosarcoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002918" },
    "Sarcoma" : { 'label' : "Sarcoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000691" },
    "Schwannoma" : { 'label' : "Schwannoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000693" },
    "Adrenal_adenoma" : { 'label' : "Adrenocortical adenoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0003104" },
    "Any_cancer_type" : { 'label' : "Any cancer type", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000311" },
    "Atypical_chronic_myeloid_leukemia" : { 'label' : "Atypical chronic myeloid leukemia", 'url' : "http://purl.obolibrary.org/obo/MONDO_0020312" },
    "Biliary_tract_cancer" : { 'label' : "Biliary tract cancer", 'url' : "http://www.ebi.ac.uk/efo/EFO_0003891" },
    "Cervical_squamous_cell_carcinoma" : { 'label' : "Cervical squamous cell carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000172" },
    "Dermatofibrosarcoma" : { 'label' : "Dermatofibrosarcome protuberans", 'url' : "http://www.orpha.net/ORDO/Orphanet_31112" },
    "Eosinophilic_chronic_leukemia" : { 'label' : "Chronic Eosinophilic Leukemia, Not Otherwise Specified", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000178" },
    "Erdheim_Chester_histiocytosis" : { 'label' : "Erdheim-Chester disease", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000926" },
    "Female_germ_cell_tumor" : { 'label' : "Ovarian germ cell tumor", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000419" },
    "Fibrous_histiocytoma" : { 'label' : "Malignant fibrous histiocytoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005561" },
    "Gastrointestinal_stromal" : { 'label' : "Gastrointestinal stromal tumor", 'url' : "http://www.orpha.net/ORDO/Orphanet_44890" },
    "Giant_cell_astrocytoma" : { 'label' : "Subependymal giant cell astrocytoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000272" },
    "Hairy_Cell_leukemia" : { 'label' : "Hairy cell leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000956" },
    "Head_and_neck_cancer" : { 'label' : "Head and neck cancer", 'url' : "http://www.ebi.ac.uk/efo/EFO_0006859" },
    "Head_and_neck_squamous_cell_carcinoma" : { 'label' : "Head and neck squamous cell carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000181" },
    "Hematologic_malignancies" : { 'label' : "Hematological system disease", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005803" },
    "Hepatic_carcinoma" : { 'label' : "Hepatocellular carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000182" },
    "Hyper_eosinophilic_advanced_snydrome" : { 'label' : "Hypereosinophilic syndrome", 'url' : "http://www.ebi.ac.uk/efo/EFO_1001467" },
    "Inflammatory_myofibroblastic" : { 'label' : "Inflammatory myofibroblastic tumor", 'url' : "http://www.orpha.net/ORDO/Orphanet_178342" },
    "Lagerhans_cell_histiocytosis" : { 'label' : "Langerhans Cell Histiocytosis", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000318" },
    "Squamous_cell_lung_carcinoma" : { 'label' : "Squamous cell lung carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000708" },
    "Lymphangioleiomyomatosis" : { 'label' : "Lymphangioleiomyomatosis", 'url' : "http://www.orpha.net/ORDO/Orphanet_538" },
    "Male_germ_cell_tumor" : { 'label' : "Germ cell tumor", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000514" },
    "Malignant_astrocytoma" : { 'label' : "Astrocytoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000272" },
    "Malignant_peripheral_nerve_sheat_tumor" : { 'label' : "Malignant peripheral nerve sheath tumor", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000760" },
    "Malignant_rhabdoid_tumor" : { 'label' : "Malignant rhabdoid tumour", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005701" },
    "Megakaryoblastic_leukemia" : { 'label' : "Adult acute megakaryoblastic leukemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_1001932" },
    "Myelodisplasic_proliferative_syndrome" : { 'label' : "Myelodysplastic proliferative syndrome", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000388" },
    "Myelodisplasic_syndrome" : { 'label' : "Myelodysplastic syndrome", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000198" },
    "Neuroendocrine_tumor" : { 'label' : "Neuroendocrine tumor", 'url' : "http://www.ebi.ac.uk/efo/EFO_1001901" },
    "Non_small_cell_lung_cancer" : { 'label' : "Non-small cell lung cancer", 'url' : "http://www.ebi.ac.uk/efo/EFO_0003060" },
    "NUT_midline_carcinoma" : { 'label' : "NUT midline carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005783" },
    "Pediatric_glioma" : { 'label' : "Pediatric glioma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0005543" },
    "Renal_carcinoma" : { 'label' : "Renal carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002890" },
    "Salivary_glands_tumor" : { 'label' : "Salivary gland tumor", 'url' : "http://purl.obolibrary.org/obo/NCIT_C3361" },
    "Systemic_mastocytosis" : { 'label' : "Systemic mastocytosis", 'url' : "http://purl.obolibrary.org/obo/NCIT_C9235" },
    "Thymic_carcinoma" : { 'label' : "Thymic carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_1000576" },
    "Urinary_tract_carcinoma" : { 'label' : "Urothelial carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0008528" },
    "Waldenstroem_macroglobulinemia" : { 'label' : "Waldenstroem macroglobulinemia", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002616" },
    "B_cell_lymphoma" : { 'label' : "B-cell lymphoma", 'url' : "http://purl.obolibrary.org/obo/HP_0012191" },
    "Myelofibrosis" : { 'label' : "Myelofibrosis", 'url' : "http://purl.obolibrary.org/obo/HP_0011974" },
    "Renal_angiomyolipoma" : { 'label' : "Renal angiomyolipoma", 'url' : "http://purl.obolibrary.org/obo/HP_0006772" },
    "Glioblastoma" : { 'label' : "Glioblastoma multiforme", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000519" },
    "Meningioma" : { 'label' : "Meningioma", 'url' : "http://www.orpha.net/ORDO/Orphanet_2495" },
    "Retinoblastoma" : { 'label' : "Retinoblastoma", 'url' : "http://www.orpha.net/ORDO/Orphanet_790" },
    "Mantle_cell_lymphoma" : { 'label' : "Mantle cell lymphoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_1001469" },
    "Myeloma" : { 'label' : "Myeloma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0001378" },
    "Bladder_carcinoma" : { 'label' : "Bladder carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000292" },
    "Cervical_carcinoma" : { 'label' : "Cervical carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0001061" },
    "Endometrial_carcinoma" : { 'label' : "Endometrial carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_1001512" },
    "Gastroesophageal_junction_adenocarcinoma" : { 'label' : "Gastroesophageal junction adenocarcinoma", 'url' : "http://purl.obolibrary.org/obo/NCIT_C9296" },
    "Lung_carcinoma" : { 'label' : "Lung carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0001071" },
    "Ovarian_carcinoma" : { 'label' : "Ovarian carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0001075" },
    "Pancreatic_carcinoma" : { 'label' : "Pancreatic carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002618" },
    "Solid_tumors" : { 'label' : "Solid tumors", 'url' : "http://purl.obolibrary.org/obo/NCIT_C9292" },
    "Gastric_carcinoma" : { 'label' : "Gastric carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0000178" },
    "Thyroid_carcinoma" : { 'label' : "Thyroid carcinoma", 'url' : "http://www.ebi.ac.uk/efo/EFO_0002892" }
}


class CancerBiomarkers(IPlugin):

    # Initiate CancerBiomarker object
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self.ensembl_current = {}
        self.symbols = {}
        self.cancerbiomarkers = {}

    def merge_data(self, genes, es, r_server, data_config, es_config):

        # Parse cancer biomarker data into self.cancerbiomarkers
        self.build_json(filename=data_config.biomarker)

        # Iterate through all genes and add cancer biomarkers data if gene symbol is present
        self._logger.info("Generating Cancer Biomarker data injection")
        for gene_id, gene in genes.iterate():
            # Extend gene with related Cancer Biomarker data
            if gene.approved_symbol in self.cancerbiomarkers:
                gene.cancerbiomarkers=self.cancerbiomarkers[gene.approved_symbol]


    def build_json(self, filename):

        with URLZSource(filename).open() as r_file:
            # fieldnames=cancerbiomarker_columns not used at the moment
            for i, row in enumerate(csv.DictReader(r_file, dialect='excel-tab'), start=1):

                Source = row["Source"]
                Gene = row["Gene"]
                IndividualMutation = row["IndividualMutation"]
                PrimaryTumorTypeFullName = row["PrimaryTumorTypeFullName"]

                # Split Source and Gene to separate out multiple entries
                mSource = [x.strip() for x in Source.split(";")]
                geneList = [x.strip() for x in Gene.split(";")]
                # If the two genes are identical, only keep one copy to prevent duplication of current biomarker
                if len(geneList)>1:
                    if geneList[0] == geneList[1]:
                        geneList = [geneList[0]]

                # Edit IndividualMutation from eg. FGFR3:V555M to FGFR3 (V555M)
                # Replace ':' with ' (' and add ')' at the end
                if ":" in IndividualMutation:
                    IndividualMutation = IndividualMutation.replace(':',' (')+')'

                # Get Tumor type names and EFO IDs/links
                PrimaryTumorTypeFullName = PrimaryTumorTypeFullName.replace(' ', '_')
                PrimaryTumorTypeFullName = PrimaryTumorTypeFullName.replace('-', '_')
                TumorNames=""
                TumorIDs=""
                if ";" in PrimaryTumorTypeFullName:
                    TumorTypes = PrimaryTumorTypeFullName.split(";")
                    diseases = []
                    for TumorType in TumorTypes:
                        diseases.append({'label': BIOMARKER_DISEASE_MAPPINGS[TumorType]['label'], 'id': (BIOMARKER_DISEASE_MAPPINGS[TumorType]['url']).split('/')[-1]})
                else:
                    diseases = [{'label':BIOMARKER_DISEASE_MAPPINGS[PrimaryTumorTypeFullName]['label'], 'id': (BIOMARKER_DISEASE_MAPPINGS[PrimaryTumorTypeFullName]['url']).split('/')[-1]}]

                # Iterate through genes and sources
                for singleGene in geneList:
                    # Replace 3 gene symbols with their approved_symbol (C15orf55=NUTM1, MLL=KMT2A, MLL2=KMT2D)
                    if singleGene == 'C15orf55':
                        singleGene = 'NUTM1'
                    elif singleGene == 'MLL':
                        singleGene = 'KMT2A'
                    elif singleGene == 'MLL2':
                        singleGene = 'KMT2D'
                    # If gene has not appeared in biomarker list yet,
                    # initialise self.cancerbiomarkers with an empty list
                    if singleGene not in self.cancerbiomarkers:
                        self.cancerbiomarkers[singleGene] = []

                    # Create empty lists for PMIDs and other references
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

                    line = {
                        "gene": singleGene,
                        "biomarker": row["Biomarker"],
                        "individualbiomarker": row["IndividualMutation"],
                        "association": row["Association"],
                        "drug": row["Drug"],
                        "drugfamily": row["DrugFamily"],
                        "drugfullname": row["DrugFullName"],
                        "diseases": diseases,
                        "evidencelevel": row["EvidenceLevel"],
                        "references": myReferences
                    }
                    # Add data for current biomarker to self.cancerbiomarkers
                    self.cancerbiomarkers[singleGene].append(line)