import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
from mrtarget.common import Actions
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from mrtarget.modules.Ontology import OntologyClassReader
from tqdm import tqdm
from elasticsearch.exceptions import NotFoundError
import logging
import datetime
import csv
import re
import requests
import urllib2
import json
import hashlib
#import requests_cache
import urlparse
from mrtarget.Settings import Config
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

logger = logging.getLogger(__name__)

class GenomicsEnglandActions(Actions):
    UPDATE_GEL = 'updatecache'
    GENERATE_EVIDENCE = 'generateevidence'


class GE(object):

    def __init__(self, es=None, r_server=None):
        self.es = es
        self.r_server = r_server
        self.lookup_data = None
        self.hashkeys = dict()
        self.hpo = OntologyClassReader()
        self.hpo_labels = dict()
        self.efo_labels = dict()
        self.panel_app_info = list()
        self.high_confidence_mappings = dict()
        self.other_zooma_mappings = dict()
        self.omim_to_efo_map = dict()
        self.symbol2ensembl = dict()
        self.zooma_to_efo_map = dict()
        self.phenotype_set = set()
        self.evidence_strings = list()
        self.map_omim = dict()
        self.fh_zooma_high = None
        self.fh_zooma_low = None
        self._logger = logging.getLogger(__name__)
        self._logger.warning("GE init")
        self.map_strings = dict()


    def _get_symbol_to_ensembl_id_gene_mapping(self):

        lookup_data_types = (LookUpDataType.TARGET, LookUpDataType.EFO)
        self.lookup_data = LookUpDataRetriever(self.es,
                                               self.r_server,
                                               data_types=lookup_data_types,
                                               autoload=True
                                               ).lookup
        for ensembl_id in self.lookup_data.available_genes.get_available_gene_ids():
            gene = self.lookup_data.available_genes.get_gene(ensembl_id,
                                                             self.r_server)
            self.symbol2ensembl[gene["approved_symbol"]] = gene["ensembl_gene_id"]

        for k,v in self.lookup_data.efo_ontology.current_classes.items():
            self.efo_labels[v.lower()] = k

    def process_all(self):
        self._logger.warning("Process all")

        #self.hpo.load_hpo_classes()
        for k, v in self.hpo.current_classes:
            self.hpo_labels[v.lower()] = k
        self._get_symbol_to_ensembl_id_gene_mapping()
        self.get_omim_to_efo_mappings()
        self.get_opentargets_zooma_to_efo_mappings()
        self.execute_ge_request()
        self.use_zooma()
        self.process_panel_app_file()
        self.write_evidence_strings(Config.GE_EVIDENCE_STRING)

    def get_omim_to_efo_mappings(self):
        self._logger.info("OMIM to EFO parsing - requesting from URL %s" % Config.OMIM_TO_EFO_MAP_URL)
        req = urllib2.Request(Config.OMIM_TO_EFO_MAP_URL)
        response = urllib2.urlopen(req)
        self._logger.info("OMIM to EFO parsing - response code %s" % response.code)
        lines = response.readlines()

        for line in lines:
            '''
            omim	efo_uri	efo_label	source	status
            '''
            (omim, efo_uri, efo_label, source, status) = line.split("\t")
            if omim not in self.omim_to_efo_map:
                self.omim_to_efo_map[omim] = []
            self.omim_to_efo_map[omim].append({'efo_uri': efo_uri, 'efo_label': efo_label})

    def get_opentargets_zooma_to_efo_mappings(self):
        self._logger.info("ZOOMA to EFO parsing - requesting from URL %s" % Config.ZOOMA_TO_EFO_MAP_URL)
        req = urllib2.Request(Config.ZOOMA_TO_EFO_MAP_URL)
        response = urllib2.urlopen(req)
        self._logger.info("ZOOMA to EFO parsing - response code %s" % response.code)
        lines = response.readlines()
        n = 0
        for line in lines:
            '''
            STUDY	BIOENTITY	PROPERTY_TYPE	PROPERTY_VALUE	SEMANTIC_TAG	ANNOTATOR	ANNOTATION_DATE
            disease	Amyotrophic lateral sclerosis 1	http://www.ebi.ac.uk/efo/EFO_0000253
            '''
            n +=1
            if n > 1:
                #self._logger.info("[%s]"%line)
                (study, bioentity, property_type, property_value, semantic_tag, annotator, annotation_date) = line.split("\t")
                if property_value not in self.omim_to_efo_map:
                    self.zooma_to_efo_map[property_value.lower()] = []
                self.zooma_to_efo_map[property_value.lower()].append({'efo_uri': semantic_tag, 'efo_label': semantic_tag})

    @staticmethod
    def request_to_panel_app():
        '''
        Makes a request to panel app to get the list of all panels
        :return: tuple of list of panel name and panel id's
        '''
        #requests_cache.install_cache('GE_results_cache_Feb', backend='sqlite', expire_after=3000000)
        r = requests.get('https://bioinfo.extge.co.uk/crowdsourcing/WebServices/list_panels', params={})
        results = r.json()

        for item in results['result']:
            yield (item['Name'], item['Panel_Id'])

    def execute_ge_request(self):
        '''
        Create panel app info list and phenotype set
        :return: Unique phenotype list
        '''
        self._logger.warning("execute_ge_request...")
        phenotype_list = []
        nb_panels = 0
        for panel_name, panel_id in self.request_to_panel_app():
            nb_panels +=1
            self._logger.warning("reading panel %s %s" % (panel_name, panel_id))
            url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
            r = requests.get(url, params={"panel_name": panel_name})
            results = r.json()
            for item in results['results']:

                ensembl_iri = None

                if item['GeneSymbol'] in self.symbol2ensembl:
                    ''' map gene symbol to ensembl '''
                    target = self.symbol2ensembl[item['GeneSymbol']]
                    ensembl_iri = "http://identifiers.org/ensembl/" + target

                if ensembl_iri and item['EnsembleGeneIds'] and item['Phenotypes'] and item['LevelOfConfidence'] == 'HighEvidence':
                    for element in item['Phenotypes']:

                        element = element.rstrip().lstrip().rstrip("?")
                        if len(element) > 0:

                            '''
                            First check whether it's an OMIM identifier
                            '''
                            match_omim = re.match('^(\d{6,})$', element)
                            match_omim2 = re.match('^\s*OMIM:(\d{6,})$', element)
                            if match_omim or match_omim2:
                                if match_omim:
                                    omim_id = match_omim.groups()[0]
                                elif match_omim2:
                                    omim_id = match_omim2.groups()[0]
                                self._logger.info("Found OMIM ID: %s" % (omim_id))
                                if omim_id in self.omim_to_efo_map:
                                    self._logger.info("Maps to EFO")
                                    for mapping in self.omim_to_efo_map[omim_id]:
                                        disease_label = mapping['efo_label']
                                        disease_uri = mapping['efo_uri']
                                        self.panel_app_info.append([panel_name,
                                                                    panel_id,
                                                                    item['GeneSymbol'],
                                                                    item['EnsembleGeneIds'][0],
                                                                    ensembl_iri,
                                                                    item['LevelOfConfidence'],
                                                                    omim_id,
                                                                    item['Publications'],
                                                                    item['Evidences'],
                                                                    [omim_id],
                                                                    disease_uri,
                                                                    disease_label
                                                                    ])
                                self.map_strings = "%s\t%s\t%s\t%s\t%s\t%s"%(panel_name, item['GeneSymbol'], item['LevelOfConfidence'], element, disease_uri, disease_label)
                            else:

                                '''
                                if there is already an OMIM xref to EFO/Orphanet, no need to map
                                '''
                                disease_uri = None
                                disease_label = None
                                is_hpo = False
                                is_efo = False

                                omim_ids = []
                                phenotype_label = None

                                match_hpo_id = re.match('^(.+)\s+(HP:\d+)$', element)
                                match_curly_brackets_omim = re.match('^{([^\}]+)},\s+(\d+)', element)
                                match_no_curly_brackets_omim = re.match('^(.+),\s+(\d{6,})$', element)
                                match_no_curly_brackets_omim_continued = re.match('^(.+),\s+(\d{6,})\s+.*$', element)
                                # Myopathy, early-onset, with fatal cardiomyopathy 611705
                                match_no_curly_brackets_no_comma_omim = re.match('^(.+)\s+(\d{6,})\s*$', element)
                                if element.lower() in self.efo_labels:
                                    disease_uri = self.efo_labels[element.lower()]
                                    disease_label = element
                                    phenotype_label = disease_label
                                    is_efo = True
                                elif element.lower() in self.hpo_labels:
                                    disease_uri = self.hpo_labels[element.lower()]
                                    disease_label = self.hpo.current_classes[disease_uri]
                                    phenotype_label = disease_label
                                    is_hpo = True
                                elif match_hpo_id:
                                    # Ichthyosis HP:64
                                    disease_label = match_hpo_id.groups()[0]
                                    phenotype_label = disease_label
                                    phenotype_id = match_hpo_id.groups()[1]
                                    disease_uri = "http://purl.obolibrary.org/obo/" + phenotype_id.replace(":", "_")
                                    if disease_uri in self.hpo.current_classes:
                                        disease_label = self.hpo.current_classes[disease_uri]
                                        is_hpo = True
                                elif match_curly_brackets_omim:
                                    #[{Pancreatitis, idiopathic}, 167800]
                                    phenotype_label = match_curly_brackets_omim.groups()[0]
                                    omim_ids.append(match_curly_brackets_omim.groups()[1])
                                elif match_no_curly_brackets_omim:
                                    #[{Pancreatitis, idiopathic}, 167800]
                                    phenotype_label = match_no_curly_brackets_omim.groups()[0]
                                    omim_ids.append(match_no_curly_brackets_omim.groups()[1])
                                elif match_no_curly_brackets_omim_continued:
                                    #[{Pancreatitis, idiopathic}, 167800]
                                    phenotype_label = match_no_curly_brackets_omim_continued.groups()[0]
                                    omim_ids.append(match_no_curly_brackets_omim_continued.groups()[1])
                                elif match_no_curly_brackets_no_comma_omim:
                                    #[{Pancreatitis, idiopathic}, 167800]
                                    phenotype_label = match_no_curly_brackets_no_comma_omim.groups()[0]
                                    omim_ids.append(match_no_curly_brackets_no_comma_omim.groups()[1])
                                else:
                                    phenotype_label = element.decode('iso-8859-1').encode('utf-8').strip()
                                    phenotype_label = re.sub(r"\#", "", phenotype_label)
                                    phenotype_label = re.sub(r"\t", "", phenotype_label)
                                    omim_ids = re.findall(r"\d{5}",phenotype_label)
                                    phenotype_label = re.sub(r"\d{5}", "", phenotype_label)
                                    phenotype_label = re.sub(r"\{", "", phenotype_label)
                                    phenotype_label = re.sub(r"\}", "", phenotype_label)

                                self._logger.info("[%s] => [%s]" % (element, phenotype_label))



                                if omim_ids is None:
                                    omim_ids = []

                                self.map_omim[phenotype_label] = omim_ids



                                if not is_hpo and not is_efo and all(l not in self.omim_to_efo_map for l in omim_ids) and phenotype_label.lower() not in self.zooma_to_efo_map:
                                    self._logger.info("Unknown term '%s' with unknown OMIM ID(s): %s"%(phenotype_label, ";".join(omim_ids)))
                                    phenotype_list.append(phenotype_label)

                                    self.panel_app_info.append([panel_name,
                                                                panel_id,
                                                                item['GeneSymbol'],
                                                                item['EnsembleGeneIds'][0],
                                                                ensembl_iri,
                                                                item['LevelOfConfidence'],
                                                                phenotype_label,
                                                                item['Publications'],
                                                                item['Evidences'],
                                                                omim_ids,
                                                                disease_uri,
                                                                disease_label])

                                else:
                                    self._logger.info("THERE IS A MATCH")

                                    if is_hpo or is_efo:
                                        self.panel_app_info.append([panel_name,
                                                                    panel_id,
                                                                    item['GeneSymbol'],
                                                                    item['EnsembleGeneIds'][0],
                                                                    ensembl_iri,
                                                                    item['LevelOfConfidence'],
                                                                    phenotype_label,
                                                                    item['Publications'],
                                                                    item['Evidences'],
                                                                    omim_ids,
                                                                    disease_uri,
                                                                    disease_label])

                                    elif omim_ids and any(l  in self.omim_to_efo_map for l in omim_ids):
                                        for omim_id in omim_ids:
                                            if omim_id in self.omim_to_efo_map:
                                                for mapping in self.omim_to_efo_map[omim_id]:
                                                    disease_label = mapping['efo_label']
                                                    disease_uri = mapping['efo_uri']

                                                    self.panel_app_info.append([panel_name,
                                                                                panel_id,
                                                                                item['GeneSymbol'],
                                                                                item['EnsembleGeneIds'][0],
                                                                                ensembl_iri,
                                                                                item['LevelOfConfidence'],
                                                                                phenotype_label,
                                                                                item['Publications'],
                                                                                item['Evidences'],
                                                                                omim_ids,
                                                                                disease_uri,
                                                                                disease_label])

                                    elif phenotype_label.lower() in self.zooma_to_efo_map:
                                        for mapping in self.zooma_to_efo_map[phenotype_label.lower()]:

                                            disease_label = mapping['efo_label']
                                            disease_uri = mapping['efo_uri']

                                            self.panel_app_info.append([panel_name,
                                                                        panel_id,
                                                                        item['GeneSymbol'],
                                                                        item['EnsembleGeneIds'][0],
                                                                        ensembl_iri,
                                                                        item['LevelOfConfidence'],
                                                                        phenotype_label,
                                                                        item['Publications'],
                                                                        item['Evidences'],
                                                                        omim_ids,
                                                                        disease_uri,
                                                                        disease_label])




                                self.map_strings = "%s\t%s\t%s\t%s\t%s\t%s" % (
                                panel_name, item['GeneSymbol'], item['LevelOfConfidence'], element, disease_uri,
                                disease_label)
            #if nb_panels > 2:
            #    break

            self.phenotype_set = set(phenotype_list)
        return self.phenotype_set

    def request_to_zooma(self, property_value=None):
        '''
        Make a request to Zooma to get correct phenotype mapping and disease label
        :param property_value: Phenotype name from Genomics England
        :return: High confidence mappings .  Writes the output in the input file
        see docs: http://www.ebi.ac.uk/spot/zooma/docs/api.html
        '''
        #requests_cache.install_cache('zooma_results_cache_jan', backend='sqlite', expire_after=3000000)
        self._logger.info("Requesting")
        r = requests.get('http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate',
                             params={'propertyValue': property_value, 'propertyType': 'phenotype'})
        results = r.json()
        for item in results:
            if item['confidence'] == "HIGH":
                self.high_confidence_mappings[property_value] = {
                    'uri': item['_links']['olslinks'][0]['semanticTag'],
                    'label': item['derivedFrom']['annotatedProperty']['propertyValue'],
                    'omim_id': self.map_omim[property_value]
                }

            else:
                self.other_zooma_mappings[property_value] = {
                    'uri': item['_links']['olslinks'][0]['semanticTag'],
                    'label': item['derivedFrom']['annotatedProperty']['propertyValue'],
                    'omim_id': self.map_omim[property_value]
                }

        return self.high_confidence_mappings

    def use_zooma(self):
        '''
        Call request to Zooma function
        :return: None.
        '''


        logger.info("use Zooma")
        for phenotype in self.phenotype_set:
            if phenotype:
                self._logger.info("Mapping '%s' with zooma..."%(phenotype))
                self.request_to_zooma(phenotype)

        with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
            tsv_writer = csv.writer(outfile, delimiter='\t')
            for phenotype, value in self.high_confidence_mappings.items():
                tsv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

        with open(Config.GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT, 'w') as map_file:
            csv_writer = csv.writer(map_file, delimiter='\t')
            for phenotype, value in self.other_zooma_mappings.items():
                csv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

    def process_panel_app_file(self):
        '''
        Core method to create Evidence strings
        :return: None
        '''
        logger.info("Process panel app file")
        now = datetime.datetime.now()


        with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
            tsv_writer = csv.writer(outfile, delimiter='\t')
            for phenotype, value in self.high_confidence_mappings.items():
                tsv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

        mapping_fh = open("/tmp/genomics_england_mapped.txt", 'wb')
        mapping_tsv_writer = csv.writer(mapping_fh, delimiter='\t')
        unmapped_fh = open("/tmp/genomics_england_unmapped.txt", 'wb')
        unmapped_tsv_writer = csv.writer(unmapped_fh, delimiter='\t')

        for row in self.panel_app_info:
            panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri, level_of_confidence, phenotype, publications, evidences, omim_ids, disease_uri, disease_label = row
            if len(omim_ids) > 0 and disease_uri:
                self.generate_single_evidence(panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri, level_of_confidence,
                                              phenotype, publications, evidences, omim_ids, disease_uri, disease_label, now)
                mapping_tsv_writer.writerow([panel_name, panel_id, gene_symbol, phenotype, disease_uri, disease_label])
            elif phenotype in self.high_confidence_mappings:
                disease_label = self.high_confidence_mappings[phenotype]['label']
                disease_uri = self.high_confidence_mappings[phenotype]['uri']
                self.generate_single_evidence(panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri, level_of_confidence, phenotype, publications, evidences, omim_ids, disease_uri , disease_label, now)
                mapping_tsv_writer.writerow([panel_name, panel_id, gene_symbol, phenotype, disease_uri, disease_label])
            elif phenotype.lower() in self.zooma_to_efo_map:
                for item in self.zooma_to_efo_map[phenotype.lower()]:
                    disease_uri = item['efo_uri']
                    disease_label = "N/A"
                    self.generate_single_evidence(panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri, level_of_confidence, phenotype, publications, evidences, omim_ids, disease_uri , disease_label, now)
                    mapping_tsv_writer.writerow(
                        [panel_name, panel_id, gene_symbol, phenotype, disease_uri, disease_label])
            elif disease_uri:
                self.generate_single_evidence(panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri,
                                              level_of_confidence, phenotype, publications, evidences, omim_ids,
                                              disease_uri, disease_label, now)
                mapping_tsv_writer.writerow(
                    [panel_name, panel_id, gene_symbol, phenotype, disease_uri, disease_label])
            else:
                unmapped_tsv_writer.writerow(
                        [panel_name, panel_id, gene_symbol, phenotype])

        mapping_fh.close()
        unmapped_fh.close()

    def generate_single_evidence(self, panel_name, panel_id, gene_symbol, ensemble_gene_ids, ensembl_iri, level_of_confidence, phenotype, publications, evidences, omim_ids, disease_uri , disease_label, now):

        single_lit_ref_list = []
        if publications is not None:
            publications = re.findall(r"\'(.+?)\'", str(publications))
            for paper_set in publications:
                paper_set = re.findall(r"\d{7,12}", paper_set)
                for paper in paper_set:
                    lit_url = "http://europepmc.org/abstract/MED/" + paper
                    single_lit_ref_list.append(evidence_core.Single_Lit_Reference(lit_id=lit_url))

        obj = opentargets.Literature_Curated(type='genetic_literature')
        target = "http://identifiers.org/ensembl/" + ensemble_gene_ids
        provenance_type = evidence_core.BaseProvenance_Type(
            database=evidence_core.BaseDatabase(
                id="Genomics England PanelApp",
                version='v4.1',
                dbxref=evidence_core.BaseDbxref(
                    url="https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/",
                    id="Genomics England PanelApp", version="v4.1")),
            literature=evidence_core.BaseLiterature(
                references=single_lit_ref_list
            )
        )
        obj.access_level = "public"
        obj.sourceID = "genomics_england"
        obj.validated_against_schema_version = "1.2.6"

        obj.unique_association_fields = {"panel_name": panel_name, "original_disease_name": disease_label, "panel_id": panel_id, "target_id": ensembl_iri, "disease_iri": disease_uri }

        hashkey = hashlib.md5(json.dumps(obj.unique_association_fields)).hexdigest()
        if hashkey in self.hashkeys:

            self._logger.warn(
                "Doc {0} - Duplicated evidence string for {1} to disease {2} URI: {3}".format(panel_name,
                                                                                              panel_id, disease_label,
                                                                                              disease_uri))
        else:

            self.hashkeys[hashkey] = obj

            obj.target = bioentity.Target(id=ensembl_iri,
                                          activity="http://identifiers.org/cttv.activity/predicted_damaging",
                                          target_type='http://identifiers.org/cttv.target/gene_evidence',
                                          target_name=gene_symbol)
            if level_of_confidence == 'LowEvidence':
                resource_score = association_score.Probability(
                    type="probability",
                    method=association_score.Method(
                        description="Further details in the Genomics England PanelApp.",
                        reference="NA",
                        url="https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/"),
                value = 0.25)
            elif level_of_confidence == 'HighEvidence':
                resource_score = association_score.Probability(
                    type="probability",
                    method=association_score.Method(
                        description="Further details in the Genomics England PanelApp.",
                        reference="NA",
                        url="https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/"),
                value = 1)
            else :
                resource_score = association_score.Probability(
                    type="probability",
                    method=association_score.Method(
                        description="Further details in the Genomics England PanelApp.",
                        reference="NA",
                        url="https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/"),
                value = 0.5)

            obj.disease = bioentity.Disease(id=disease_uri, source_name=phenotype, name=disease_label)
            obj.evidence = evidence_core.Literature_Curated()
            obj.evidence.is_associated = True
            obj.evidence.evidence_codes = ["http://purl.obolibrary.org/obo/ECO_0000205"]
            obj.evidence.provenance_type = provenance_type
            obj.evidence.date_asserted = now.isoformat()
            obj.evidence.provenance_type = provenance_type
            obj.evidence.resource_score = resource_score
            #specific
            linkout = evidence_linkout.Linkout(
                urlparse.urljoin(Config.GE_LINKOUT_URL, panel_id, gene_symbol),
                nice_name='Further details in the Genomics England PanelApp')
            obj.evidence.urls = [linkout]


    def write_evidence_strings(self, filename):
        '''
        Validate the evidence string file internally and write it to a file
        :param filename: name of the empty input file
        :return: None .  Writes the output in the input file
        '''
        logger.info("Writing Genomics England evidence strings")
        with open(filename, 'w') as tp_file:
            for hashkey, evidence_string in self.hashkeys.items():
                error = evidence_string.validate(logger)
                if error > 1:
                    logger.error(evidence_string.to_JSON(indentation=4))
                tp_file.write(evidence_string.to_JSON(indentation=None) + "\n")


def main():
    ge_object = GE()
    ge_object.execute_ge_request()
    ge_object.use_zooma()
    ge_object.process_panel_app_file()
    ge_object.write_evidence_strings(Config.GE_EVIDENCE_STRING)


if __name__ == "__main__":
    main()
