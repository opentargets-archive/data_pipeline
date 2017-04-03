import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
import logging
import csv
import re
import requests
#import requests_cache
import urlparse
from settings import Config
import sys
reload(sys)
sys.setdefaultencoding('utf-8')


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fmt = logging.Formatter('%(levelname)s:%(name)s:%(message)s')
h = logging.StreamHandler()
h.setFormatter(fmt)
logger.addHandler(h)


class GE(object):

    def __init__(self):
        self.panel_app_info = list()
        self.high_confidence_mappings = dict()
        self.other_zooma_mappings = dict()
        self.phenotype_set = set()
        self.evidence_strings = list()
        self.map_omim = dict()

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
        phenotype_list = []
        for panel_name, panel_id in self.request_to_panel_app():
            url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
            r = requests.get(url, params={"panel_name": panel_name})
            results = r.json()
            for item in results['results']:
                if item['EnsembleGeneIds'] and item['Phenotypes'] and item['LevelOfConfidence'] == 'HighEvidence':
                    for element in item['Phenotypes']:
                        new_element = element.decode('iso-8859-1').encode('utf-8').strip()
                        new_element = re.sub(r"\#", "", new_element)
                        new_element = re.sub(r"\t", "", new_element)
                        omim_id = re.findall(r"\d{5}",new_element)
                        new_element = re.sub(r"\d{5}", "", new_element)
                        new_element = re.sub(r"\{", "", new_element)
                        new_element = re.sub(r"\}", "", new_element)
                        phenotype_list.append(new_element)
                        self.map_omim[new_element] = omim_id
                        self.panel_app_info.append([panel_name,
                                                    panel_id,
                                                    item['GeneSymbol'],
                                                    item['EnsembleGeneIds'][0],
                                                    item['LevelOfConfidence'],
                                                    new_element,
                                                    item['Publications'],
                                                    item['Evidences']
                                                    ])
        self.phenotype_set = set(phenotype_list)
        return self.phenotype_set

    def request_to_zooma(self, property_value):
        '''
        Make a request to Zooma to get correct phenotype mapping and disease label
        :param property_value: Phenotype name from Genomics England
        :return: High confidence mappings .  Writes the output in the input file
        '''
        #requests_cache.install_cache('zooma_results_cache_jan', backend='sqlite', expire_after=3000000)
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

        with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
            tsv_writer = csv.writer(outfile, delimiter='\t')
            for phenotype, value in self.high_confidence_mappings.items():
                tsv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

        with open(Config.GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT, 'w') as map_file:
            csv_writer = csv.writer(map_file, delimiter='\t')
            for phenotype, value in self.other_zooma_mappings.items():
                csv_writer.writerow([phenotype, value['uri'], value['label'], value['omim_id']])

        return self.high_confidence_mappings

    def use_zooma(self):
        '''
        Call request to Zooma function
        :return: None.
        '''
        logger.info("use Zooma")
        for phenotype in self.phenotype_set:
            if phenotype:
                self.request_to_zooma(phenotype)

    def process_panel_app_file(self):
        '''
        Core method to create Evidence strings
        :return: None
        '''
        logger.info("Process panel app file")
        for row in self.panel_app_info:
            panel_name, panel_id, gene_symbol, ensemble_gene_ids, level_of_confidence, phenotype, publications, evidences = row
            if phenotype in self.high_confidence_mappings:
                single_lit_ref_list = []
                if publications is not None:
                    publications = re.findall(r"\'(.+?)\'", str(publications))
                    for paper_set in publications:
                        paper_set = re.findall(r"\d{7,12}", paper_set)
                        for paper in paper_set:
                            lit_url = "http://europepmc.org/abstract/MED/" + paper
                            single_lit_ref_list.append(evidence_core.Single_Lit_Reference(lit_id = lit_url))

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
                obj.validated_against_schema_version = "1.2.4"

                disease_uri = self.high_confidence_mappings[phenotype]['uri']
                obj.unique_association_fields = {"panel_name": panel_name, "original_disease_name": self.high_confidence_mappings[phenotype]['label'], "panel_id": panel_id }
                obj.target = bioentity.Target(id=target,
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

                obj.disease = bioentity.Disease(id=disease_uri, source_name=phenotype, name=[self.high_confidence_mappings[phenotype]['label']])
                obj.evidence = evidence_core.Literature_Curated()
                obj.evidence.is_associated = True
                obj.evidence.evidence_codes = ["http://purl.obolibrary.org/obo/ECO_0000205"]
                obj.evidence.provenance_type = provenance_type
                obj.evidence.date_asserted = '2016-11-25'
                obj.evidence.provenance_type = provenance_type
                obj.evidence.resource_score = resource_score
                #specific
                linkout = evidence_linkout.Linkout(
                    urlparse.urljoin(Config.GE_LINKOUT_URL, panel_id, gene_symbol),
                    nice_name='Further details in the Genomics England PanelApp')
                obj.evidence.urls = [linkout]
                error = obj.validate(logger)
                if error > 0:
                    logger.error(obj.to_JSON())
                    # sys.exit(1)
                self.evidence_strings.append(obj)

    def write_evidence_strings(self, filename):
        '''
        Validate the evidence string file internally and write it to a file
        :param filename: name of the empty input file
        :return: None .  Writes the output in the input file
        '''
        logger.info("Writing Genomics England evidence strings")
        with open(filename, 'w') as tp_file:
            for evidence_string in self.evidence_strings:
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
