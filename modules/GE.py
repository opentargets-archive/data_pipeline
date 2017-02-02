import opentargets.model.core as opentargets
import opentargets.model.bioentity as bioentity
import opentargets.model.evidence.core as evidence_core
import opentargets.model.evidence.linkout as evidence_linkout
import opentargets.model.evidence.association_score as association_score
import logging
import csv
import re
import requests
import json
import os.path
import requests_cache
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


class GenomicsEnglandRESTClient:

    def __init__(self):
        pass

    def check_cache(self, filename=None):
        '''
        Check if JSON response (Data) from panel_app request is already there (Cached) , else make a new request
        :param filename: file containing response (Data) from panel_app request in JSON format
        :return: Contents of the file -- JSON response (Data) from panel_app request
        '''
        data = None
        if os.path.isfile(filename):
            with open(filename, 'r') as data_file:
                data = json.load(data_file)
            data_file.close()
        return data

    def update_cache(self, data=None, filename=None):
        '''
        If a new request is made to GE panel app REST API , cache it's response data
        :param data: JSON response from Ge panel_app REST API request
        :param filename: name of the file where data is cached
        :return: None. Just updates the cache (JSON data)
        '''
        if data is not None:
            with open(filename, 'w') as outfile:
                json.dump(data, outfile)

    def request_to_panel_app(self):
        '''
        Makes a request to panel app to get the list of all panels
        :return: tuple of list of panel name and panel id's
        '''

        results = self.check_cache(filename=Config.GE_LIST_PANELS_CACHE)

        if results is None:
            r = requests.get('https://bioinfo.extge.co.uk/crowdsourcing/WebServices/list_panels', params={})
            results = r.json()
            self.update_cache(data=results, filename=Config.GE_LIST_PANELS_CACHE)

        for item in results['result']:
            yield (item['Name'], item['Panel_Id'])

    def get_gene_info(self, panel_name, panel_id, tsvwriter):
        '''
        Get all info from GE panel other than Panel id and Name
        :param panel_name: name of the panel in GE panel app
        :param panel_id: id of the panel in GE panel app
        :param tsvwriter: csvwriter object (tab separated) containing the name of the input file
        :return: None.  Writes the output in the input file
        '''
        url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
        logging.debug("get_gene_info : %s"%(panel_id))
        filename = os.path.join('/tmp/' + panel_id + '.json')
        results = self.check_cache(filename=filename)

        if results is None:
            r = requests.get(url, params={"panel_name": panel_name})
            results = r.json()
            self.update_cache(data=results, filename=filename)

        for item in results['results']:
            if item['EnsembleGeneIds']:
                tsvwriter.writerow(
                    [
                        panel_name,
                        panel_id,
                        item['GeneSymbol'],
                        item['EnsembleGeneIds'][0],
                        item['LevelOfConfidence'],
                        item['Phenotypes'],
                        item['Publications'],
                        item['Evidences']
                    ]
                )

    def execute_ge_request(self):
        '''
        Create a file for storing GE panel app information and call get_gene_info function
        :return: None
        '''
        with open(Config.GE_PANEL_APP_INFO, 'w') as outfile:
            tsvwriter = csv.writer(outfile, delimiter='\t')
            for panel_name, panel_id in self.request_to_panel_app():
                self.get_gene_info(panel_name, panel_id, tsvwriter)

    def request_to_search_genes(self):
        '''
        Make a request to GE panel app to get all phenotypes
        :return: Phenotype set (unique elements in phenotype list)
        '''
        results = self.check_cache(filename=Config.GE_SEARCH_GENES_ALL_CACHE)
        if results is None:
            url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
            r = requests.get(url)
            results = r.json()
            self.update_cache(data=results, filename=Config.GE_SEARCH_GENES_ALL_CACHE)
        phenotype_list = []
        for item in results['results']:
            if item['Phenotypes']:
                if item['LevelOfConfidence'] == 'HighEvidence':
                    for element in item['Phenotypes']:
                        new_element = element.decode('iso-8859-1').encode('utf-8').strip()
                        new_element = re.sub(r"\#", "", new_element)
                        new_element = re.sub(r"\t", "", new_element)
                        new_element = re.sub(r",\d{5}$", "", new_element)
                        new_element = re.sub(r"\{", "", new_element)
                        new_element = re.sub(r"\}", "", new_element)
                        phenotype_list.append(new_element)
        phenotype_set = set(phenotype_list)
        return phenotype_set

    def request_to_zooma(self, property_value, tsvwriter):
        '''
        Make a request to Zooma to get correct phenotype mapping and disease label
        :param property_value: Phenotype name from Genomics England
        :param tsvwriter: csvwriter object (tab separated) containing the name of the input file
        :return: None .  Writes the output in the input file
        '''
        requests_cache.install_cache('zooma_results_cache_jan', backend='sqlite', expire_after=2000000)
        r = requests.get('http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate',
                             params={'propertyValue': property_value, 'propertyType': 'phenotype'} )
        results = r.json()
        for item in results:
            if item['confidence'] == "HIGH":
                tsvwriter.writerow([property_value,item['_links']['olslinks'][0]['semanticTag'],
                      item['derivedFrom']['annotatedProperty']['propertyValue']])
            else:
                with open(Config.GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT, 'w') as map_file:
                    csv_writer = csv.writer(map_file, delimiter='\t')
                    csv_writer.writerow([property_value, item['_links']['olslinks'][0]['semanticTag'],
                                        item['derivedFrom']['annotatedProperty']['propertyValue']])

    def execute_zooma(self):
        '''
        Create a file to collect zooma disease mapping info
        :return: None.
        '''
        phenotype_unique_set = self.request_to_search_genes()
        with open(Config.GE_ZOOMA_DISEASE_MAPPING, 'w') as outfile:
            tsvwriter = csv.writer(outfile, delimiter='\t')
            for phenotype in phenotype_unique_set:
                if phenotype:
                    self.request_to_zooma(phenotype, tsvwriter)


class GE():

    def __init__(self):
        self.evidence_strings = list()
        self.disease_mappings = dict()

    def process_disease_mapping_file(self):
        '''
        split the info in zooma disease mapping file and store that info as a dictionary for efficient retrieval
        :return: dictionary of uri (Correct mapping) as keys and disease label as Value
        '''
        with open('/tmp/zooma_disease_mapping.csv', 'r') as sample_file:
            for row in sample_file:
                    (ge_property, correct_mapping, label) = format(row).split('\t')
                    label = label.strip()
                    ge_property = ge_property.strip()
                    self.disease_mappings[ge_property] = {'uri': correct_mapping, 'label': label}
        return self.disease_mappings

    def process_panel_app_file(self):
        '''
        Core method to create Evidence strings
        :return: evidence string objects
        '''
        with open(Config.GE_PANEL_APP_INFO, 'r') as panel_app_file:

            for line in panel_app_file:
                panel_name, panel_id, gene_symbol, ensemble_gene_ids, level_of_confidence, phenotypes, publications, evidences = line.split('\t')
                phenotypes = re.findall(r"\'(.+?)\'",phenotypes)

                for item in phenotypes:
                    if item in self.disease_mappings:
                        single_lit_ref_list = []
                        if publications is not None:
                            publications = re.findall(r"\'(.+?)\'", str(publications))

                            for paper_set in publications:
                                paper_set = re.findall(r"(?<!\d)\d{7,12}(?!\d)", paper_set)
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

                        disease_uri = self.disease_mappings[item]['uri']
                        obj.unique_association_fields = {"panel_name": panel_name, "original_disease_name": self.disease_mappings[item]['label'], "panel_id": panel_id }
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

                        obj.disease = bioentity.Disease(id=disease_uri, source_name=item, name=[self.disease_mappings[item]['label']])
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
        return self.evidence_strings

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
                if error == 0:
                    logger.debug("noError")
                else:
                    logger.error(evidence_string.to_JSON(indentation=4))
                    #sys.exit(1)
                tp_file.write(evidence_string.to_JSON(indentation=None) + "\n")


def main():
    ge_rest_object = GenomicsEnglandRESTClient()
    ge_rest_object.execute_ge_request()
    ge_rest_object.execute_zooma()
    ge = GE()
    ge.process_disease_mapping_file()
    ge.process_panel_app_file()
    ge.write_evidence_strings(Config.GE_EVIDENCE_STRING)
    sys.exit(0)


if __name__ == "__main__":
    main()
