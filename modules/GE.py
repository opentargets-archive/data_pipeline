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


def check_cache(filename=None):
    data = None
    if os.path.isfile(filename):
        with open(filename, 'r') as data_file:
            data = json.load(data_file)
        data_file.close()
    return data


def update_cache(data=None, filename=None):
    if data is not None:
        with open(filename, 'w') as outfile:
            json.dump(data, outfile)


# create a file of panel names and use it later
def request_to_panel_app():
    results = check_cache(filename=Config.LIST_PANELS_CACHE)

    if results is None:
        r = requests.get('https://bioinfo.extge.co.uk/crowdsourcing/WebServices/list_panels', params={})
        results = r.json()
        update_cache(data=results, filename=Config.LIST_PANELS_CACHE)

    for item in results['result']:
        yield (item['Name'], item['Panel_Id'])


def get_gene_info(panel_name, panel_id, tsvwriter):
    url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
    logging.debug("get_gene_info : %s"%(panel_id))
    #filename = '/tmp/' + panel_id + '.json'
    filename = os.path.join('/tmp/' + panel_id + '.json')
    results = check_cache(filename=filename)

    if results is None:
        r = requests.get(url, params={"panel_name": panel_name})
        results = r.json()
        update_cache(data=results, filename=filename)

    for item in results['results']:
        if item['EnsembleGeneIds']:
            tsvwriter.writerow([panel_name, panel_id, item['GeneSymbol'], item['EnsembleGeneIds'][0], item['LevelOfConfidence'], item['Phenotypes'], item['Publications'], item['Evidences']])
            #fh.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n"%(panel_name, panel_id, item['GeneSymbol'], item['EnsembleGeneIds'][0], item['LevelOfConfidence'], item['Phenotypes'],  item['Publications'], item['Evidences']))


def execute_ge_request():
    with open('/tmp/all_panel_app_information.csv', 'w') as outfile:
        tsvwriter = csv.writer(outfile, delimiter='\t')
        for panel_name, panel_id in request_to_panel_app():
            get_gene_info(panel_name, panel_id, tsvwriter)


def request_to_search_genes():
    results = check_cache(filename=Config.SEARCH_GENES_ALL_CACHE)
    if results is None:
        url = 'https://bioinfo.extge.co.uk/crowdsourcing/WebServices/search_genes/all/'
        r = requests.get(url)
        results = r.json()
        update_cache(data=results, filename=Config.SEARCH_GENES_ALL_CACHE)
    phenotype_list = []
    for item in results['results']:
        # print(results)
        if item['Phenotypes']:
            if item['LevelOfConfidence'] == 'HighEvidence':
                for element in item['Phenotypes']:
                    new_element = element.encode('utf-8').strip()
                    phenotype_list.append(new_element)
    phenotype_set = set(phenotype_list)
    return phenotype_set


def request_to_zooma(property_value, tsvwriter):
    requests_cache.install_cache('zooma_results_cache_jan', backend='sqlite', expire_after=2000000)
    r = requests.get('http://www.ebi.ac.uk/spot/zooma/v2/api/services/annotate',
                         params={'propertyValue': property_value, 'propertyType': 'phenotype'} )
    if r.encoding == 'UTF-8':
        results = r.json()

        for item in results:
            if item['confidence'] == "HIGH":
                print(property_value.encode('utf-8').decode("utf-8"),
                      item['_links']['olslinks'][0]['semanticTag'].encode('utf-8').decode("utf-8"),
                      item['derivedFrom']['annotatedProperty']['propertyValue'].encode('utf-8').decode("utf-8"))
                property_value.strip('\t')
                tsvwriter.writerow([property_value,item['_links']['olslinks'][0]['semanticTag'],
                      item['derivedFrom']['annotatedProperty']['propertyValue'].encode("utf-8").strip()])


def execute_zooma():
    phenotype_unique_set = request_to_search_genes()
    with open('/tmp/zooma_disease_mapping.csv', 'w') as outfile:
        tsvwriter = csv.writer(outfile, delimiter='\t')
        for phenotype in phenotype_unique_set:
            request_to_zooma(phenotype, tsvwriter)


class GE():

    def __init__(self):
        self.evidence_strings = list()
        self.disease_mappings = dict()

        # use python3 for encoding entire file

    def process_disease_mapping_file(self):
        with open('/tmp/zooma_disease_mapping.csv', 'r') as sample_file:
            #sample_file_reader = csv.reader(sample_file, delimiter='\t')
            for row in sample_file:
                try:
                    (ge_property, correct_mapping, label) = row.split('\t')
                    label = label.strip()
                    ge_property = ge_property.strip()
                    self.disease_mappings[ge_property] = {'uri': correct_mapping, 'label': label}
                except ValueError:
                    logger.debug(self.disease_mappings)
                    logger.debug('Ignoring: malformed line: "{}"'.format(row))
        return self.disease_mappings

        '''
        Read all my panel information from what I extracted before from the web services
        '''

    def process_panel_app_file(self):
        with open('/tmp/all_panel_app_information.csv', 'r') as panel_app_file:

            for line in panel_app_file:
                panel_name, panel_id, gene_symbol, ensemble_gene_ids, level_of_confidence, phenotypes, publications, evidences = line.split('\t')
                phenotypes = re.findall(r"\'(.+?)\'",phenotypes)

                for item in phenotypes:
                    if item in self.disease_mappings:
                        logger.debug(1)
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
                        logger.debug(target)
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
                        obj.validated_against_schema_version = "1.2.3"

                        disease_uri = self.disease_mappings[item]['uri']
                        logger.debug("[%s] => [%s]" % (self.disease_mappings[item]['uri'], disease_uri))

                        obj.unique_association_fields = {"panel_name": panel_name, "original_disease_name": self.disease_mappings[item]['label'], "panel_id": panel_id }
                        obj.target = bioentity.Target(id=[target],
                                                      activity="http://identifiers.org/cttv.activity/predicted_damaging",
                                                      target_type='http://identifiers.org/cttv.target/gene_evidence',
                                                      target_name=gene_symbol)
                        # http://www.ontobee.org/ontology/ECO?iri=http://purl.obolibrary.org/obo/ECO_0000204 -- An evidence type that is based on an assertion by the author of a paper, which is read by a curator.

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

                        obj.disease = bioentity.Disease(id=[disease_uri], source_name=[item], name=[self.disease_mappings[item]['label']])
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
                            #url='GE_LINKOUT_URL/%s/%s' % (panel_id,gene_symbol,),
                            nice_name='Further details in the Genomics England PanelApp')
                        obj.evidence.urls = [linkout]
                        logger.debug("validating: %s"%disease_uri)
                        try:
                            error = obj.validate(logger)
                            if error > 0:
                                logger.error(obj.to_JSON())
                                # sys.exit(1)
                            else:
                                logger.debug("NoError")
                        except NameError:
                            logger.debug("NameError")
                        self.evidence_strings.append(obj)
        return self.evidence_strings

    def write_evidence_strings(self, filename):
        logger.info("Writing Genomics England evidence strings")
        logger.debug(self.evidence_strings)
        with open(filename, 'w') as tp_file:
            n = 0
            for evidence_string in self.evidence_strings:
                n += 1
                #print(evidence_string)
                #print(n)
                logger.info(evidence_string.disease.id[0])
                # get max_phase_for_all_diseases
                try:
                    error = evidence_string.validate(logger)
                    if error == 0:
                        logger.debug("noError")
                    else:
                        logger.error("REPORTING ERROR %i" % n)
                        logger.error(evidence_string.to_JSON(indentation=4))
                    #sys.exit(1)
                except NameError:
                    logger.debug("NameError")
                tp_file.write(evidence_string.to_JSON(indentation=None) + "\n")


def main():
    execute_ge_request()
    execute_zooma()
    ge = GE()
    ge.process_disease_mapping_file()
    ge.process_panel_app_file()
    ge.write_evidence_strings(Config.GE_EVIDENCE_STRING)
    logger.info("---- DONE ------")
    sys.exit(0)


if __name__ == "__main__":
    main()
