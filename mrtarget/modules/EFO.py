import logging
from collections import OrderedDict
from mrtarget.common.connection import PipelineConnectors
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader, DiseaseUtils
import opentargets_ontologyutils.efo
from rdflib import URIRef
from mrtarget.Settings import Config

logger = logging.getLogger(__name__)

'''
Module to Fetch the EFO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an EFO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''

def get_ontology_code_from_url(url):
    base_code = url.split('/')[-1]
    if '/identifiers.org/efo/' in url:
        if ('_' not in base_code) and (':' not in base_code):
            return "EFO_"+base_code
    if ('/identifiers.org/orphanet/' in url) and not ("Orphanet_" in base_code):
        return "Orphanet_"+base_code
    if ('/identifiers.org/eco/' in url) and ('ECO:' in base_code):
        return "ECO_"+base_code.replace('ECO:','')
    if ('/identifiers.org/so/' in url) and ('SO:' in base_code):
        return "SO_"+base_code.replace('SO:','')
    if ('/identifiers.org/doid/' in url) and ('ECO:' in base_code):
        return "DOID_"+base_code.replace('SO:','')
    if base_code is None:
        return url
    return base_code

class EFO(JSONSerializable):
    def __init__(self,
                 code='',
                 label='',
                 synonyms=[],
                 phenotypes=[],
                 path=[],
                 path_codes=[],
                 path_labels=[],
                 therapeutic_labels=[],
                 # id_org=None,
                 definition=""):
        self.code = code
        self.label = label
        self.efo_synonyms = synonyms
        self.phenotypes = phenotypes
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        self.therapeutic_labels = therapeutic_labels
        # self.id_org = id_org
        self.definition = definition
        self.children=[]

    def get_id(self):
        return self.code
        # return get_ontology_code_from_url(self.path_codes[0][-1])

    def create_suggestions(self):

        field_order = [self.label,
                       self.code,
                       # self.efo_synonyms,
                       ]

        self._private = {'suggestions' : dict(input = [],
                                              output = self.label,
                                              payload = dict(efo_id = self.get_id(),
                                                             efo_url = self.code,
                                                             efo_label = self.label,),
                                              )
        }

        for field in field_order:
            if isinstance(field, list):
                self._private['suggestions']['input'].extend(field)
            else:
                self._private['suggestions']['input'].append(field)
        self._private['suggestions']['input'].append(self.get_id())


class EfoProcess():

    def __init__(self,
                 loader):
        self.loader = loader
        self.efos = OrderedDict()
        self.logger = logging.getLogger(__name__+".EfoProcess")

    def process_all(self):
        self._process_ontology_data()
        self._store_efo()

    def _process_ontology_data(self):

        self.disease_ontology = OntologyClassReader()
        efo_uri = Config.ONTOLOGY_CONFIG.get('uris','efo')
        opentargets_ontologyutils.efo.load_open_targets_disease_ontology(self.disease_ontology, efo_uri)

        '''
        Get all phenotypes
        '''
        utils = DiseaseUtils()
        uri_hpo = Config.ONTOLOGY_CONFIG.get('uris', 'hpo')
        uri_mp = Config.ONTOLOGY_CONFIG.get('uris', 'mp')
        uri_disease_phenotypes = Config.ONTOLOGY_CONFIG.items('disease_phenotypes_uris')
        disease_phenotypes = utils.get_disease_phenotypes(self.disease_ontology, uri_hpo, uri_mp, uri_disease_phenotypes)

        for uri,label in self.disease_ontology.current_classes.items():
            properties = self.disease_ontology.parse_properties(URIRef(uri))

            #create a text block definition/description by joining others together
            definition = ''
            if 'http://purl.obolibrary.org/obo/IAO_0000115' in properties:
                definition = ". ".join(properties['http://purl.obolibrary.org/obo/IAO_0000115'])

            synonyms = []
            if 'http://www.ebi.ac.uk/efo/alternative_term' in properties:
                synonyms = properties['http://www.ebi.ac.uk/efo/alternative_term']
            phenotypes = []
            if uri in disease_phenotypes:
                phenotypes = disease_phenotypes[uri]['phenotypes']

            therapeutic_labels = [item[0] for item in self.disease_ontology.classes_paths[uri]['labels']]
            therapeutic_labels = self._remove_duplicates(therapeutic_labels)

            efo = EFO(code=uri,
                      label=label,
                      synonyms=synonyms,
                      phenotypes=phenotypes,
                      path=self.disease_ontology.classes_paths[uri]['all'],
                      path_codes=self.disease_ontology.classes_paths[uri]['ids'],
                      path_labels=self.disease_ontology.classes_paths[uri]['labels'],
                      therapeutic_labels=therapeutic_labels,
                      definition=definition
                      )
            id = self.disease_ontology.classes_paths[uri]['ids'][0][-1]
            if uri in self.disease_ontology.children:
                efo.children = self.disease_ontology.children[uri]
            self.efos[id] = efo

    def _remove_duplicates(self, xs):

        newlist = []

        for item in xs:
            if item not in newlist:
                newlist.append(item)
        return newlist

    def _store_efo(self):

        for efo_id, efo_obj in self.efos.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            ID=efo_id,
                            body = efo_obj)
        self.loader.flush_all_and_wait(Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME)
    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):
        self.logger.info("Starting QC")
        #number of EFO terms
        efo_term_count = 0

        #top level terms (i.e. categories)
        efo_top_levels = []

        #terms without a description
        efo_missing_description_count = 0

        #loop over all efo terms and calculate the metrics
        #Note: try to avoid doing this more than once!
        for efo_term in esquery.get_all_diseases():
            efo_term_count += 1

            #path_labels is a list of lists of all paths to the root
            #top level terms will be those with one list of one item that is itself
            if len(efo_term["path_labels"]) == 1:
                if len(efo_term["path_labels"][0]) == 1:
                    efo_top_levels.append(efo_term["label"])

            if efo_term["definition"] == None or len(efo_term["definition"].strip()) == 0:
                efo_missing_description_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["efo.count"] = efo_term_count
        metrics["efo.top"] = sorted(efo_top_levels)
        metrics["efo.top.count"] = len(efo_top_levels)
        metrics["efo.missing_description.count"] = efo_missing_description_count

        #return the metrics to the caller so they can write to file or further compare
        self.logger.info("Finished QC")
        return metrics
