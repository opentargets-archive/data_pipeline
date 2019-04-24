import logging
from collections import OrderedDict
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader, DiseaseUtils
import opentargets_ontologyutils.efo
from rdflib import URIRef
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll
import simplejson as json
from opentargets_urlzsource import URLZSource
from mrtarget.constants import Const

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
                 therapeutic_codes=[],
                 definition=""):
        self.code = code
        self.label = label
        self.efo_synonyms = synonyms
        self.phenotypes = phenotypes
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        self.therapeutic_labels = therapeutic_labels
        self.therapeutic_codes = therapeutic_codes
        # self.id_org = id_org
        self.definition = definition
        self.children=[]

    def get_id(self):
        return self.code

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


"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, index, doc):
    for efo_id, efo_obj in items:
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = efo_id
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = efo_obj.to_json()

        yield action

class EfoProcess():

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
                 efo_uri, hpo_uri, mp_uri,
                 disease_phenotype_uris,
                 workers_write, queue_write
                 ):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.efo_uri = efo_uri
        self.hpo_uri = hpo_uri
        self.mp_uri = mp_uri
        self.disease_phenotype_uris = disease_phenotype_uris
        self.workers_write = workers_write
        self.queue_write = queue_write

        self.efos = OrderedDict()
        self.logger = logging.getLogger(__name__+".EfoProcess")

    def process_all(self, dry_run):
        self._process_ontology_data()
        self._store_efo(dry_run)

    def _process_ontology_data(self):

        self.disease_ontology = OntologyClassReader()
        opentargets_ontologyutils.efo.load_open_targets_disease_ontology(self.disease_ontology,  self.efo_uri)

        '''
        Get all phenotypes
        '''
        #becuse of opentargets_ontologyutils for legacy iterates over key,uri pairs
        disease_phenotype_uris_counter = enumerate(self.disease_phenotype_uris)

        utils = DiseaseUtils()
        disease_phenotypes = utils.get_disease_phenotypes(self.disease_ontology, self.hpo_uri, self.mp_uri, disease_phenotype_uris_counter)

        #for uri,label in self.disease_ontology.current_classes.items():
        for uri in self.disease_ontology.classes_paths:
            #get the short code form of the uri
            classes_path = self.disease_ontology.classes_paths[uri]
            id = classes_path['ids'][0][-1]
            label = classes_path['labels'][0][-1]
            if uri != classes_path["all"][0][-1]["uri"]:
                raise RuntimeError('mismatch between uri and classes_path["all"][0][-1]["uri"] %s %s' % (uri, classes_path["all"][0][-1]["uri"]))


            properties = self.disease_ontology.parse_properties(URIRef(uri))

            #create a text block definition/description by joining others together
            definition = ''
            if 'http://purl.obolibrary.org/obo/IAO_0000115' in properties:
                definition = ". ".join(properties['http://purl.obolibrary.org/obo/IAO_0000115'])

            #build a set of all the relevant synonyms
            synonyms = set()            
            #exact synonyms
            if 'http://www.geneontology.org/formats/oboInOwl#hasExactSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym'])

            #related synonyms (partially overlapping)
            if 'http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym'])

            #generic synoynms
            if 'http://www.geneontology.org/formats/oboInOwl#hasSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasSynonym'])

            #narrow synonyms
            if 'http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym'])

            #could have http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym, but that is better captured by parent term

            phenotypes = []
            if uri in disease_phenotypes:
                phenotypes = disease_phenotypes[uri]['phenotypes']

            if uri not in self.disease_ontology.classes_paths:
                self.logger.warning("Unable to find %s", uri)
                continue


            therapeutic_labels = self.disease_ontology.therapeutic_labels[uri]
            therapeutic_uris = self.disease_ontology.therapeutic_uris[uri]
            therapeutic_codes = [self.disease_ontology.classes_paths[ta_uri]['ids'][0][-1] for ta_uri in therapeutic_uris]


            efo = EFO(code=uri,
                      label=label,
                      synonyms=synonyms,
                      phenotypes=phenotypes,
                      path=classes_path['all'],
                      path_codes=classes_path['ids'],
                      path_labels=classes_path['labels'],
                      therapeutic_labels=therapeutic_labels,
                      therapeutic_codes=therapeutic_codes,
                      definition=definition
                      )

            if uri in self.disease_ontology.children:
                efo.children = self.disease_ontology.children[uri]

            #logger.debug(str(classes_path['ids']))
            self.logger.debug("done %s %s %s", id, uri, label)

            if id in self.efos:
                self.logger.warning("duplicate %s", id)
                continue
            self.efos[id] = efo

    def _store_efo(self, dry_run):

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):

            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(self.efos.items(), self.es_index, self.es_doc)
            failcount = 0
            if not dry_run:
                for result in elasticsearch.helpers.parallel_bulk(es, actions,
                        thread_count=self.workers_write, queue_size=self.queue_write, 
                        chunk_size=chunk_size):
                    success, details = result
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s failed to index" % failcount)

        
    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):
        self.logger.info("Starting QC")
        #number of EFO terms
        efo_term_count = 0

        #top level terms (i.e. categories)
        efo_top_levels = []

        #terms without a description
        efo_missing_description_count = 0

        #loop over all efo terms and calculate the metrics
        #Note: try to avoid doing this more than once!
        for efo_term in Search().using(es).index(index).query(MatchAll()).scan():
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
