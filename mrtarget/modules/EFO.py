import logging
from collections import OrderedDict
from tqdm import tqdm 
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.connection import PipelineConnectors
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.modules.Ontology import OntologyClassReader, DiseaseUtils
from rdflib import URIRef
from mrtarget.Settings import Config

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)

'''
Module to Fetch the EFO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an EFO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''

class EfoActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

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
                 loader,):
        self.loader = loader
        self.efos = OrderedDict()

    def process_all(self):
        self._process_ontology_data()
        self._store_efo()

    def _process_ontology_data(self):

        self.disease_ontology = OntologyClassReader()
        self.disease_ontology.load_open_targets_disease_ontology()
        '''
        Get all phenotypes
        '''
        utils = DiseaseUtils()
        disease_phenotypes = utils.get_disease_phenotypes(self.disease_ontology)

        for uri,label in self.disease_ontology.current_classes.items():
            properties = self.disease_ontology.parse_properties(URIRef(uri))
            definition = ''
            if 'http://www.ebi.ac.uk/efo/definition' in properties:
                definition = ". ".join(properties['http://www.ebi.ac.uk/efo/definition'])
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

class DiseaseGraph:
    """
    A DAG of disease nodes whose elements are instances of class DiseaseNode
    Input: g - an RDFLib-generated ConjugativeGraph, i.e. list of RDF triples
    """

    def __init__(self, g):
        self.g = g
        self.root = None
        self.node_map = {}
        self.node_cnt = 0
        self.print_rdf_tree_from_root(g)
        self.make_node_graph(g)

    def print_rdf_tree_from_root(self, g):
        print("STUB for method: print_rdf_tree_from_root()")

    def make_node_graph(self, g):
        print("STUB for method: make_node_graph()")


class DiseaseNode:
    """
    A class representing all triples associated with a particular disease subject
    e.g. asthma: http://www.ebi.ac.uk/efo/EFO_0000270
    and its parents and children
    """

    def __init__(self, name="name", parents = [], children = []):
        self.name = name,
        self.parents = parents
        self.children = children
