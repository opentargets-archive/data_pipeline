from collections import OrderedDict
import logging
from tqdm import tqdm
from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle
from mrtarget.modules.Ontology import OntologyClassReader, DiseaseUtils
from rdflib import URIRef
from settings import Config

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
                 # id_org=None,
                 definition=""):
        self.code = code
        self.label = label
        self.efo_synonyms = synonyms
        self.phenotypes = phenotypes
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        # self.id_org = id_org
        self.definition = definition
        self.children=[]

    def get_id(self):
        return self.code
        # return get_ontology_code_from_url(self.path_codes[0][-1])

    def creat_suggestions(self):

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

            efo = EFO(code=uri,
                      label=label,
                      synonyms=synonyms,
                      phenotypes=phenotypes,
                      path=self.disease_ontology.classes_paths[uri]['all'],
                      path_codes=self.disease_ontology.classes_paths[uri]['ids'],
                      path_labels=self.disease_ontology.classes_paths[uri]['labels'],
                      definition=definition
                      )
            id = self.disease_ontology.classes_paths[uri]['ids'][0][-1]
            if uri in self.disease_ontology.children:
                efo.children = self.disease_ontology.children[uri]
            self.efos[id] = efo


    def _store_efo(self):

        for efo_id, efo_obj in self.efos.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                            ID=efo_id,
                            body = efo_obj)



class EFOLookUpTable(object):
    """
    A redis-based pickable efo look up table. 
    Allows to grab the EFO saved in ES and load it up in memory/redis so that it can be accessed quickly from multiple processes, reducing memory usage by sharing.
    """

    def __init__(self,
                 es,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7):
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = r_server,
                                            ttl = ttl)
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        if r_server is not None:
            self._load_efo_data(r_server)

    def _load_efo_data(self, r_server = None):
        for efo in tqdm(self._es_query.get_all_diseases(),
                        desc='loading diseases',
                        unit=' diseases',
                        unit_scale=True,
                        total=self._es_query.count_all_diseases(),
                        leave=False,
                        ):
            self.set_efo(efo, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

    def get_efo(self, efo_id, r_server = None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))

    def set_efo(self, efo, r_server = None):
        efo_key = efo['path_codes'][0][-1]
        self._table.set(efo_key,efo, r_server=self._get_r_server(r_server))

    def get_available_gefo_ids(self, r_server = None):
        return self._table.keys(r_server = self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_efo(key, r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server=None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class instantation or at the method level')
        return r_server

    def keys(self):
        return self._table.keys()


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