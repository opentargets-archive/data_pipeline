import warnings
from collections import OrderedDict
import logging

import sys

from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.ElasticsearchQuery import ESQuery
from common.PGAdapter import  EFONames, EFOPath, EFOFirstChild
from common.Redis import RedisLookupTablePickle
from modules.Ontology import OntologyClassReader
from settings import Config

__author__ = 'andreap'

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
                 path=[],
                 path_codes=[],
                 path_labels=[],
                 # id_org=None,
                 definition=""):
        self.code = code
        self.label = label
        self.efo_synonyms = synonyms
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
                 adapter,):
        self.adapter=adapter
        self.session=adapter.session
        self.disease_ontology = None
        self.efos = OrderedDict()

    def process_all(self):
        self._process_ontology_data()
        self._store_efo()

    def _process_ontology_data(self):

        self.disease_ontology = OntologyClassReader()
        self.disease_ontology.load_efo_classes_with_paths()
        for uri,label in self.disease_ontology.current_classes.items():
            properties = self.disease_ontology.parse_properties(URIRef(uri))
            definition = ''
            if 'http://www.ebi.ac.uk/efo/definition' in properties:
                definition = ". ".join(properties['http://www.ebi.ac.uk/efo/definition'])
            synonyms = []
            if 'http://www.ebi.ac.uk/efo/alternative_term' in properties:
                synonyms = properties['http://www.ebi.ac.uk/efo/alternative_term']

            efo = EFO(code=uri,
                      label=label,
                      synonyms=synonyms,
                      path=self.disease_ontology.classes_paths[uri]['all'],
                      path_codes=self.disease_ontology.classes_paths[uri]['ids'],
                      path_labels=self.disease_ontology.classes_paths[uri]['labels'],
                      definition=definition
                      )
            id = self.disease_ontology.classes_paths[uri]['ids'][0][-1]
            self.efos[id] = efo

    def _process_efo_data(self):

        # efo_uri = {}
        for row in self.session.query(EFONames).yield_per(1000):
            # if row.uri_id_org:
            #     idorg2efos[row.uri_id_org] = row.uri
            # efo_uri[get_ontology_code_from_url(row.uri)]=row.uri
            synonyms = []
            if row.synonyms != [None]:
                synonyms = sorted(list(set(row.synonyms)))
            self.efos[get_ontology_code_from_url(row.uri)] = EFO(row.uri,
                                     row.label,
                                     synonyms,
                                     # id_org=row.uri_id_org,
                                     definition=row.description,
                                     path_codes= [],
                                     path_labels=[],
                                     path = [])


        for row in self.session.query(EFOPath).yield_per(1000):
            full_path_codes = []
            full_path_labels = []
            efo_code = get_ontology_code_from_url(row.uri)
            if  (efo_code in self.efos) and \
                    (efo_code != 'cttv_root'):
                efo = self.efos[efo_code]
                full_tree_path = row.tree_path[1:]#skip cttv_root
                for node in row.tree_path:
                    if isinstance(node, list):
                        for node_element in node:
                            full_path_codes.append(get_ontology_code_from_url(node_element['uri']))
                            full_path_labels.append(node_element['label'])
                    else:
                        full_path_codes.append(get_ontology_code_from_url(node['uri']))
                        full_path_labels.append(node['label'])
                if 'cttv_root' in full_path_codes:
                    del full_path_codes[full_path_codes.index('cttv_root')]
                    del full_path_labels[full_path_labels.index('CTTV Root')]

                efo.path_codes.append(full_path_codes)
                efo.path_labels.append(full_path_labels)
                efo.path.append(full_tree_path)
                self.efos[efo_code] = efo

        """temporary clean efos with empty path"""
        keys = self.efos.keys()
        for k in keys:
            efo = self.efos[k]
            if not efo.path:
                del self.efos[k]
                logging.warning("removed efo %s since it has an empty path, add it in postgres"%k)

        """temporary drop genetic_disorder uncharectized fields"""
        keys = self.efos.keys()
        for k in keys:
            efo = self.efos[k]
            if k == 'genetic_disorder_uncategorized':
                del self.efos[k]
            else:
                for path in efo.path_codes:
                    if 'genetic_disorder_uncategorized' in path:
                        del self.efos[k]
                        logging.warning("removed efo %s it is mapped to genetic_disorder_uncategorized bucket"%k)
                        break


        for row in self.session.query(EFOFirstChild).yield_per(1000):
            if get_ontology_code_from_url(row.first_child_uri) == 'genetic_disorder_uncategorized':
                continue
            efo_code_parent = get_ontology_code_from_url(row.parent_uri)
            efo_code_child = get_ontology_code_from_url(row.first_child_uri)
            if efo_code_parent in self.efos:
                efo_parent = self.efos[efo_code_parent]
                efo_child = self.efos[efo_code_child]
                efo_parent.children.append(dict(code = efo_code_child,
                                         label = efo_child.label))
                self.efos[efo_code_parent] = efo_parent




    def _store_efo(self):
        JSONObjectStorage.store_to_pg(self.session,
                                              Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                              Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                              self.efos)




class EfoUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                         Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                         )
        self.loader.optimize_all()



class EfoRetriever():
    """
    Will retrieve a EFO object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter,
                 cache_size = 25):
        warnings.warn('use redis based instead', DeprecationWarning, stacklevel=2)
        self.adapter=adapter
        self.session=adapter.session
        self.cache = OrderedDict()
        self.cache_size = cache_size

    def get_efo(self, efoid):
        if efoid in self.cache:
            efo = self.cache[efoid]
        else:
            efo = self._get_from_db(efoid)
            self._add_to_cache(efoid, efo)

        return efo

    def _get_from_db(self, efoid):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                                       Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                                       efoid)
        efo = EFO(efoid)
        if json_data:
            efo.load_json(json_data)
        return efo

    def _add_to_cache(self, efoid, efo):
        self.cache[efoid]=efo
        while len(self.cache) >self.cache_size:
            self.cache.popitem(last=False)


class EFOLookUpTable(object):
    """
    A redis-based pickable efo look up table
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
            self._table.set(get_ontology_code_from_url(efo['code']),efo, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

    def get_efo(self, efo_id, r_server = None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))

    def set_efo(self, efo, r_server = None):
        self._table.set(get_ontology_code_from_url(efo['code']),efo, r_server=self._get_r_server(r_server))

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