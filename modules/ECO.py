import os
import warnings
import logging
import json
from collections import OrderedDict

from tqdm import tqdm

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchQuery import ESQuery
from common.Redis import RedisLookupTablePickle
from modules.Ontology import OntologyClassReader
from settings import Config


from logging.config import fileConfig

logger = logging.getLogger(__name__)


'''
Module to Fetch the ECO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an ECO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''


class EcoActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

def get_ontology_code_from_url(url):
    return url.split('/')[-1]


class ECO(JSONSerializable):
    def __init__(self,
                 code='',
                 label='',
                 path=[],
                 path_codes=[],
                 path_labels=[],
                 # id_org=None,
                 ):
        self.code = code
        self.label = label
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        # self.id_org = id_org

    def get_id(self):
        # return self.code
        return get_ontology_code_from_url(self.code)





class EcoProcess():

    def __init__(self, loader):
        self.loader = loader
        self.ecos = OrderedDict()
        self.evidence_ontology = OntologyClassReader()

    def process_all(self):
        self._process_ontology_data()
        #self._process_eco_data()
        self._store_eco()

    def _process_ontology_data(self):

        self.evidence_ontology.load_evidence_classes()
        for uri,label in self.evidence_ontology.current_classes.items():
            #logger.debug("URI: %s, label:%s"%(uri, label))
            eco = ECO(uri,
                      label,
                      self.evidence_ontology.classes_paths[uri]['all'],
                      self.evidence_ontology.classes_paths[uri]['ids'],
                      self.evidence_ontology.classes_paths[uri]['labels']
                      )
            id = self.evidence_ontology.classes_paths[uri]['ids'][0][-1]
            self.ecos[id] = eco

    # def _process_eco_data(self):
    #
    #     for row in self.session.query(ECOPath).yield_per(1000):
    #         # if row.uri_id_org:
    #             # idorg2ecos[row.uri_id_org] = row.uri
    #             path_codes = []
    #             path_labels = []
    #             # tree_path = convert_tree_path(row.tree_path)
    #             for node in row.tree_path:
    #                 if isinstance(node, list):
    #                     for node_element in node:
    #                         path_codes.append(get_ontology_code_from_url(node_element['uri']))
    #                         path_labels.append(node_element['label'])
    #                         # break#TODO: just taking the first one, change this and all the dependent code to handle multiple paths
    #                 else:
    #                     path_codes.append(get_ontology_code_from_url(node['uri']))
    #                     path_labels.append(node['label'])
    #             eco = ECO(row.uri,
    #                       path_labels[-1],
    #                       row.tree_path,
    #                       path_codes,
    #                       path_labels,
    #                       # id_org=row.uri_id_org,
    #                       )
    #             self.ecos[get_ontology_code_from_url(row.uri)] = eco
    #     #TEMP FIX FOR MISSING ECO
    #     missing_uris =['http://www.targevalidation.org/literature_mining']
    #     for uri in missing_uris:
    #         code = get_ontology_code_from_url(uri)
    #         if code not in self.ecos:
    #             eco = ECO(uri,
    #                       [code],
    #                       [{"uri": uri, "label":code}],
    #                       [code],
    #                       [code],
    #                       )
    #             self.ecos[code] = eco

    def _store_eco(self):
        for eco_id, eco_obj in self.ecos.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_ECO_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_ECO_DOC_NAME,
                            ID=eco_id,
                            body=eco_obj)


class ECOLookUpTable(object):
    """
    A redis-based pickable gene look up table
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
            self._load_eco_data(r_server)

    def _load_eco_data(self, r_server = None):
        for eco in tqdm(self._es_query.get_all_eco(),
                        desc='loading eco',
                        unit=' eco',
                        unit_scale=True,
                        total=self._es_query.count_all_eco(),
                        leave=False,
                       ):
            self._table.set(get_ontology_code_from_url(eco['code']),eco, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

    def get_eco(self, efo_id, r_server = None):
        return self._table.get(efo_id, r_server=r_server)

    def set_eco(self, eco, r_server = None):
        self._table.set(get_ontology_code_from_url(eco['code']),eco, r_server=self._get_r_server(r_server))

    def get_available_eco_ids(self, r_server = None):
        return self._table.keys()

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_eco(key, r_server)

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

