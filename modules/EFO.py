from collections import OrderedDict
from datetime import datetime
import logging
from sqlalchemy import and_
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import EvidenceStringStorage
from common.PGAdapter import ElasticsearchLoad, EFONames, EFOPath
from settings import Config

__author__ = 'andreap'

class EfoActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

def get_ontology_code_from_url(url):
        return url.split('/')[-1]

class EFO(JSONSerializable):
    def __init__(self,
                 code,
                 label,
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

    def get_id(self):
        return get_ontology_code_from_url(self.path_codes[0][-1])

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
                 adapter):
        self.adapter=adapter
        self.session=adapter.session
        self.efos = OrderedDict()

    def process_all(self):
        self._process_efo_data()
        self._store_efo()

    def _process_efo_data(self):

        # efo_uri = {}
        for row in self.session.query(EFONames).yield_per(1000):
            # if row.uri_id_org:
            #     idorg2efos[row.uri_id_org] = row.uri
            # efo_uri[get_ontology_code_from_url(row.uri)]=row.uri
            synonyms = []
            if row.synonyms != [None]:
                synonyms = row.synonyms
            self.efos[row.uri] = EFO(row.uri,
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
            if row.uri in self.efos:
                efo = self.efos[row.uri]
                full_tree_path = row.tree_path
                for node in row.tree_path:
                    if isinstance(node, list):
                        for node_element in node:
                            full_path_codes.append(get_ontology_code_from_url(node_element['uri']))
                            full_path_labels.append(node_element['label'])
                    else:
                        full_path_codes.append(get_ontology_code_from_url(node['uri']))
                        full_path_labels.append(node['label'])
                efo.path_codes.append(full_path_codes)
                efo.path_labels.append(full_path_labels)
                efo.path.append(full_tree_path)
                self.efos[row.uri] = efo

        """temporary clean efos with empty path"""
        keys = self.efos.keys()
        for k in keys:
            efo = self.efos[k]
            if not efo.path:
                del self.efos[k]
                logging.warning("removed efo %s since it has an empty path, add it in postgres"%k)




    def _store_efo(self):
        EvidenceStringStorage.store_to_pg(self.session,
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
        EvidenceStringStorage.refresh_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                         Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                         )
