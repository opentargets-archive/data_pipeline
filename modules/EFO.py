from collections import OrderedDict
import logging
from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import  EFONames, EFOPath, EFOFirstChild
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
            if get_ontology_code_from_url(row.uri) in self.efos:
                efo = self.efos[get_ontology_code_from_url(row.uri)]
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
                self.efos[get_ontology_code_from_url(row.uri)] = efo

        """temporary clean efos with empty path"""
        keys = self.efos.keys()
        for k in keys:
            efo = self.efos[k]
            if not efo.path:
                del self.efos[k]
                logging.warning("removed efo %s since it has an empty path, add it in postgres"%k)

        for row in self.session.query(EFOFirstChild).yield_per(1000):
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