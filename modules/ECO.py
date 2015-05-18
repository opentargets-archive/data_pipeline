from collections import OrderedDict

from common import Actions
from common.DataStructure import JSONSerializable
from common.ElasticsearchLoader import JSONObjectStorage
from common.PGAdapter import  ECOPath
from settings import Config

__author__ = 'andreap'

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
        return get_ontology_code_from_url(self.path_codes[-1])





class EcoProcess():

    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session
        self.ecos = OrderedDict()

    def process_all(self):
        self._process_eco_data()
        self._store_eco()

    def _process_eco_data(self):
        for row in self.session.query(ECOPath).yield_per(1000):
            # if row.uri_id_org:
                # idorg2ecos[row.uri_id_org] = row.uri
                path_codes = []
                path_labels = []
                # tree_path = convert_tree_path(row.tree_path)
                for node in row.tree_path:
                    if isinstance(node, list):
                        for node_element in node:
                            path_codes.append(get_ontology_code_from_url(node_element['uri']))
                            path_labels.append(node_element['label'])
                            # break#TODO: just taking the first one, change this and all the dependent code to handle multiple paths
                    else:
                        path_codes.append(get_ontology_code_from_url(node['uri']))
                        path_labels.append(node['label'])
                eco = ECO(row.uri,
                          path_labels[-1],
                          row.tree_path,
                          path_codes,
                          path_labels,
                          # id_org=row.uri_id_org,
                          )
                self.ecos[get_ontology_code_from_url(row.uri)] = eco



    def _store_eco(self):
        JSONObjectStorage.store_to_pg(self.session,
                                              Config.ELASTICSEARCH_ECO_INDEX_NAME,
                                              Config.ELASTICSEARCH_ECO_DOC_NAME,
                                              self.ecos)




class EcoUploader():

    def __init__(self,
                 adapter,
                 loader):
        self.adapter=adapter
        self.session=adapter.session
        self.loader=loader

    def upload_all(self):
        JSONObjectStorage.refresh_index_data_in_es(self.loader,
                                         self.session,
                                         Config.ELASTICSEARCH_ECO_INDEX_NAME,
                                         Config.ELASTICSEARCH_ECO_DOC_NAME,
                                         )


class EcoRetriever():
    """
    Will retrieve a EFO object form the processed json stored in postgres
    """
    def __init__(self,
                 adapter):
        self.adapter=adapter
        self.session=adapter.session

    def get_eco(self, ecoid):
        json_data = JSONObjectStorage.get_data_from_pg(self.session,
                                                       Config.ELASTICSEARCH_ECO_INDEX_NAME,
                                                       Config.ELASTICSEARCH_ECO_DOC_NAME,
                                                       ecoid)
        eco = ECO(ecoid)
        eco.load_json(json_data)
        return eco