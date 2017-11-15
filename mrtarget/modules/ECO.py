from collections import OrderedDict

from tqdm import tqdm
from mrtarget.common import TqdmToLogger
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.modules.Ontology import OntologyClassReader
from mrtarget.Settings import Config
import logging
logger = logging.getLogger(__name__)


'''
Module to Fetch the ECO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an ECO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''


class EcoActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

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
        return ECOLookUpTable.get_ontology_code_from_url(self.code)

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


