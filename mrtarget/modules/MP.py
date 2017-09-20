import logging
from collections import OrderedDict
from tqdm import tqdm 
from mrtarget.common import TqdmToLogger
from mrtarget.common import Actions
from mrtarget.common.connection import PipelineConnectors
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle
from mrtarget.modules.Ontology import OntologyClassReader, DiseaseUtils
from rdflib import URIRef
from mrtarget.Settings import Config

logger = logging.getLogger(__name__)
tqdm_out = TqdmToLogger(logger,level=logging.INFO)

'''
Module to Fetch the MP ontology and store it in ElasticSearch as a lookup table
'''


class MpActions(Actions):
    PROCESS='process'
    UPLOAD='upload'

def get_ontology_code_from_url(url):
    base_code = url.split('/')[-1]
    # http://purl.obolibrary.org/obo/HP_0012531
    if '/purl.obolibrary.org/obo/' in url:
        if ('_' not in base_code) and (':' not in base_code):
            return "HP_"+base_code
    if base_code is None:
        return url
    return base_code

class MP(JSONSerializable):
    def __init__(self,
                 code='',
                 label='',
                 exact_synonyms=[],
                 broad_synonyms=[],
                 narrow_synonyms=[],
                 path=[],
                 path_codes=[],
                 path_labels=[],
                 # id_org=None,
                 definition=""):
        self.code = code
        self.label = label
        self.broad_synonyms = broad_synonyms
        self.exact_synonyms = exact_synonyms
        self.narrow_synonyms = narrow_synonyms
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        self.definition = definition
        self.children=[]
        self._logger = logging.getLogger(__name__)

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
                                              payload = dict(mp_id = self.get_id(),
                                                             mp_url = self.code,
                                                             mp_label = self.label,),
                                              )
        }

        for field in field_order:
            if isinstance(field, list):
                self._private['suggestions']['input'].extend(field)
            else:
                self._private['suggestions']['input'].append(field)
        self._private['suggestions']['input'].append(self.get_id())


class MpProcess():

    def __init__(self,
                 loader,):
        self.loader = loader
        self.mps = OrderedDict()

    def process_all(self):
        self._process_ontology_data()
        self._store_mp()

    def _process_ontology_data(self):

        self.phenotype_ontology = OntologyClassReader()
        self.phenotype_ontology.load_mammalian_phenotype_ontology()

        for uri,label in self.phenotype_ontology.current_classes.items():
            self._logger.debug("--- %s --- %s"%(uri, label))
            properties = self.phenotype_ontology.parse_properties(URIRef(uri))
            definition = ''
            if 'http://purl.obolibrary.org/obo/IAO_0000115' in properties:
                definition = ". ".join(properties['http://purl.obolibrary.org/obo/IAO_0000115'])
            exact_synonyms = []
            narrow_synonyms = []
            broad_synonyms = []
            # oboInOwl:hasExactSynonym
            # http://www.geneontology.org/formats/oboInOwl#hasExactSynonym
            if 'http://www.geneontology.org/formats/oboInOwl#hasExactSynonym' in properties:
                exact_synonyms = properties['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym']
            if 'http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym' in properties:
                broad_synonyms = properties['http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym']
            if 'http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym' in properties:
                narrow_synonyms = properties['http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym']

            mp = MP(code=uri,
                      label=label,
                      exact_synonyms=exact_synonyms,
                      broad_synonyms=broad_synonyms,
                      narrow_synonyms=narrow_synonyms,
                      path=self.phenotype_ontology.classes_paths[uri]['all'],
                      path_codes=self.phenotype_ontology.classes_paths[uri]['ids'],
                      path_labels=self.phenotype_ontology.classes_paths[uri]['labels'],
                      definition=definition
                      )
            id = self.phenotype_ontology.classes_paths[uri]['ids'][0][-1]
            if uri in self.phenotype_ontology.children:
                mp.children = self.phenotype_ontology.children[uri]
            self.mps[id] = mp


    def _store_mp(self):

        for mp_id, mp_obj in self.mps.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_MP_LABEL_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_MP_LABEL_DOC_NAME,
                            ID=mp_id,
                            body = mp_obj)



class MPLookUpTable(object):
    """
    A redis-based pickable mp look up table.
    Allows to grab the MP saved in ES and load it up in memory/redis so that it can be accessed quickly from multiple processes, reducing memory usage by sharing.
    """

    def __init__(self,
                 es=None,
                 namespace=None,
                 r_server=None,
                 ttl = 60*60*24+7,
                 autoload=True):
        self._es = es
        self.r_server = r_server
        self._es_query = ESQuery(self._es)
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = self.r_server,
                                            ttl = ttl)

        if self.r_server is not None and autoload:
            self._load_mp_data(r_server)

    def _load_mp_data(self, r_server = None):
        for mp in tqdm(self._es_query.get_all_mammalian_phenotypes(),
                        desc='loading mammalian phenotypes',
                        unit=' mammalian phenotypes',
                        unit_scale=True,
                        file=tqdm_out,
                        total=self._es_query.count_all_mammalian_phenotypes(),
                        leave=False,
                        ):
            self.set_mp(mp, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

    def get_mp(self, mp_id, r_server=None):
        return self._table.get(mp_id, r_server=self._get_r_server(r_server))

    def set_mp(self, mp, r_server=None):
        mp_key = mp['path_codes'][0][-1]
        self._table.set(mp_key, mp, r_server=self._get_r_server(r_server))

    def get_available_mp_ids(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_mp(key, r_server=self._get_r_server(r_server))

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def keys(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server
