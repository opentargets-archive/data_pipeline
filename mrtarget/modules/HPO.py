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
Module to Fetch the HPO ontology and store it in ElasticSearch as a lookup table
'''


class HpoActions(Actions):
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

class HPO(JSONSerializable):
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
                                              payload = dict(hpo_id = self.get_id(),
                                                             hpo_url = self.code,
                                                             hpo_label = self.label,),
                                              )
        }

        for field in field_order:
            if isinstance(field, list):
                self._private['suggestions']['input'].extend(field)
            else:
                self._private['suggestions']['input'].append(field)
        self._private['suggestions']['input'].append(self.get_id())


class HpoProcess():

    def __init__(self,
                 loader,):
        self.loader = loader
        self.hpos = OrderedDict()

    def process_all(self):
        self._process_ontology_data()
        self._store_hpo()

    def _process_ontology_data(self):

        self.phenotype_ontology = OntologyClassReader()
        self.phenotype_ontology.load_human_phenotype_ontology()

        for uri,label in self.phenotype_ontology.current_classes.items():
            print "--- %s --- %s"%(uri, label)
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

            hpo = HPO(code=uri,
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
                hpo.children = self.phenotype_ontology.children[uri]
            self.hpos[id] = hpo


    def _store_hpo(self):

        for hpo_id, hpo_obj in self.hpos.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_HPO_LABEL_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_HPO_LABEL_DOC_NAME,
                            ID=hpo_id,
                            body = hpo_obj)
