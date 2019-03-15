import logging
from collections import OrderedDict
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader, DiseaseUtils
import opentargets_ontologyutils.efo
from rdflib import URIRef
from mrtarget.constants import Const

logger = logging.getLogger(__name__)

'''
Module to Fetch the EFO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an EFO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''

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
                 therapeutic_codes=[],
                 definition=""):
        self.code = code
        self.label = label
        self.efo_synonyms = synonyms
        self.phenotypes = phenotypes
        self.path = path
        self.path_codes = path_codes
        self.path_labels = path_labels
        self.therapeutic_labels = therapeutic_labels
        self.therapeutic_codes = therapeutic_codes
        # self.id_org = id_org
        self.definition = definition
        self.children=[]

    def get_id(self):
        return self.code

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
                 loader,
                 efo_uri,
                 hpo_uri,
                 mp_uri,
                 disease_phenotype_uris
                 ):
        self.loader = loader
        self.efos = OrderedDict()
        self.logger = logging.getLogger(__name__+".EfoProcess")
        self.efo_uri = efo_uri
        self.hpo_uri = hpo_uri
        self.mp_uri = mp_uri
        self.disease_phenotype_uris = disease_phenotype_uris

    def process_all(self, dry_run):
        self._process_ontology_data()
        self._store_efo(dry_run)

    def _process_ontology_data(self):

        self.disease_ontology = OntologyClassReader()
        opentargets_ontologyutils.efo.load_open_targets_disease_ontology(self.disease_ontology,  self.efo_uri)

        '''
        Get all phenotypes
        '''
        #becuse of opentargets_ontologyutils for legacy iterates over key,uri pairs
        disease_phenotype_uris_counter = enumerate(self.disease_phenotype_uris)

        utils = DiseaseUtils()
        disease_phenotypes = utils.get_disease_phenotypes(self.disease_ontology, self.hpo_uri, self.mp_uri, disease_phenotype_uris_counter)

        #for uri,label in self.disease_ontology.current_classes.items():
        for uri in self.disease_ontology.classes_paths:
            #get the short code form of the uri
            classes_path = self.disease_ontology.classes_paths[uri]
            id = classes_path['ids'][0][-1]
            label = classes_path['labels'][0][-1]

            properties = self.disease_ontology.parse_properties(URIRef(uri))

            #create a text block definition/description by joining others together
            definition = ''
            if 'http://purl.obolibrary.org/obo/IAO_0000115' in properties:
                definition = ". ".join(properties['http://purl.obolibrary.org/obo/IAO_0000115'])

            #build a set of all the relevant synonyms
            synonyms = set()            
            #exact synonyms
            if 'http://www.geneontology.org/formats/oboInOwl#hasExactSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasExactSynonym'])

            #related synonyms (partially overlapping)
            if 'http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym'])

            #generic synoynms
            if 'http://www.geneontology.org/formats/oboInOwl#hasSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasSynonym'])

            #narrow synonyms
            if 'http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym' in properties:
                synonyms.update(properties['http://www.geneontology.org/formats/oboInOwl#hasNarrowSynonym'])

            #could have http://www.geneontology.org/formats/oboInOwl#hasBroadSynonym, but that is better captured by parent term

            phenotypes = []
            if uri in disease_phenotypes:
                phenotypes = disease_phenotypes[uri]['phenotypes']

            if uri not in self.disease_ontology.classes_paths:
                logger.warning("Unable to find %s", uri)
                continue


            therapeutic_labels = self.disease_ontology.therapeutic_labels[uri]
            therapeutic_uris = self.disease_ontology.therapeutic_uris[uri]
            therapeutic_codes = [self.disease_ontology.classes_paths[uri]['ids'][0][-1] for uri in therapeutic_uris]


            efo = EFO(code=uri,
                      label=label,
                      synonyms=synonyms,
                      phenotypes=phenotypes,
                      path=classes_path['all'],
                      path_codes=classes_path['ids'],
                      path_labels=classes_path['labels'],
                      therapeutic_labels=therapeutic_labels,
                      therapeutic_codes=therapeutic_codes,
                      definition=definition
                      )

            if uri in self.disease_ontology.children:
                efo.children = self.disease_ontology.children[uri]

            logger.debug(str(classes_path['ids']))
            logger.debug("done %s %s %s %s", id, uri, label, classes_path['labels'][0][-1])

            if id in self.efos:
                logger.warning("duplicate %s", id)
                continue
            self.efos[id] = efo

    def _store_efo(self, dry_run):
        logger.info("writing to elasticsearch")

        #setup elasticsearch
        if not dry_run:
            self.loader.create_new_index(Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME))

        for efo_id, efo_obj in self.efos.items():
            if not dry_run:
                logger.debug("putting %s", efo_id)
                self.loader.put(index_name=Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME,
                                doc_type=Const.ELASTICSEARCH_EFO_LABEL_DOC_NAME,
                                ID=efo_id,
                                body = efo_obj)

        #cleanup elasticsearch
        if not dry_run:
            logger.debug("Flushing elasticsearch")
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()
            logger.debug("Flushed elasticsearch")

        logger.info("wrote to elasticsearch")

        
    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):
        self.logger.info("Starting QC")
        #number of EFO terms
        efo_term_count = 0

        #top level terms (i.e. categories)
        efo_top_levels = []

        #terms without a description
        efo_missing_description_count = 0

        #loop over all efo terms and calculate the metrics
        #Note: try to avoid doing this more than once!
        for efo_term in esquery.get_all_diseases():
            efo_term_count += 1

            #path_labels is a list of lists of all paths to the root
            #top level terms will be those with one list of one item that is itself
            if len(efo_term["path_labels"]) == 1:
                if len(efo_term["path_labels"][0]) == 1:
                    efo_top_levels.append(efo_term["label"])

            if efo_term["definition"] == None or len(efo_term["definition"].strip()) == 0:
                efo_missing_description_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["efo.count"] = efo_term_count
        metrics["efo.top"] = sorted(efo_top_levels)
        metrics["efo.top.count"] = len(efo_top_levels)
        metrics["efo.missing_description.count"] = efo_missing_description_count

        #return the metrics to the caller so they can write to file or further compare
        self.logger.info("Finished QC")
        return metrics
