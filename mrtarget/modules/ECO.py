from collections import OrderedDict

import csv
from opentargets_urlzsource import URLZSource
from mrtarget.common.IO import check_to_open
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
import opentargets_ontologyutils.eco_so
import logging
import elasticsearch
import simplejson as json
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll


'''
Module to Fetch the ECO ontology and store it in ElasticSearch to be used in evidence and association processing. 
WHenever an evidence or association has an ECO code, we use this module to decorate and expand the information around the code and ultimately save it in the objects.
'''
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

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, index, doc):
    for eco_id, eco_obj in items:
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = eco_id
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = eco_obj.to_json()

        yield action

class EcoProcess():

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            eco_uri, so_uri, workers_write, queue_write):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.eco_uri = eco_uri
        self.so_uri = so_uri
        self.workers_write = workers_write
        self.queue_write = queue_write

        self.ecos = OrderedDict()
        self.evidence_ontology = OntologyClassReader()

    def process_all(self, dry_run):
        self._process_ontology_data()
        self._store_eco(dry_run)

    def _process_ontology_data(self):
        opentargets_ontologyutils.eco_so.load_evidence_classes(self.evidence_ontology, 
            self.so_uri, self.eco_uri)

        for uri,label in self.evidence_ontology.current_classes.items():
            eco = ECO(uri,
                      label,
                      self.evidence_ontology.classes_paths[uri]['all'],
                      self.evidence_ontology.classes_paths[uri]['ids'],
                      self.evidence_ontology.classes_paths[uri]['labels']
                      )
            id = self.evidence_ontology.classes_paths[uri]['ids'][0][-1]
            self.ecos[id] = eco

    def _store_eco(self, dry_run):

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):

            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(self.ecos.items(), self.es_index, self.es_doc)
            failcount = 0

            if not dry_run:
                results = None
                if self.workers_write > 0:
                    results = elasticsearch.helpers.parallel_bulk(es, actions,
                            thread_count=self.workers_write,
                            queue_size=self.queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(es, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)

    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):

        #number of eco entries
        eco_count = 0
        #Note: try to avoid doing this more than once!
        for eco_entry in Search().using(es).index(index).query(MatchAll()).scan():
            eco_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["eco.count"] = eco_count

        return metrics


ECO_SCORES_HEADERS = ["uri", "code", "score"]

def load_eco_scores_table(filename, eco_lut_obj):
    #logger has to be built inside the process when multiprocessing
    logger = logging.getLogger(__name__)

    table = {}
    if check_to_open(filename):
        with URLZSource(filename).open() as r_file:
            for i, d in enumerate(csv.DictReader(r_file, fieldnames=ECO_SCORES_HEADERS, dialect='excel-tab'), start=1):
                #lookup tables use short ids not full iri
                eco_uri = d["uri"]
                short_eco_code = ECOLookUpTable.get_ontology_code_from_url(eco_uri)
                if short_eco_code in eco_lut_obj:
                    table[eco_uri] = float(d["score"])
                else:
                    logger.error("eco uri '%s' from eco scores file at line %d is not part of the ECO LUT so not using it", eco_uri, i)
    else:
        logger.error("eco_scores file %s does not exist", filename)

    return table
