from collections import OrderedDict

import csv
from mrtarget.common.IO import check_to_open, URLZSource
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader
from mrtarget.constants import Const
import opentargets_ontologyutils.eco_so
from mrtarget.Settings import Config #TODO remove
import logging

logger = logging.getLogger(__name__)


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

class EcoProcess():

    def __init__(self, loader, eco_uri, so_uri):
        self.loader = loader
        self.ecos = OrderedDict()
        self.evidence_ontology = OntologyClassReader()
        self.eco_uri = eco_uri
        self.so_uri = so_uri

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

        #setup elasticsearch
        if not dry_run:
            self.loader.create_new_index(Const.ELASTICSEARCH_ECO_INDEX_NAME)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_ECO_INDEX_NAME))

        for eco_id, eco_obj in self.ecos.items():
            if not dry_run:
                self.loader.put(index_name=Const.ELASTICSEARCH_ECO_INDEX_NAME,
                    doc_type=Const.ELASTICSEARCH_ECO_DOC_NAME,
                    ID=eco_id, body=eco_obj)

        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_ECO_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()

    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):

        #number of eco entries
        eco_count = 0
        #Note: try to avoid doing this more than once!
        for eco_entry in esquery.get_all_eco():
            eco_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["eco.count"] = eco_count

        return metrics


ECO_SCORES_HEADERS = ["uri", "code", "score"]

def load_eco_scores_table(filename, eco_lut_obj):
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
                    #logging in child processess can lead to hung threads
                    # see https://codewithoutrules.com/2018/09/04/python-multiprocessing/
                    #logger.error("eco uri '%s' from eco scores file at line %d is not part of the ECO LUT so not using it", eco_uri, i)
                    pass
    else:
        logger.error("eco_scores file %s does not exist", filename)

    return table
