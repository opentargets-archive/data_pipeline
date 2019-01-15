from collections import OrderedDict

import csv
from mrtarget.common.IO import check_to_open, URLZSource
from mrtarget.common.LookupTables import ECOLookUpTable
from mrtarget.common.DataStructure import JSONSerializable
from opentargets_ontologyutils.rdf_utils import OntologyClassReader
import opentargets_ontologyutils.eco_so
from mrtarget.Settings import Config
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

    def __init__(self, loader):
        self.loader = loader
        self.ecos = OrderedDict()
        self.evidence_ontology = OntologyClassReader()

    def process_all(self):
        self._process_ontology_data()
        self._store_eco()

    def _process_ontology_data(self):

        uri_so = Config.ONTOLOGY_CONFIG.get('uris', 'so')
        uri_eco = Config.ONTOLOGY_CONFIG.get('uris', 'eco')

        opentargets_ontologyutils.eco_so.load_evidence_classes(self.evidence_ontology, uri_so, uri_eco)

        for uri,label in self.evidence_ontology.current_classes.items():
            eco = ECO(uri,
                      label,
                      self.evidence_ontology.classes_paths[uri]['all'],
                      self.evidence_ontology.classes_paths[uri]['ids'],
                      self.evidence_ontology.classes_paths[uri]['labels']
                      )
            id = self.evidence_ontology.classes_paths[uri]['ids'][0][-1]
            self.ecos[id] = eco

    def _store_eco(self):
        for eco_id, eco_obj in self.ecos.items():
            self.loader.put(index_name=Config.ELASTICSEARCH_ECO_INDEX_NAME,
                            doc_type=Config.ELASTICSEARCH_ECO_DOC_NAME,
                            ID=eco_id,
                            body=eco_obj)
        self.loader.flush_all_and_wait(Config.ELASTICSEARCH_ECO_INDEX_NAME)

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
                    logger.error("eco uri '%s' from eco scores file at line %d is not part of the ECO LUT so not using it",
                                 eco_uri, i)
    else:
        logger.error("eco_scores file %s does not exist", filename)

    return table
