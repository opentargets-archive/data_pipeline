import logging

import simplejson as json
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

from opentargets_urlzsource import URLZSource
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.connection import new_es_client
from mrtarget.common.chembl_lookup import ChEMBLLookup

import tempfile
import dbm
import shelve

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(items, index, doc):
    for ident, item in items:
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = ident
        #elasticsearch client uses https://github.com/elastic/elasticsearch-py/blob/master/elasticsearch/serializer.py#L24
        #to turn objects into JSON bodies. This in turn calls json.dumps() using simplejson if present.
        action["_source"] = item

        yield action


class DrugProcess(object):

    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            workers_write, queue_write,
            chembl_target_uri, 
            chembl_mechanism_uri, 
            chembl_component_uri, 
            chembl_protein_uri, 
            chembl_molecule_uri,
            chembl_indication_uri):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.workers_write = workers_write
        self.queue_write = queue_write
        self.chembl_target_uri = chembl_target_uri
        self.chembl_mechanism_uri = chembl_mechanism_uri
        self.chembl_component_uri = chembl_component_uri
        self.chembl_protein_uri = chembl_protein_uri
        self.chembl_molecule_uri = chembl_molecule_uri
        self.chembl_indication_uri = chembl_indication_uri

        self.logger = logging.getLogger(__name__)

    def process_all(self, dry_run):
        drugs = self.generate()
        self.store(dry_run, drugs)


    def create_shelf(self, uri, key_f):
        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        with URLZSource(uri).open() as f_obj:
            for line_no, line in enumerate(f_obj):
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    self.logger.error("Unable to read line %d %s", line_no, e)
                    raise e
                    
                key = key_f(obj)
                shelf[str(key)] = obj
        return shelf

    def create_shelf_multi(self, uri, key_f):
        # Shelve creates a file with specific database. Using a temp file requires a workaround to open it.
        # dumbdbm creates an empty database file. In this way shelve can open it properly.

        #note: this file is never deleted!
        filename = tempfile.NamedTemporaryFile(delete=False).name
        shelf = shelve.Shelf(dict=dbm.open(filename, 'n'))
        with URLZSource(uri).open() as f_obj:
            for line_no, line in enumerate(f_obj):
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError as e:
                    self.logger.error("Unable to read line %d", line_no)
                    raise e

                key = str(key_f(obj))
                if key not in shelf:
                    shelf[key] = [obj]
                else:
                    shelf[key] = shelf[key]+[obj]
        return shelf

    def generate(self):

        # pre-load into indexed shelf dicts

        self.logger.debug("Starting pre-loading")
        mols = self.create_shelf(self.chembl_molecule_uri, lambda x : x["molecule_chembl_id"])
        indications = self.create_shelf_multi(self.chembl_indication_uri, lambda x : x["molecule_chembl_id"])
        mechanisms = self.create_shelf_multi(self.chembl_mechanism_uri, lambda x : x["molecule_chembl_id"])
        self.logger.debug("Completed pre-loading")

        drugs = {}
        #TODO finish

        for ident in mols:
            mol = mols[ident]

            drug = {}
            drug["id"] = ident

            if "molecule_type" in mol and mol["molecule_type"] is not None:
                #TODO format check
                drug["molecule_type"] = str(mol["molecule_type"])
                
            if "first_approval" in mol and mol["first_approval"] is not None:
                #TODO format check
                drug["first_approval"] = str(mol["first_approval"])

            if "max_phase" in mol and mol["max_phase"] is not None:
                #TODO check this is 0 1 2 3 4
                drug["max_phase"] = int(mol["max_phase"])

            if ident in indications:
                drug["indications"] = []
                for indication in indications[ident]:
                    out = {}

                    if "efo_id" in indication and 
                            indication["efo_id"] is not None
                            and indication["efo_id"] is not "*":
                        #TODO ideally we want a full URI here
                        #TODO make sure this is an ID we care about
                        #TODO make sure this is with an underscore not colon
                        out["efo_id"] = indication["efo_id"]

                    drug["indications"].append(out)

            if ident in mechanisms:
                drug["mechanisms"] = []
                for mechanism in mechanisms[ident]:
                    out = {}
                    drug["mechanisms"].append(out)

            #TODO only keep those with indications or mechanisms ?

            drugs[ident] = drug

        return drugs

    def store(self, dry_run, data):
        self.logger.debug("Starting storage")
        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):
            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(data.items(), self.es_index, self.es_doc)
            failcount = 0
            if not dry_run:
                for result in elasticsearch.helpers.parallel_bulk(es, actions,
                        thread_count=self.workers_write, queue_size=self.queue_write, 
                        chunk_size=chunk_size):
                    success, details = result
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s failed to index" % failcount)
        
        self.logger.debug("Completed storage")


    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, es, index):
        self.logger.info("Starting QC")

        #number of drug entries
        drug_count = 0
        #Note: try to avoid doing this more than once!
        for drug_entry in Search().using(es).index(index).query(MatchAll()).scan():
            drug_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["drug.count"] = drug_count

        self.logger.info("Finished QC")
        return metrics
        
