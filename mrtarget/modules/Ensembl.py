import simplejson as json
import logging
import more_itertools

from opentargets_urlzsource import URLZSource
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(lines, index, doc):
    for line in lines:
        entry = json.loads(line)
        action = {}
        action["_index"] = index
        action["_type"] = doc
        action["_id"] = entry['id']
        action["_source"] = line

        yield action


class EnsemblProcess(object):
    """
    Load a set of Ensembl genes from a JSON file into Elasticsearch.
    It should be generated using the create_genes_dictionary.py script in opentargets/genetics_backend/makeLUTs
    e.g.
    python create_genes_dictionary.py -o "./" -e -z -n homo_sapiens_core_93_38
    """
    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            ensembl_filename, workers_write, queue_write):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.ensembl_filename = ensembl_filename
        self.logger = logging.getLogger(__name__)
        self.workers_write = workers_write
        self.queue_write = queue_write

    def process(self, dry_run):
        def _put_line(line):
            return 1

        self.logger.info('Reading Ensembl gene info from %s' % self.ensembl_filename)

        lines = more_itertools.with_iter(URLZSource(self.ensembl_filename).open())

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)

        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):   
            #write into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(lines, self.es_index, self.es_doc)
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

    def qc(self, es, index):
        """
        Run a series of QC tests on the Ensembl Elasticsearch index. Returns a dictionary
        of string test names and result objects
        """
        self.logger.info("Starting QC")
        # number of genes
        ensembl_count = 0
        # Note: try to avoid doing this more than once!
        for e in Search().using(es).index(index).query(MatchAll()).scan():
            ensembl_count += 1

        # put the metrics into a single dict
        metrics = dict()
        metrics["ensembl.count"] = ensembl_count

        self.logger.info("Finished QC")
        return metrics
