import simplejson as json
import logging
import more_itertools

from opentargets_urlzsource import URLZSource
from mrtarget.constants import Const
import elasticsearch

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(lines, dry_run, index):
    for line in lines:
        entry = json.loads(line)
        if not dry_run:
            action = {}
            action["_index"] = index
            action["_type"] = Const.ELASTICSEARCH_ENSEMBL_DOC_NAME
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
    def __init__(self, loader, workers_write, queue_write):
        self.loader = loader
        self.logger = logging.getLogger(__name__)
        self.workers_write = workers_write
        self.queue_write = queue_write

    def process(self, ensembl_filename, dry_run):
        def _put_line(line):
            return 1

        self.logger.info('Reading Ensembl gene info from %s' % ensembl_filename)

        lines = more_itertools.with_iter(URLZSource(ensembl_filename).open())

        #setup elasticsearch
        if not dry_run:
            self.loader.create_new_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME))

        #write into elasticsearch
        index = self.loader.get_versioned_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME)
        chunk_size = 1000 #TODO make configurable
        actions = elasticsearch_actions(lines, dry_run, index)
        failcount = 0
        for result in elasticsearch.helpers.parallel_bulk(self.loader.es, actions,
                thread_count=self.workers_write, queue_size=self.queue_write, 
                chunk_size=chunk_size):
            success, details = result
            if not success:
                failcount += 1

        #cleanup elasticsearch
        if not dry_run:
            self.loader.es.indices.flush(self.loader.get_versioned_index(
                Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME), 
                wait_if_ongoing=True)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()

        if failcount:
            raise RuntimeError("%s failed to index" % failcount)

    def qc(self, esquery):
        """
        Run a series of QC tests on the Ensembl Elasticsearch index. Returns a dictionary
        of string test names and result objects
        """
        self.logger.info("Starting QC")
        # number of genes
        ensembl_count = 0
        # Note: try to avoid doing this more than once!
        for _ in esquery.get_all_ensembl_genes():
            ensembl_count += 1

        # put the metrics into a single dict
        metrics = dict()
        metrics["ensembl.count"] = ensembl_count

        self.logger.info("Finished QC")
        return metrics
