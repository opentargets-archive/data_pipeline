import simplejson as json
import logging
import more_itertools

from opentargets_urlzsource import URLZSource
from mrtarget.constants import Const

class EnsemblProcess(object):
    """
    Load a set of Ensembl genes from a JSON file into Elasticsearch.
    It should be generated using the create_genes_dictionary.py script in opentargets/genetics_backend/makeLUTs
    e.g.
    python create_genes_dictionary.py -o "./" -e -z -n homo_sapiens_core_93_38
    """
    def __init__(self, loader):
        self.loader = loader
        self.logger = logging.getLogger(__name__)

    def process(self, ensembl_filename, dry_run):
        def _put_line(line):
            return 1

        self.logger.info('Reading Ensembl gene info from %s' % ensembl_filename)

        #setup elasticsearch
        if not dry_run:
            self.loader.create_new_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME))

        inserted_lines = 0
        for line in more_itertools.with_iter(URLZSource(ensembl_filename).open()):
            entry = json.loads(line)
            #store in elasticsearch if not dry running
            if not dry_run:
                self.loader.put(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME,
                    Const.ELASTICSEARCH_ENSEMBL_DOC_NAME,
                    entry['id'], line)
            inserted_lines += 1

        self.logger.info("Read %d lines from %s", inserted_lines, ensembl_filename)

        self.logger.info("flush index")

        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_ENSEMBL_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()

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
