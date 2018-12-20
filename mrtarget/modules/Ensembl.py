import json
import logging
import more_itertools
import functional
import configargparse

from mrtarget.common import URLZSource
from mrtarget.Settings import Config


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

    def process(self, ensembl_filename):
        def _put_line(line):
            self.loader.put(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME,
                            Config.ELASTICSEARCH_ENSEMBL_DOC_NAME,
                            line['id'],
                            json.dumps(line))
            return 1

        self.logger.info('Reading Ensembl gene info from %s' % ensembl_filename)

        inserted_lines = functional.seq(more_itertools.with_iter(URLZSource(ensembl_filename).open()))\
            .map(json.loads)\
            .map(_put_line)\
            .len()

        self.logger.info("Read %d lines from %s", inserted_lines, ensembl_filename)

        self.logger.info("flush index")
        self.loader.flush_all_and_wait(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME)

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
