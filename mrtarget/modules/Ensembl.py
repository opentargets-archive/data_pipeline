import json
import logging
import csv

from mrtarget.common import URLZSource
from mrtarget.Settings import Config


class EnsemblProcess(object):

    def __init__(self, loader):
        self.loader = loader
        self.logger = logging.getLogger(__name__+".EnsemblProcess")

    def process(self, ensembl_release=Config.ENSEMBL_RELEASE_VERSION):

        filename = Config.ENSEMBL_FILENAME
        self.logger.info('Reading Ensembl gene info from %s' % filename)

        with URLZSource(filename).open() as tsv_file:
            for row in csv.DictReader(tsv_file, dialect='excel-tab'):

                for key in ['start', 'end', 'strand', 'version', 'ensembl_release']:
                    row[key] = int(row[key])

                self.loader.put(Config.ELASTICSEARCH_ENSEMBL_INDEX_NAME,
                                Config.ELASTICSEARCH_ENSEMBL_DOC_NAME,
                                row['id'],
                                json.dumps(row),
                                #row,
                                True)

    """
    Run a series of QC tests on the Ensembl Elasticsearch index. Returns a dictionary
    of string test names and result objects
    """

    def qc(self, esquery):

        self.logger.info("Starting QC")
        # number of genes
        ensembl_count = 0
        # Note: try to avoid doing this more than once!
        for ensembl in esquery.get_all_ensembl_genes():
            ensembl_count += 1

        # put the metrics into a single dict
        metrics = dict()
        metrics["ensembl.count"] = ensembl_count

        self.logger.info("Finished QC")
        return metrics