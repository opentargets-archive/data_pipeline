import logging
import jsonpickle
import base64
import lxml.etree as etree

from mrtarget.common.UniprotIO import Parser
from opentargets_urlzsource import URLZSource
from mrtarget.constants import Const
import elasticsearch


"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(entries, dry_run, index):
    for entry in entries:
        #horrible hack, just save it as a blob
        json_seqrec = base64.b64encode(jsonpickle.encode(entry))

        if not dry_run:
            action = {}
            action["_index"] = index
            action["_type"] = Const.ELASTICSEARCH_UNIPROT_DOC_NAME
            action["_id"] = entry.id
            action["_source"] = {'entry': json_seqrec}

            yield action

def generate_uniprot(uri):
    with URLZSource(uri).open() as r_file:
        for event, elem in etree.iterparse(r_file, events=("end",), 
                tag='{http://uniprot.org/uniprot}entry'):

            #parse the XML into an object
            entry = Parser(elem, return_raw_comments=False).parse()
            elem.clear()

            yield entry

class UniprotDownloader(object):
    def __init__(self, loader, workers_write, queue_write):
        self.logger = logging.getLogger(__name__)
        self.loader = loader
        self.workers_write = workers_write
        self.queue_write = queue_write

    def process(self, uri, dry_run):
        self.logger.debug("download uniprot uri %s", uri)
        self.logger.debug("to generate this file you have to call this url "
                            "https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml")

        #setup elasticsearch
        if not dry_run:
            self.logger.debug("re-create index as we don't want duplicated entries but a fresh index")
            self.loader.create_new_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME))


        #write into elasticsearch
        failcount = 0
        if not dry_run:
            index = self.loader.get_versioned_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME)
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(generate_uniprot(uri), dry_run, index)
            for result in elasticsearch.helpers.parallel_bulk(self.loader.es, actions,
                    thread_count=self.workers_write, queue_size=self.queue_write, 
                    chunk_size=chunk_size):
                success, details = result
                if not success:
                    failcount += 1

        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()

        if failcount:
            raise RuntimeError("%s failed to index" % failcount)

    def qc(self, esquery):
        """Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
        of string test names and result objects
        """
        self.logger.info("Starting QC")
        #number of uniprot entries
        uniprot_count = 0
        #Note: try to avoid doing this more than once!
        for unprot_entry in esquery.get_all_uniprot_entries():
            uniprot_count += 1

            if uniprot_count % 1000 == 0:
                self.logger.debug("QC of %d uniprot entries", uniprot_count)

        #put the metrics into a single dict
        metrics = dict()
        metrics["uniprot.count"] = uniprot_count

        self.logger.info("Finished QC")
        return metrics
