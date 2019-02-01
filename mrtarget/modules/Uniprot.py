import logging
import jsonpickle
import base64
import lxml.etree as etree
import itertools as iters
import more_itertools as miters

from mrtarget.common.UniprotIO import Parser
from mrtarget.common import URLZSource
from mrtarget.constants import Const


class UniprotDownloader(object):
    NS = "{http://uniprot.org/uniprot}"
    def __init__(self, loader, dry_run=False):
        self.logger = logging.getLogger(__name__)
        self.total_entries = None
        self.loader = loader

    def process(self, uri, dry_run):
        self.logger.debug("download uniprot uri %s", uri)
        self.logger.debug("to generate this file you have to call this url "
                            "https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml")

        #setup elasticsearch
        if not dry_run:
            self.logger.debug("re-create index as we don't want duplicated entries but a fresh index")
            self.loader.create_new_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME, recreate=True)
            #need to directly get the versioned index name for this function
            self.loader.prepare_for_bulk_indexing(
                self.loader.get_versioned_index(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME))

        with URLZSource(uri).open() as r_file:
            self.logger.debug("iterate through the whole uniprot xml file")
            self.total_entries = 0
            for event, elem in etree.iterparse(r_file, events=("end",), tag=self.NS + 'entry'):
                #parse the XML into an object
                entry = Parser(elem, return_raw_comments=False).parse()
                elem.clear()

                #horrible hack, just save it as a blob
                #have already (re-)created the index so don't do it again
                json_seqrec = base64.b64encode(jsonpickle.encode(entry))
                #we canskip this bit (and only this bit!) if dry running
                if not dry_run:
                    self.loader.put(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME, 
                        Const.ELASTICSEARCH_UNIPROT_DOC_NAME, entry.id, 
                        {'entry': json_seqrec}, create_index=False)

                self.total_entries += 1

            self.logger.debug("finished loading %d uniprot entries", self.total_entries)

        #flush and wait for the index to be complete and ready before ending this step


        #cleanup elasticsearch
        if not dry_run:
            self.loader.flush_all_and_wait(Const.ELASTICSEARCH_UNIPROT_INDEX_NAME)
            #restore old pre-load settings
            #note this automatically does all prepared indexes
            self.loader.restore_after_bulk_indexing()

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
