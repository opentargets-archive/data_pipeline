import logging
import jsonpickle
import base64
import lxml.etree as etree
import itertools as iters
import more_itertools as miters

from mrtarget.common.UniprotIO import Parser
from mrtarget.common import URLZSource
from mrtarget.Settings import Config


class UniprotDownloader(object):
    NS = "{http://uniprot.org/uniprot}"
    def __init__(self, loader, dry_run=False):
        self.logger = logging.getLogger(__name__)
        self.dry_run = dry_run

        self.loader = loader

    def cache_human_entries(self, uri=Config.UNIPROT_URI):
        if not self.dry_run:
            self.logger.debug("download uniprot uri %s", Config.UNIPROT_URI)
            self.logger.debug("to generate this file you have to call this url "
                              "https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml")

            with URLZSource(uri).open() as r_file:
                self.logger.debug("re-create index as we don't want duplicated entries but a fresh index")
                self.loader.create_new_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME, recreate=True)

                self.logger.debug("iterate through the whole uniprot xml file")

                def _operate(x):
                    (i, el) = x
                    self._save_to_elasticsearch(self.loader, el.id, el,
                                                Config.ELASTICSEARCH_UNIPROT_INDEX_NAME,
                                                Config.ELASTICSEARCH_UNIPROT_DOC_NAME)
                    return i

                total_entries = \
                    miters.last(iters.imap(_operate,
                                           enumerate(self._iterate_xml(r_file, UniprotDownloader.NS),
                                                     start=1)))

                self.logger.debug("finished loading %d uniprot entries", total_entries)
                self.loader.close()
        else:
            self.logger.debug("skipping uniprot as --dry-run was activated")

    @staticmethod
    def _iterate_xml(handle, ns):
        for event, elem in etree.iterparse(handle, events=("end",), tag=ns + 'entry'):
            yield Parser(elem, return_raw_comments=False).parse()
            elem.clear()

    @staticmethod
    def _save_to_elasticsearch(loader_obj, uniprotid, seqrec, index_name, doc_name):
        json_seqrec = base64.b64encode(jsonpickle.encode(seqrec))
        loader_obj.put(index_name, doc_name, uniprotid, {'entry': json_seqrec}, create_index=False)

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
