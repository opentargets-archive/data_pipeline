import logging
import jsonpickle
import base64
from xml.etree import cElementTree as ElementTree

from mrtarget.common.UniprotIO import Parser
from mrtarget.common import URLZSource
from mrtarget.Settings import Config


class UniprotDownloader():
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
                entries = 0
                for i, xml in enumerate(self._iterate_xml(r_file, self.NS), start=1):
                    result = Parser(xml, return_raw_comments=True).parse()
                    self._save_to_elasticsearch(result.id, result)
                    entries = i

                self.logger.debug("finished loading %d uniprot entries", entries)
                self.loader.close()
        else:
            self.logger.debug("skipping uniprot as --dry-run was activated")

    @staticmethod
    def _iterate_xml(handle, ns):
        for event, elem in ElementTree.iterparse(handle, events=("start", "end")):
            if event == "end" and elem.tag == ns + "entry":
                yield elem
                elem.clear()

    def _save_to_elasticsearch(self, uniprotid, seqrec):
        self.logger.debug("saving %s",uniprotid)
        json_seqrec = base64.b64encode(jsonpickle.encode(seqrec))
        self.loader.put(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME,
                        Config.ELASTICSEARCH_UNIPROT_DOC_NAME,
                        uniprotid,
                        dict(entry =json_seqrec),
                        create_index=False)

    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):
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
