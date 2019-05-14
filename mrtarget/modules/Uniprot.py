import logging
import jsonpickle
import base64
import lxml.etree as etree

from mrtarget.common.UniprotIO import Parser
from opentargets_urlzsource import URLZSource
from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
import elasticsearch
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll
import simplejson as json

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(entries, index, doc):
    for entry in entries:
        #horrible hack, just save it as a blob
        json_seqrec = base64.b64encode(jsonpickle.encode(entry))

        action = {}
        action["_index"] = index
        action["_type"] = doc
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
    def __init__(self, es_hosts, es_index, es_doc, es_mappings, es_settings,
            uri, workers_write, queue_write):
        self.es_hosts = es_hosts
        self.es_index = es_index
        self.es_doc = es_doc
        self.es_mappings = es_mappings
        self.es_settings = es_settings
        self.uri = uri
        self.workers_write = workers_write
        self.queue_write = queue_write

        self.logger = logging.getLogger(__name__)

    def process(self, dry_run):
        self.logger.debug("download uniprot uri %s", self.uri)
        self.logger.debug("to generate this file you have to call this url "
                            "https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml")

        with URLZSource(self.es_mappings).open() as mappings_file:
            mappings = json.load(mappings_file)

        with URLZSource(self.es_settings).open() as settings_file:
            settings = json.load(settings_file)
        chunk_size = 1000 # TODO make configurable
        es = new_es_client(self.es_hosts)
        with ElasticsearchBulkIndexManager(es, self.es_index, settings, mappings):

            items = generate_uniprot(self.uri)
            actions = elasticsearch_actions(items, self.es_index, self.es_doc)

            #write into elasticsearch
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
        """Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
        of string test names and result objects
        """
        self.logger.info("Starting QC")
        #number of uniprot entries
        uniprot_count = 0
        #Note: try to avoid doing this more than once!
        for unprot_entry in Search().using(es).index(index).query(MatchAll()).scan():
            uniprot_count += 1

            if uniprot_count % 1000 == 0:
                self.logger.debug("QC of %d uniprot entries", uniprot_count)

        #put the metrics into a single dict
        metrics = dict()
        metrics["uniprot.count"] = uniprot_count

        self.logger.info("Finished QC")
        return metrics
