from StringIO import StringIO
import logging
from xml.etree import cElementTree as ElementTree
import requests
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.UniprotIO import UniprotIterator, Parser
from requests.exceptions import Timeout, HTTPError, ConnectionError
import jsonpickle
import base64

from mrtarget.Settings import Config, build_uniprot_query


class UniprotDownloader():
    def __init__(self,
                 loader,
                 query="reviewed:yes+AND+organism:9606",
                 format="xml",
                 chunk_size=1000,
                 timeout=300,):
        self.query = build_uniprot_query(Config.MINIMAL_ENSEMBL) if Config.MINIMAL else query
        self.format = "&format=" + format
        self.url = "http://www.uniprot.org/uniprot/?query="
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.loader = Loader(loader.es, chunk_size=10,
                             dry_run=loader.is_dry_run())
        self.NS = "{http://uniprot.org/uniprot}"
        self.logger = logging.getLogger(__name__)

    def cache_human_entries(self):
        offset = 0
        c=0
        data = self._get_data_from_remote(self.chunk_size, offset)
        while data.content:
            for xml in self._iterate_xml(StringIO(data.content)):
                seqrec = Parser(xml, return_raw_comments=True).parse()
                self._save_to_elasticsearch(seqrec.id, seqrec)
                c+=1
            offset += self.chunk_size
            self.logger.info('downloaded %i entries from uniprot'%c)
            data = self._get_data_from_remote(self.chunk_size, offset)
        self.logger.info('downloaded %i entries from uniprot'%c)

    def _iterate_xml(self, handle):
        for event, elem in ElementTree.iterparse(handle, events=("start", "end")):
            if event == "end" and elem.tag == self.NS + "entry":
                yield elem
                elem.clear()

    def _get_data_from_remote(self, limit, offset):
        if offset:
            url = self.url + self.query + self.format + "&limit=%i&offset=%i" % (limit, offset)
        else:
            url = self.url + self.query + self.format + "&limit=%i" % (limit)
        try:
            r = requests.get(url, timeout=self.timeout)
            if r.status_code == 200:
                return r
            else:
                raise IOError('cannot get data from uniprot.org')
        except (ConnectionError, Timeout, HTTPError) as e:
            raise IOError(e)

    def _save_to_elasticsearch(self, uniprotid, seqrec):
        # seqrec = UniprotIterator(StringIO(uniprot_xml), 'uniprot-xml').next()
        json_seqrec = base64.b64encode(jsonpickle.encode(seqrec))
        # pprint(jsonpickle.json.loads(json_seqrec))
        self.loader.put(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME,
                        Config.ELASTICSEARCH_UNIPROT_DOC_NAME,
                        uniprotid,
                        dict(entry =json_seqrec))
    """
    Run a series of QC tests on EFO elasticsearch index. Returns a dictionary
    of string test names and result objects
    """
    def qc(self, esquery):

        #number of uniprot entries
        uniprot_count = 0
        #Note: try to avoid doing this more than once!
        for uniprot_entry in esquery.get_all_uniprot_entries():
            uniprot_count += 1

        #put the metrics into a single dict
        metrics = dict()
        metrics["uniprot.count"] = uniprot_count

        return metrics


class UniprotData():
    """ Retrieve data for a uniprot entry from the local cache or the remote website uniprot.org
    """

    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session

    #TODO: method to retrieve a single uniprot entry from the db cache with failover to uniprot.org




'''store seqrec as json
jsonpickle.dumps(a, unpicklable=False, make_refs=False)
'''
