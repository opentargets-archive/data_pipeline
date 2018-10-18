from cStringIO import StringIO
import logging
from xml.etree import cElementTree as ElementTree
import requests
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.UniprotIO import UniprotIterator, Parser
from requests.exceptions import Timeout, HTTPError, ConnectionError
import jsonpickle
import base64
from requests_futures.sessions import FuturesSession
from concurrent.futures import ThreadPoolExecutor
import gzip

import elasticsearch
from mrtarget.Settings import Config, build_uniprot_query


def sanitise_dict_for_json(d):
    for k, v in d.iteritems():
        if isinstance(v, dict):
            v=sanitise_dict_for_json(v)
        if '.' in k:
            k_sane = k.replace('.', '|')
            d[k_sane] = v
            del d[k]
    return d


class UniprotDownloader():
    def __init__(self, loader, dry_run=False):
        #the trailing slash is required by uniprot
        self.url = "http://www.uniprot.org/uniprot/"
        self.dry_run = dry_run
        self.urlparams = dict()
        self.urlparams["query"] = build_uniprot_query(Config.MINIMAL_ENSEMBL) if Config.MINIMAL else "reviewed:yes+AND+organism:9606"
        self.urlparams["format"] = "xml"
        #requests will not transparently de-compress for us
        #if we use this, we have to ungzip it ourselves
        self.urlparams["compress"] = "yes"

        #size of chunks to get from uniprot
        self.chunk_size = 100
        #timeout for queries to uniprot
        self.timeout = 300

        #number of concurrent requests
        self.workers = min([16, Config.WORKERS_NUMBER])

        self.loader = loader
        self.NS = "{http://uniprot.org/uniprot}"
        self.logger = logging.getLogger(__name__+".UniprotDownloader")

    def cache_human_entries(self):
        with FuturesSession(executor=ThreadPoolExecutor(max_workers=self.workers)) as session:

            # set the index properly in order to insert in bulk mode
            self.loader.create_new_index(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME, recreate=True)

            #query to get hoe many to retrieve
            future = self._get_data_from_remote(session, 1,0)
            total = int(future.result().headers['X-Total-Results'])
            self.logger.info("Looking for %i uniprot entries", total)

            #create a futures for each of the pages
            futures = []
            offset = 0
            while offset < total:
                futures.append(self._get_data_from_remote(session, self.chunk_size, offset))
                offset += self.chunk_size

            self.logger.info("Queued %i futures for uniprot", len(futures))

            #now loop over the responses of the futures
            #callbacks will put into elastic, so just need to loop over to make sure
            #they are all finished
            for future in futures:
                future.result()

            self.loader.flush()
            self.loader.close()
            self.logger.info('downloaded %i entries from uniprot'%total)

    def _iterate_xml(self, handle):
        for event, elem in ElementTree.iterparse(handle, events=("start", "end")):
            if event == "end" and elem.tag == self.NS + "entry":
                yield elem
                elem.clear()

    def _get_data_from_remote(self, session, limit, offset):
        # make a local copy for this query of the parameters
        params = dict(self.urlparams)
        # set the paging
        # if you dont use offset uniprot will return everything! so
        # make sure you use an initial offset=0 as well
        params["limit"] = limit
        params["offset"] = offset

        self.logger.debug('querying url %s with params %s', self.url, params)
        return session.get(self.url, params=params, timeout=self.timeout, background_callback=self._cb_get)

    def _cb_get(self, session, response):
        try:
            self.logger.debug('Got response for %s',response.url)
            if response.status_code is not 200:
                self.logger.error("unable to get data from uniprot.org ("+response.status_code+")")
                raise IOError('unable to get data from uniprot.org ('+response.status_code+')')
        except (ConnectionError, Timeout, HTTPError) as e:
            self.logger.error("unable to get data from "+response.url)
            raise IOError(e)
        if response is not None:
            with gzip.GzipFile(fileobj=StringIO(response.content)) as gzipfile:
                for xml in self._iterate_xml(gzipfile):
                    result = Parser(xml, return_raw_comments=True).parse()
                    self._save_to_elasticsearch(result.id, result)

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
