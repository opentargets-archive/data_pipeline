from StringIO import StringIO
import logging
from xml.etree import cElementTree as ElementTree
import requests
from common import Actions
from common.ElasticsearchLoader import Loader
from common.PGAdapter import UniprotInfo
from common.UniprotIO import UniprotIterator, Parser
from requests.exceptions import Timeout, HTTPError, ConnectionError
import jsonpickle

from settings import Config

__author__ = 'andreap'

def sanitise_dict_for_json(d):
    for k, v in d.iteritems():
        if isinstance(v, dict):
            v=sanitise_dict_for_json(v)
        if '.' in k:
            k_sane = k.replace('.', '|')
            d[k_sane] = v
            del d[k]
    return d


class UniProtActions(Actions):
    CACHE='cache'

class OLDUniprotDownloader():
    """ this class is deprecated
    """
    def __init__(self,
                 query="reviewed:yes+AND+organism:9606",
                 format="xml",
                 chunk_size=1000,
                 timeout=300,
                 pg_session=None):
        self.query = query
        self.format = "&format=" + format
        self.url = "http://www.uniprot.org/uniprot/?query="
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.pg = pg_session
        self.NS = "{http://uniprot.org/uniprot}"


    def get_entry(self):
        offset = 0
        data = self.get_data(self.chunk_size, offset)
        while data.content:
            for xml in self._iterate_xml(StringIO(data.content)):
                seqrec = Parser(xml, return_raw_comments=True).parse()
                self._save_to_postgresql(seqrec.id, ElementTree.tostring(xml))
                yield seqrec
            offset += self.chunk_size
            data = self.get_data(self.chunk_size, offset)

    def _iterate_xml(self, handle):
        for event, elem in ElementTree.iterparse(handle, events=("start", "end")):
            if event == "end" and elem.tag == self.NS + "entry":
                yield elem
                elem.clear()

    def get_data(self, limit, offset):
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

    def get_single_entry(self, uniprotid):
        uniprot_xml = self.get_single_entry_xml(uniprotid)
        if uniprot_xml:
            return UniprotIterator(StringIO(uniprot_xml), return_raw_comments=True).next()
        else:
            logging.debug("cannot get uniprot data for uniprot id %s" % uniprotid)

    def get_single_entry_xml(self, uniprotid):
        try:
            '''get from uniprot.org'''
            return self.get_single_entry_xml_remote(uniprotid)
        except:
            ''' get from local postgresql'''
            return self.get_single_entry_xml_local(uniprotid)

    def get_single_entry_xml_remote(self, uniprotid):
        url = self.url + uniprotid + self.format
        r = requests.get(url, timeout=60)
        if r.status_code == 200:
            self._save_to_postgresql(uniprotid, r.content)
            return r.content
        else:
            logging.debug('cannot get data from remote uniprot.org for uniprotid: %s' % uniprotid)
        return

    def get_single_entry_xml_local(self, uniprotid):
        uniprot_data = self.pg.query(UniprotInfo).filter(UniprotInfo.uniprot_accession == uniprotid).first()
        if uniprot_data:
            return uniprot_data.uniprot_entry

    def _save_to_postgresql(self, uniprotid, uniprot_xml):
        entry = self.pg.query(UniprotInfo).filter_by(uniprot_accession=uniprotid).count()
        if not entry:
            self.pg.add(UniprotInfo(uniprot_accession=uniprotid,
                                    uniprot_entry=uniprot_xml))
            self.pg.commit()
            # except:
            # logging.warning("cannot store to postgres uniprot xml")


class UniprotDownloader():
    def __init__(self,
                 loader,
                 query="reviewed:yes+AND+organism:9606",
                 format="xml",
                 chunk_size=1000,
                 timeout=300,):
        self.query = query
        self.format = "&format=" + format
        self.url = "http://www.uniprot.org/uniprot/?query="
        self.chunk_size = chunk_size
        self.timeout = timeout
        self.loader = Loader(loader.es, chunk_size = 10)
        self.NS = "{http://uniprot.org/uniprot}"


    def cache_human_entries(self):
        # self._delete_cache()
        offset = 0
        c=0
        data = self._get_data_from_remote(self.chunk_size, offset)
        while data.content:
            for xml in self._iterate_xml(StringIO(data.content)):
                seqrec = Parser(xml, return_raw_comments=True).parse()
                '''sanitise for json'''
                # seqrec.annotations = sanitise_dict_for_json(seqrec.annotations)
                    # if '.' in k:
                    #     k_sane = k.replace('.', '-')
                    #     seqrec.annotations[k_sane] = seqrec.annotations[k]
                    #     del seqrec.annotations[k]
                    # if 'dbxref_extended' in seqrec.annotations and \
                    #     'Gene3D' in seqrec.annotations['dbxref_extended']:
                    #     del seqrec.annotations['dbxref_extended']['Gene3D']

                self._save_to_elasticsearch(seqrec.id, seqrec)
                c+=1
            offset += self.chunk_size
            logging.info('downloaded %i entries from uniprot'%c)
            data = self._get_data_from_remote(self.chunk_size, offset)
        logging.info('downloaded %i entries from uniprot'%c)

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


    # def _save_to_postgresql(self, uniprotid, uniprot_xml):
    #     entry = self.session.query(UniprotInfo).filter_by(uniprot_accession=uniprotid).count()
    #     if not entry:
    #         self.session.add(UniprotInfo(uniprot_accession=uniprotid,
    #                                 uniprot_entry=uniprot_xml))
    #         self.session.commit()
    #         self.cached_count+=1
    #         if (self.cached_count%5000)==0:
    #             logging.info("Cached %i entries from uniprot to local postgres"%self.cached_count)

    def _save_to_elasticsearch(self, uniprotid, seqrec):
        # seqrec = UniprotIterator(StringIO(uniprot_xml), 'uniprot-xml').next()
        json_seqrec = jsonpickle.dumps(seqrec)
        # pprint(jsonpickle.json.loads(json_seqrec))
        self.loader.put(Config.ELASTICSEARCH_UNIPROT_INDEX_NAME,
                        Config.ELASTICSEARCH_UNIPROT_DOC_NAME,
                        uniprotid,
                        dict(entry =json_seqrec))
        # entry = self.session.query(UniprotInfo).filter_by(uniprot_accession=uniprotid).count()
        # if not entry:
        #     self.session.add(UniprotInfo(uniprot_accession=uniprotid,
        #                                  uniprot_entry=uniprot_xml))
        #     self.session.commit()
        #     self.cached_count += 1
        #     if (self.cached_count % 5000) == 0:
        #         logging.info("Cached %i entries from uniprot to local postgres" % self.cached_count)

    # def _delete_cache(self):
    #     self.cached_count = 0
    #     rows_deleted= self.session.query(UniprotInfo).delete()
    #     if rows_deleted:
    #         logging.info('deleted %i rows from uniprot_info'%rows_deleted)



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