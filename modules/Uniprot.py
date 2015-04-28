from StringIO import StringIO
from xml.etree import cElementTree as ElementTree
from common.UniprotIO import UniprotIterator, Parser


__author__ = 'andreap'

class UniprotDownloader():
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
        c = 0
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
        return r

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
        entry = self.pg.query(UniprotInfo).filter_by(uniprot_accession=uniprotid).first()
        if not entry:
            self.pg.add(UniprotInfo(uniprot_accession=uniprotid,
                                    uniprot_entry=uniprot_xml))
            self.pg.commit()
            # except:
            # logging.warning("cannot store to postgres uniprot xml")