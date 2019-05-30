import logging
from mrtarget.common.Redis import RedisLookupTablePickle
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll,Match

class HPALookUpTable(object):
    """
    A redis-based pickable hpa look up table using gene id as table
    id
    """

    def __init__(self,
                 es, index,
                 namespace,
                 r_server,
                 ttl=(60 * 60 * 24 + 7)):
        self._es = es
        self.r_server = r_server
        self._es_index = index
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=self.r_server,
                                             ttl=ttl)
        self._logger = logging.getLogger(__name__)

        if self.r_server:
            self._load_hpa_data(self.r_server)

    def _load_hpa_data(self, r_server=None):
        for el in Search().using(self._es).index(self._es_index).query(MatchAll()).scan():
            el = el.to_dict()
            self.set_hpa(el, r_server=self._get_r_server(r_server))

    def get_hpa(self, idx, r_server=None):
        return self._table.get(idx, r_server=self._get_r_server(r_server))

    def set_hpa(self, hpa, r_server=None):
        self._table.set(hpa['gene'], hpa,
                r_server=self._get_r_server(r_server))

    def get_available_hpa_ids(self, r_server=None):
        return self._table.keys(self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_hpa(key, r_server=self._get_r_server(r_server))

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def keys(self, r_server=None):
        return self._table.keys(self._get_r_server(r_server))

    def _get_r_server(self, r_server=None):
        return r_server if r_server else self.r_server

class GeneLookUpTable(object):
    """
    A redis-based pickable gene look up table
    """

    def __init__(self, es, es_index,
                 namespace, r_server,
                 ttl = 60*60*24+7):
        self._logger = logging.getLogger(__name__)
        self._es = es
        self._es_index = es_index
        self.r_server = r_server
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = self.r_server,
                                            ttl = ttl)
        self._logger = logging.getLogger(__name__)
        self.uniprot2ensembl = {}
        if self.r_server:
            self.load_gene_data(self.r_server)

    def load_gene_data(self, r_server):
        #load all targets
        for target in Search().using(self._es).index(self._es_index).query(MatchAll()).scan():
            target = target.to_dict()
            self._table.set(target['id'],target, r_server=self._get_r_server(r_server))
            if target['uniprot_id']:
                self.uniprot2ensembl[target['uniprot_id']] = target['id']
            for accession in target['uniprot_accessions']:
                self.uniprot2ensembl[accession] = target['id']

    def set_gene(self, target, r_server = None):
        self._table.set(target['id'],target, r_server=self._get_r_server(r_server))

    def get_available_gene_ids(self, r_server = None):
        return self._table.keys(r_server = self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def get_gene(self, key, r_server = None):
        if key in self:
            return self._table.get(key, r_server=self._get_r_server(r_server))

        #not in redis, get from elastic
        response = Search().using(self._es).index(self._es_index).query(Match(_id=key))[0:1].execute()
        gene = response.hits[0].to_dict()
        #store it in redis for later
        self.set_gene(gene, r_server)
        return gene

    def __getitem__(self, key, r_server = None):
        return self.get_gene(key,r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, self._get_r_server(r_server))

    def __missing__(self, key):
        #do nothing
        pass

    def keys(self, r_server=None):
        return self._table.keys(self._get_r_server(r_server))

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server


class ECOLookUpTable(object):
    """
    A redis-based pickable gene look up table
    """


    def __init__(self,
                 es, es_index,
                 namespace=None,
                 r_server=None,
                 ttl=60 * 60 * 24 + 7):
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=r_server,
                                             ttl=ttl)
        self._es = es
        self._es_index = es_index
        self.r_server = r_server
        self._logger = logging.getLogger(__name__)
        if r_server is not None:
            self._load_eco_data(r_server)

    @staticmethod
    def get_ontology_code_from_url(url):
        #note, this is not a guaranteed solution
        #to do it properly, it has to be from the actual
        #ontology file or from OLS API
        if '/' in url:
            return url.split('/')[-1]
        else:
            #assume already a short code
            return url

    def _load_eco_data(self, r_server=None):
        data = Search().using(self._es).index(self._es_index).query(MatchAll()).scan()
        for eco in data:
            eco = eco.to_dict()
            self._table.set(self.get_ontology_code_from_url(eco['code']), eco,
                r_server=self._get_r_server(r_server))  


    def get_eco(self, efo_id, r_server=None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))


    def set_eco(self, eco, r_server=None):
        self._table.set(self.get_ontology_code_from_url(eco['code']), eco, 
            r_server=self._get_r_server(r_server))


    def get_available_eco_ids(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))


    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))


    def __getitem__(self, key, r_server=None):
        return self.get_eco(key, r_server=self._get_r_server(r_server))


    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))


    def _get_r_server(self, r_server=None):
        return r_server if r_server else self.r_server


    def keys(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))

class EFOLookUpTable(object):
    """
    A redis-based pickable efo look up table.
    Allows to grab the EFO saved in ES and load it up in memory/redis so that it can be accessed quickly from multiple processes, reducing memory usage by sharing.
    """

    def __init__(self,
                 es, index,
                 namespace,
                 r_server,
                 ttl = 60*60*24+7):
        self._es = es
        self._es_index = index
        self.r_server = r_server
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = self.r_server,
                                            ttl = ttl)
        self._logger = logging.getLogger(__name__)
        if self.r_server is not None:
            self._load_efo_data(r_server)

    @staticmethod
    def get_ontology_code_from_url(url):
        #note, this is not a guaranteed solution
        #to do it properly, it has to be from the actual
        #ontology file or from OLS API
        if '/' in url:
            return url.split('/')[-1]
        else:
            #assume already a short code
            return url

    def _load_efo_data(self, r_server = None):

        data = Search().using(self._es).index(self._es_index).query(MatchAll()).scan()
        for efo in data:
            efo = efo.to_dict()
            efo_key = efo['path_codes'][0][-1]
            self._table.set(efo_key,efo, r_server=self._get_r_server(r_server))

    def get_efo(self, efo_id, r_server=None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))

    def get_available_gefo_ids(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_efo(key, r_server=self._get_r_server(r_server))

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def keys(self, r_server=None):
        return self._table.keys(r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server
