import logging
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle
from mrtarget.constants import Const
from elasticsearch_dsl import Search
from elasticsearch_dsl.query import MatchAll,Match

class HPALookUpTable(object):
    """
    A redis-based pickable hpa look up table using gene id as table
    id
    """

    def __init__(self,
                 es=None,
                 namespace=None,
                 r_server=None,
                 ttl=(60 * 60 * 24 + 7)):
        self._es = es
        self.r_server = r_server
        self._es_query = ESQuery(self._es)
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=self.r_server,
                                             ttl=ttl)
        self._logger = logging.getLogger(__name__)

        if self.r_server:
            self._load_hpa_data(self.r_server)

    def _load_hpa_data(self, r_server=None):
        for el in self._es_query.get_all_hpa():
            self.set_hpa(el, r_server=self._get_r_server(r_server))

    def get_hpa(self, idx, r_server=None):
        return self._table.get(idx, r_server=self._get_r_server(r_server))

    def set_hpa(self, hpa, r_server=None):
        self._table.set(hpa['gene'], hpa,
                        r_server=self._get_r_server(r_server))

    def get_available_hpa_ids(self, r_server=None):
        return self._table.keys(self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key,
                                        r_server=self._get_r_server(r_server))

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
        self._es_query = ESQuery(self._es)
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = self.r_server,
                                            ttl = ttl)
        self._logger = logging.getLogger(__name__)
        self.uniprot2ensembl = {}
        if self.r_server:
            self.load_gene_data(self.r_server)

    def load_gene_data(self, r_server):
        #load all targets
        data = Search().using(self._es).index(self._es_index).query(MatchAll()).scan()
        for target in data:
            target = target.to_dict()
            self._table.set(target['id'],target, r_server=self._get_r_server(r_server))
            if target['uniprot_id']:
                self.uniprot2ensembl[target['uniprot_id']] = target['id']
            for accession in target['uniprot_accessions']:
                self.uniprot2ensembl[accession] = target['id']

    def get_gene(self, target_id, r_server = None):
        try:
            return self._table.get(target_id, r_server=self._get_r_server(r_server))
        except KeyError:
            try:
                target = self._es_query.get_objects_by_id(target_id,
                                                          Const.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                                          Const.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                                          source_exclude='ortholog.*'
                                                          ).next()
            except Exception as e:
                self._logger.exception('Cannot retrieve target from elasticsearch')
                raise KeyError()
            self.set_gene(target, r_server)
            return target

    def set_gene(self, target, r_server = None):
        self._table.set(target['id'],target, r_server=self._get_r_server(r_server))

    def get_available_gene_ids(self, r_server = None):
        return self._table.keys(r_server = self._get_r_server(r_server))

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server = None):
        return self.get_gene(key, self._get_r_server(r_server))

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
                 es=None,
                 namespace=None,
                 r_server=None,
                 ttl = 60*60*24+7):
        self._es = es
        self.r_server = r_server
        self._es_query = ESQuery(self._es)
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
        self._logger = logging.getLogger(__name__)
        for i,efo in enumerate(self._es_query.get_all_diseases()):
            #TODO can be improved by sending elements in batches
            self.set_efo(efo, r_server=self._get_r_server(r_server))
            if i % 1000 == 0:
                self._logger.debug("Loaded %s efo", i)                

    def get_efo(self, efo_id, r_server=None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))

    def set_efo(self, efo, r_server=None):
        efo_key = efo['path_codes'][0][-1]
        self._table.set(efo_key,efo, r_server=self._get_r_server(r_server))

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
