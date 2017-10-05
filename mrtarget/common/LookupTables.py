import logging
from tqdm import tqdm

class GeneLookUpTable(object):
    """
    A redis-based pickable gene look up table
    """

    def __init__(self,
                 es=None,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7,
                 targets = [],
                 autoload=True):
        self._logger = logging.getLogger(__name__)
        self._es = es
        self.r_server = r_server
        self._es_query = ESQuery(self._es)
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = self.r_server,
                                            ttl = ttl)
        self._logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self._logger,level=logging.INFO)
        self.uniprot2ensembl = {}
        if self.r_server and autoload:
            self.load_gene_data(self.r_server, targets)

    def load_gene_data(self, r_server = None, targets = []):
        data = None
        if targets:
            data = self._es_query.get_targets_by_id(targets)
            total = len(targets)
        if data is None:
            data = self._es_query.get_all_targets()
            total = self._es_query.count_all_targets()
        for target in tqdm(
                data,
                desc = 'loading genes',
                unit = ' gene',
                unit_scale = True,
                total = total,
                file=self.tqdm_out,
                leave=False):
            self._table.set(target['id'],target, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches
            if target['uniprot_id']:
                self.uniprot2ensembl[target['uniprot_id']] = target['id']
            for accession in target['uniprot_accessions']:
                self.uniprot2ensembl[accession] = target['id']

    def load_uniprot2ensembl(self, targets = []):
        uniprot_fields = ['uniprot_id','uniprot_accessions', 'id']
        if targets:
            data = self._es_query.get_targets_by_id(targets,
                                                    fields= uniprot_fields)
            total = len(targets)
        else:
            data = self._es_query.get_all_targets(fields= uniprot_fields)
            total = self._es_query.count_all_targets()
        for target in tqdm(data,
                           desc='loading mappings from uniprot to ensembl',
                           unit=' gene mapping',
                           unit_scale=True,
                           file=self.tqdm_out,
                           total=total,
                           leave=False,
                           ):
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
                                                          Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                                          Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
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
        redis_contain = self._table.__contains__(key, r_server=self._get_r_server(r_server))
        if redis_contain:
            return True
        if not redis_contain:
            return self._es_query.exists(index=Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME,
                                         doc_type=Config.ELASTICSEARCH_GENE_NAME_DOC_NAME,
                                         id=key,
                                         )

    def __getitem__(self, key, r_server = None):
        return self.get_gene(key, self._get_r_server(r_server))

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, self._get_r_server(r_server))

    def __missing__(self, key):
        print key

    def keys(self, r_server=None):
        return self._table.keys(self._get_r_server(r_server))

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server
