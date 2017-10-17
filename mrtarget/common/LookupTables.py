import logging
from tqdm import tqdm
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle
from mrtarget.common.connection import new_redis_client, PipelineConnectors
from mrtarget.Settings import Config
from mrtarget.common import TqdmToLogger

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
        self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)

        if self.r_server:
            self._load_hpa_data(self.r_server)

    def _load_hpa_data(self, r_server=None):
        for el in tqdm(self._es_query.get_all_hpa(),
                       desc='loading hpa',
                       unit=' hpa',
                       unit_scale=True,
                       total=self._es_query.count_all_hpa(),
                       file=self.tqdm_out,
                       leave=False):
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


class ECOLookUpTable(object):
    """
    A redis-based pickable gene look up table
    """


    def __init__(self,
                 es,
                 namespace=None,
                 r_server=None,
                 ttl=60 * 60 * 24 + 7):
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=r_server,
                                             ttl=ttl)
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        self._logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
        if r_server is not None:
            self._load_eco_data(r_server)

    def get_ontology_code_from_url(self, url):
        return url.split('/')[-1]

    def _load_eco_data(self, r_server=None):
        for eco in tqdm(self._es_query.get_all_eco(),
                        desc='loading eco',
                        unit=' eco',
                        unit_scale=True,
                        file=self.tqdm_out,
                        total=self._es_query.count_all_eco(),
                        leave=False,
                        ):
            self._table.set(self.get_ontology_code_from_url(eco['code']), eco,
                            r_server=self._get_r_server(r_server))  # TODO can be improved by sending elements in batches


    def get_eco(self, efo_id, r_server=None):
        return self._table.get(efo_id, r_server=self._get_r_server(r_server))


    def set_eco(self, eco, r_server=None):
        self._table.set(self.get_ontology_code_from_url(eco['code']), eco, r_server=self._get_r_server(r_server))


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
        self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
        if self.r_server is not None:
            self._load_efo_data(r_server)

    def _load_efo_data(self, r_server = None):
        for efo in tqdm(self._es_query.get_all_diseases(),
                        desc='loading diseases',
                        unit=' diseases',
                        unit_scale=True,
                        file=self.tqdm_out,
                        total=self._es_query.count_all_diseases(),
                        leave=False,
                        ):
            self.set_efo(efo, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches

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


# class MPLookUpTable(object):
#     """
#     A redis-based pickable mp look up table.
#     Allows to grab the MP saved in ES and load it up in memory/redis so that it can be accessed quickly from multiple processes, reducing memory usage by sharing.
#     """
#
#     def __init__(self,
#                  es=None,
#                  namespace=None,
#                  r_server=None,
#                  ttl = 60*60*24+7,
#                  autoload=True):
#         self._es = es
#         self.r_server = r_server
#         self._es_query = ESQuery(self._es)
#         self._table = RedisLookupTablePickle(namespace = namespace,
#                                             r_server = self.r_server,
#                                             ttl = ttl)
#
#         self._logger = logging.getLogger(__name__)
#         self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
#         if self.r_server is not None and autoload:
#             self._load_mp_data(r_server)
#
#     def _load_mp_data(self, r_server = None):
#         for mp in tqdm(self._es_query.get_all_mammalian_phenotypes(),
#                         desc='loading mammalian phenotypes',
#                         unit=' mammalian phenotypes',
#                         unit_scale=True,
#                         file=self.tqdm_out,
#                         total=self._es_query.count_all_mammalian_phenotypes(),
#                         leave=False,
#                         ):
#             self.set_mp(mp, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches
#
#     def get_mp(self, mp_id, r_server=None):
#         return self._table.get(mp_id, r_server=self._get_r_server(r_server))
#
#     def set_mp(self, mp, r_server=None):
#         mp_key = mp['path_codes'][0][-1]
#         self._table.set(mp_key, mp, r_server=self._get_r_server(r_server))
#
#     def get_available_mp_ids(self, r_server=None):
#         return self._table.keys(r_server=self._get_r_server(r_server))
#
#     def __contains__(self, key, r_server=None):
#         return self._table.__contains__(key, r_server=self._get_r_server(r_server))
#
#     def __getitem__(self, key, r_server=None):
#         return self.get_mp(key, r_server=self._get_r_server(r_server))
#
#     def __setitem__(self, key, value, r_server=None):
#         self._table.set(key, value, r_server=self._get_r_server(r_server))
#
#     def keys(self, r_server=None):
#         return self._table.keys(r_server=self._get_r_server(r_server))
#
#     def _get_r_server(self, r_server = None):
#         return r_server if r_server else self.r_server

# class HPOLookUpTable(object):
#     """
#     A redis-based pickable hpo look up table.
#     Allows to grab the HPO saved in ES and load it up in memory/redis so that it can be accessed quickly from multiple processes, reducing memory usage by sharing.
#     """
#
#     def __init__(self,
#                  es=None,
#                  namespace=None,
#                  r_server=None,
#                  ttl = 60*60*24+7):
#         self._es = es
#         self.r_server = r_server
#         self._es_query = ESQuery(self._es)
#         self._table = RedisLookupTablePickle(namespace = namespace,
#                                             r_server = self.r_server,
#                                             ttl = ttl)
#         self._logger = logging.getLogger(__name__)
#         self.tqdm_out = TqdmToLogger(self._logger, level=logging.INFO)
#
#         if self.r_server is not None:
#             self._load_hpo_data(r_server)
#
#     def _load_hpo_data(self, r_server = None):
#         for hpo in tqdm(self._es_query.get_all_human_phenotypes(),
#                         desc='loading human phenotypes',
#                         unit=' human phenotypes',
#                         unit_scale=True,
#                         file=self.tqdm_out,
#                         total=self._es_query.count_all_human_phenotypes(),
#                         leave=False,
#                         ):
#             self.set_hpo(hpo, r_server=self._get_r_server(r_server))#TODO can be improved by sending elements in batches
#
#     def get_hpo(self, hpo_id, r_server=None):
#         return self._table.get(hpo_id, r_server=self._get_r_server(r_server))
#
#     def set_hpo(self, hpo, r_server=None):
#         hpo_key = hpo['path_codes'][0][-1]
#         self._table.set(hpo_key, hpo, r_server=self._get_r_server(r_server))
#
#     def get_available_hpo_ids(self, r_server=None):
#         return self._table.keys(r_server=self._get_r_server(r_server))
#
#     def __contains__(self, key, r_server=None):
#         return self._table.__contains__(key, r_server=self._get_r_server(r_server))
#
#     def __getitem__(self, key, r_server=None):
#         return self.get_hpo(key, r_server=self._get_r_server(r_server))
#
#     def __setitem__(self, key, value, r_server=None):
#         self._table.set(key, value, r_server=self._get_r_server(r_server))
#
#     def keys(self, r_server=None):
#         return self._table.keys(r_server=self._get_r_server(r_server))
#
#     def _get_r_server(self, r_server = None):
#         return r_server if r_server else self.r_server

class LiteratureLookUpTable(object):
    """
    A redis-based pickable literature look up table
    """

    def __init__(self,
                 es = None,
                 namespace = None,
                 r_server = None,
                 ttl = 60*60*24+7):
        self._table = RedisLookupTablePickle(namespace = namespace,
                                            r_server = r_server,
                                            ttl = ttl)
        if es is None:
            connector = PipelineConnectors()
            connector.init_services_connections(publication_es=True)
            self._es = connector.es_pub
        else:
            self._es = es

        self._es_query = ESQuery(self._es)
        self.r_server = r_server if r_server else new_redis_client()

        if r_server is not None:
            self._load_literature_data(r_server)
        self._logger = logging.getLogger(__name__)

    def _load_literature_data(self, r_server = None):
        # for pub_source in tqdm(self._es_query.get_all_pub_from_validated_evidence(datasources=['europepmc']),
        #                 desc='loading publications',
        #                 unit=' publication',
        #                 unit_scale=True,
        #                 leave=False,
        #                 ):
        #     pub = Publication()
        #     pub.load_json(pub_source)
        #
        #     self.set_literature(pub,self._get_r_server(
        #             r_server))# TODO can be improved by sending elements in batches
        return

    def get_literature(self, pmid, r_server = None):
        try:
            return self._table.get(pmid, r_server=self._get_r_server(r_server))
        except KeyError:
            try:
                pub = self._es_query.get_objects_by_id(pmid,
                                                          Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME,
                                                          Config.ELASTICSEARCH_PUBLICATION_DOC_NAME).next()
            except Exception as e:
                self._logger.exception('Cannot retrieve target from elasticsearch')
                raise KeyError()
            self.set_literature(pub, r_server)
            return pub

    def set_literature(self, literature, r_server = None):
        self._table.set((literature.pub_id), literature, r_server=self._get_r_server(
            r_server))

    def get_available_literature_ids(self, r_server = None):
        return self._table.keys()

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key, r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_literature(key, r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server=None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class instantiation or at the method level')
        return r_server

    def keys(self):
        return self._table.keys()

