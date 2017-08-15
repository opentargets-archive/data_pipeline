from __future__ import absolute_import
import logging
import re
import hashlib
import ujson as json
import functools as ft
import operator as oper
from tqdm import tqdm
from mrtarget.common import TqdmToLogger

import petl
from mrtarget.common import URLZSource

from mrtarget.common import Actions
from mrtarget.common.ElasticsearchQuery import ESQuery, Loader
from mrtarget.common.Redis import RedisLookupTablePickle, RedisQueueStatusReporter, RedisQueueWorkerProcess, RedisQueue

from mrtarget.Settings import Config
from addict import Dict
from mrtarget.common.DataStructure import JSONSerializable, json_serialize, PipelineEncoder


def level_from_text(key):
    level_translation = {'Not detected': 0,
                              'Low': 1,
                              'Medium': 2,
                              'High': 3,
                              }
    return level_translation[key]


def reliability_from_text(key):
    reliability_translation = {'Supportive': True,
                                    'Uncertain': False,
                                    ## new types for hpa v16
                                    'Approved' : True,
                                    'Supported': True,
                                    }
    return reliability_translation[key]


class HPAExpression(Dict, JSONSerializable):
    def __init__(self, *args, **kwargs):
        super(HPAExpression,self).__init__(*args, **kwargs)
        if 'data_release' not in self:
            self.data_release = Config.RELEASE_VERSION

        if 'tissues' not in self:
            self.tissues = []

        if 'cancer' not in self:
            self.cancer = Dict()

        if 'subcellular_location' not in self:
            self.subcellular_location = {}

    def set_id(self, gene_id):
        self.gene = gene_id

    def get_id(self):
        return self.gene if 'gene' in self else None

    @staticmethod
    def new_tissue_protein(*args, **kwargs):
        protein = Dict(*args, **kwargs)

        if 'level' not in protein:
            protein.level = -1

        if 'reliability' not in protein:
            protein.reliability = False

        if 'cell_type' not in protein:
            protein.cell_type = []

        return protein

    @staticmethod
    def new_tissue_rna(*args, **kwargs):
        rna = Dict(*args, **kwargs)

        if 'level' not in rna:
            rna.level = -1

        if 'value' not in rna:
            rna.value = 0

        if 'unit' not in rna:
            rna.unit = ''

        return rna

    @staticmethod
    def new_tissue(*args, **kwargs):
        tissue = Dict(*args, **kwargs)
        if 'efo_code' not in tissue:
            tissue.efo_code = ''

        if 'label' not in tissue:
            tissue.label = ''

        if 'protein' not in tissue:
            tissue.protein = HPAExpression.new_tissue_protein()
        if 'rna' not in tissue:
            tissue.rna = HPAExpression.new_tissue_rna()

        return tissue

    def stamp_data_release(self):
        self.data_release = Config.RELEASE_VERSION

    def to_json(self):
        self.stamp_data_release()
        return json.dumps(self.to_dict(),
                          default=json_serialize,
                          sort_keys=True,
                          # indent=4,
                          cls=PipelineEncoder)

    def load_json(self, data):
        try:
            self.update(json.loads(data))
        except Exception as e:
            raise e


def format_expression(rec):
    d = HPAExpression(gene=rec['gene'],
                      data_release=Config.RELEASE_VERSION)

    # for each tissue
    for el in rec['data']:
        t_code, t_name, c_types = el
        tissue = d.new_tissue(label = list(t_name)[0],
                              efo_code = t_code)

        # iterate all cell_types
        for ct in c_types:
            ct_name, ct_level, ct_reliability = ct
            ctype = Dict()
            ctype.level = ct_level
            ctype.reliability = ct_reliability
            ctype.name = ct_name
            tissue.protein.cell_type.append(ctype)

            if ct_level > tissue.protein.level:
                tissue.protein.level = ct_level
                tissue.protein.reliability = ct_reliability

        # per tissue
        d.tissues.append(tissue)

    return d.to_dict()


def format_expression_with_rna(rec):
    # get gene,result,data = rec
    exp = HPAExpression(gene=rec['gene'],
                        data_release=Config.RELEASE_VERSION)

    if rec['result']:
        exp.update(rec['result'])

    if rec['data']:
        new_tissues = []
        has_tissues = len(exp.tissues) > 0

        sorted(rec['data'], key=oper.itemgetter(0))
        t_set = ft.reduce(lambda x, y: x.union(set([y['efo_code']])),
                          exp.tissues, set()) \
                    if has_tissues else set()
        nt_set = ft.reduce(lambda x, y: x.union(set([y[0]])),
                          rec['data'], set())

        intersection = t_set.intersection(nt_set)
        intersection_idxs = [i for i, e in enumerate(exp.tissues) if e['efo_code'] in intersection]
        intersection_idxs_data = [i for i, e in enumerate(rec['data']) if e[0] in intersection]
        difference = nt_set.difference(t_set)
        difference_idxs = [i for i, e in enumerate(rec['data']) if e[0] in difference]

        for i, _ in enumerate(intersection):
            tidx = intersection_idxs[i]
            didx = intersection_idxs_data[i]

            exp.tissues[tidx].rna.level = int(rec['data'][didx][2])
            exp.tissues[tidx].rna.value = float(rec['data'][didx][3])
            exp.tissues[tidx].rna.unit = rec['data'][didx][4]


        for idx in difference_idxs:
            rna = rec['data'][idx]
            t = exp.new_tissue(efo_code=rna[0],
                               label=rna[1])
            t.rna.level = int(rna[2])
            t.rna.value = float(rna[3])
            t.rna.unit = rna[4]

            new_tissues.append(t)

        # iterate all tissues
        exp.tissues.extend(new_tissues)

    return exp.to_dict()

def clean_tissue_name(tissue_name):
    tname = tissue_name
    if tissue_name.endswith(' tissue'):
        tname = tissue_name[:-7]
    
    return tname


def code_from_tissue(tissue_name):
    '''from stripped tissue name get a ripemd160 hash code none if error'''
    tid = None
    try:
        tid = hashlib.new('ripemd160', tissue_name.strip()).hexdigest()

    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.exception(e)

    return tid


# def code_from_tissue(tissue_name):
#     t2m = Config.TISSUE_TRANSLATION_MAP['tissue']
#     tid = None
#     try:
#         tid = t2m[tissue_name]
#     except KeyError:
#         logger = logging.getLogger(__name__)
#         logger.debug('the tissue name %s was not found in the mapping',
#                      tissue_name)
#         # TODO the id has to be one word to not get splitted by the analyser
#         # this is a temporal fix by the time we get all items mapped
#         tid = tissue_name.strip().replace(' ', '_')
#         tid = re.sub('[^0-9a-zA-Z_]+', '',tid)
# 
#     return tid


def hpa2tissues(hpa=None):
    '''return a list of tissues if any or empty list'''
    def _split_tissue(k, v):
        '''from tissue dict to rna and protein dicts pair'''
        tid = k
        tlabel = v['label']

        rna = {'id': tid, 'label': tlabel, 'level': v['rna']['level'],
               'unit': v['rna']['unit'],
               'value': v['rna']['value']} if v['rna'] else {}

        protein = {'id': tid, 'label': tlabel,
                   'level': v['protein']['level']} if v['protein'] else {}
        return (rna, protein)

    # generate a list with rna, protein pairs per tissue
    splitted_tissues = [_split_tissue(t['efo_code'], t) for t in hpa.tissues
                        if hpa is not None]

    rnas = [[(l+1, tissue[0]) for l in xrange(tissue[0]['level'])]
            for tissue in splitted_tissues if tissue[0]]

    proteins = [[(l+1, tissue[1]) for l in xrange(tissue[1]['level'])]
                for tissue in splitted_tissues if tissue[1]]

    def _reduce_func(x, y):
        for el in y:
            k = str(el[0])
            if k in x:
                x[k].append(el[1])
            else:
                x[k] = [el[1]]

        return x

    return {'rna': ft.reduce(_reduce_func, rnas, {}),
            'protein': ft.reduce(_reduce_func, proteins, {})}


class HPAActions(Actions):
    PROCESS = 'process'


class HPADataDownloader():
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def retrieve_normal_tissue_data(self):
        """Parse 'normal_tissue' csv file,
        the expression profiles for proteins in human tissues from HPA

        :return: dict
        """
        self.logger.info('get normal tissue rows into dicts')
        table = (
            petl.fromcsv(URLZSource(Config.HPA_NORMAL_TISSUE_URL))
            .rename({'Tissue': 'tissue',
                     'Cell type': 'cell_type',
                     'Level': 'level',
                     'Reliability': 'reliability',
                     'Gene': 'gene'})
            .cut('tissue', 'cell_type', 'level', 'reliability', 'gene')
            .addfield('tissue_label', lambda rec: clean_tissue_name(rec['tissue']\
                                                    .replace('1', '')\
                                                    .replace('2', '')\
                                                    .strip()) )
            .addfield('tissue_code', lambda rec: code_from_tissue(rec['tissue_label']))
            .addfield('tissue_level', lambda rec: level_from_text(rec['level']))
            .addfield('tissue_reliability', lambda rec: reliability_from_text(rec['reliability']))
            .cut('gene', 'tissue_code',
                 'tissue_label', 'tissue_level',
                 'tissue_reliability', 'cell_type')
            .aggregate(('gene', 'tissue_code'),
                       aggregation={'cell_types': (('cell_type','tissue_level',
                                              'tissue_reliability'),list),
                                    'tissue_label': ('tissue_label',set)},
                       presorted=True)
            .aggregate('gene', aggregation={'data': (('tissue_code',
                                                      'tissue_label',
                                                      'cell_types'),list)},
                       presorted=True)
            .addfield('result', lambda rec: format_expression(rec))
            .cut('gene','result')
            )

        return table

    def retrieve_rna_data(self):
        """
        Parse 'rna_tissue' csv file,
        RNA levels in 56 cell lines and 37 tissues based on RNA-seq from HPA.

        :return: dict
        """
        self.logger.info('get rna tissue rows into dicts')
        self.logger.debug('melting rna level table into geneid tissue level')
        t_level = (
            petl.fromcsv(URLZSource(Config.HPA_RNA_LEVEL_URL),
                               delimiter='\t')
            .melt(key='ID', variablefield='tissue', valuefield='rna_level')
            .rename({'ID': 'gene'})
            .addfield('tissue_label', lambda rec: clean_tissue_name(rec['tissue']\
                                                    .replace('1', '')\
                                                    .replace('2', '')\
                                                    .strip()) )
            .addfield('tissue_code', lambda rec: code_from_tissue(rec['tissue_label']))
            .cutout('tissue')
        )


        t_value = (
            petl.fromcsv(URLZSource(Config.HPA_RNA_VALUE_URL),
                               delimiter='\t')
            .melt(key='ID', variablefield='tissue', valuefield='rna_value')
            .rename({'ID': 'gene'})
            .addfield('tissue_label', lambda rec: rec['tissue']\
                                                    .replace('1', '')\
                                                    .replace('2', '')\
                                                    .strip() )
            .addfield('tissue_code', lambda rec: code_from_tissue(rec['tissue_label']))
            .addfield('rna_unit', 'TPM')
            .cutout('tissue')
        )

        t_join = (petl.join(t_level,
                           t_value,
                           key=('gene','tissue_code','tissue_label'),
                           presorted=True)
                  .aggregate('gene',
                             aggregation={'data': (('tissue_code',
                                                      'tissue_label',
                                                      'rna_level',
                                                      'rna_value',
                                                      'rna_unit'),list)},
                       presorted=True)
        )

        return t_join

#     def retrieve_cancer_data(self):
#         self.logger.info('retrieve cancer data from HPA')
#         table = (
#             petl.fromcsv(URLZSource(Config.HPA_CANCER_URL))
#             .rename({'Tumor': 'tumor',
#                      'Level': 'level',
#                      'Count patients': 'count_patients',
#                      'Total patients': 'total_patients',
#                      'Gene': 'gene',
#                      'Expression type': 'expression_type'})
#             .cut('tumor', 'count_patients', 'level', 'total_patients', 'gene',
#                  'expression_type')
#             )
#
#         for d in petl.dicts(table):
#             yield d
#
#     def retrieve_subcellular_location_data(self):
#         self.logger.info('retrieve subcellular location data from HPA')
#         table = (
#             petl.fromcsv(URLZSource(Config.HPA_SUBCELLULAR_LOCATION_URL))
#             .rename({'Main location': 'main_location',
#                      'Other location': 'other_location',
#                      'Gene': 'gene',
#                      'Reliability': 'reliability',
#                      'Expression type': 'expression_type'})
#             .cut('main_location', 'other_location', 'gene', 'reliability',
#                  'expression_type')
#             )
#
#         for d in table.dicts():
#             yield d


class ExpressionObjectStorer(RedisQueueWorkerProcess):

    def __init__(self, es, r_server, queue, dry_run=False):
        super(ExpressionObjectStorer, self).__init__(queue, None)
        self.es = None
        self.r_server = None
        self.loader = None
        self.dry_run = dry_run

    def process(self, data):
        geneid, gene = data
        self.loader.put(Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                       Config.ELASTICSEARCH_EXPRESSION_DOC_NAME,
                       ID=geneid,
                       body=gene,
                       create_index=False)

    def init(self):
        super(ExpressionObjectStorer, self).init()
        self.loader = Loader(dry_run=self.dry_run)

    def close(self):
        super(ExpressionObjectStorer, self).close()
        self.loader.close()


class HPAProcess():
    def __init__(self, loader, r_server):
        self.loader = loader
        self.esquery = ESQuery(loader.es)
        self.r_server = r_server
        self.downloader = HPADataDownloader()
        self.logger = logging.getLogger(__name__)
        self.hpa_normal_table = None
        self.hpa_rna_table = None
        self.hpa_merged_table = None

    def process_all(self, dry_run=False):

        self.hpa_normal_table = self.process_normal_tissue()
        self.hpa_rna_table = self.process_rna()
        self.hpa_merged_table = self.process_join()

        self.store_data(dry_run=dry_run)
        self.loader.close()

    def process_normal_tissue(self):
        return self.downloader.retrieve_normal_tissue_data()

    def process_rna(self):
        return self.downloader.retrieve_rna_data()

    def process_join(self):
        hpa_merged_table = (
            petl.outerjoin(self.hpa_normal_table, self.hpa_rna_table,
                           key='gene', presorted=True)
            .addfield('expression', lambda rec: format_expression_with_rna(rec))
            .cut('expression')
        )
        return hpa_merged_table


    def store_data(self, dry_run=False):
        self.logger.info('store_data called')

        self.logger.debug('calling to create new expression index')
        self.loader.create_new_index(Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME)
        queue = RedisQueue(queue_id=Config.UNIQUE_RUN_ID + '|expression_data_storage',
                           r_server=self.r_server,
                           serialiser='json',
                           max_size=10000,
                           job_timeout=600)

        q_reporter = RedisQueueStatusReporter([queue])
        q_reporter.start()
        loaders = min([16,Config.WORKERS_NUMBER])

        workers = [ExpressionObjectStorer(self.loader.es,
                                    None,
                                    queue,
                                    dry_run=dry_run) for _ in range(loaders)]

        for w in workers:
            w.start()

        for row in self.hpa_merged_table.data():
            # just one field with all data frommated into a dict
            hpa = row[0]
            queue.put((hpa['gene'], hpa), self.r_server)

        queue.set_submission_finished(r_server=self.r_server)

        for w in workers:
            w.join()

        q_reporter.join()

        self.logger.info('all expressions objects pushed to elasticsearch')

#         if self.data.values()[0]['cancer']:  # if there is cancer data
#             pass
#         if self.data.values()[0]['subcellular_location']:  # if there is subcellular location data
#             pass


class HPALookUpTable(object):
    """
    A redis-based pickable hpa look up table using gene id as table
    id
    """

    def __init__(self,
                 es=None,
                 namespace=None,
                 r_server=None,
                 ttl=(60*60*24+7)):
        self._es = es
        self.r_server = r_server
        self._es_query = ESQuery(self._es)
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=self.r_server,
                                             ttl=ttl)
        self._logger = logging.getLogger(__name__)
        self.tqdm_out = TqdmToLogger(self._logger,level=logging.INFO)

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

    def _get_r_server(self, r_server = None):
        return r_server if r_server else self.r_server
