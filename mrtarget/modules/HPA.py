from __future__ import absolute_import
import csv
import logging
import re
import functools as ft
from StringIO import StringIO
from zipfile import ZipFile
from tqdm import tqdm
from mrtarget.common import TqdmToLogger

import requests
import petl
from mrtarget.common import URLZSource

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery, Loader
from mrtarget.common.Redis import RedisLookupTablePickle, RedisQueueStatusReporter, RedisQueueWorkerProcess, RedisQueue

from mrtarget.Settings import Config
from addict import Dict


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


def format_expression(rec):
    d = Dict()
    d.gene = rec['gene']
    d.data_release = Config.RELEASE_VERSION
    d.tissues = []
    d.cancer = {}
    d.subcellular_location = {}

    tissues = []
    # for each tissue
    for el in rec['data']:
        t_code, t_name, c_types = el
        tissue = Dict()
        tissue.label = list(t_name)[0]
        tissue.efo_code = t_code

        protein = Dict()
        protein.level = 0
        protein.reliability = False

        lct = []
        # iterate all cell_types
        for ct in c_types:
            ct_name, ct_level, ct_reliability = ct
            ctype = Dict()
            ctype.level = ct_level
            ctype.reliability = ct_reliability
            ctype.name = ct_name
            lct.append(ctype)

            if ct_level > protein.level:
                protein.level = ct_level
                protein.reliability = ct_reliability

        protein.cell_type = lct
        tissue.protein = protein
        tissues.append(tissue)

    d.tissues = tissues
    return d.to_dict()


def format_expression_with_rna(rec):
    # get gene,result,data = rec
    return rec

def code_from_tissue(tissue_name):
    t2m = Config.TISSUE_TRANSLATION_MAP['tissue']
    tid = None
    try:
        tid = t2m[tissue_name]
    except KeyError:
        logger = logging.getLogger(__name__)
        logger.debug('the tissue name %s was not found in the mapping',
                     tissue_name)
        # TODO the id has to be one word to not get splitted by the analyser
        # this is a temporal fix by the time we get all items mapped
        tid = tissue_name.strip().replace(' ', '_')
        tid = re.sub('[^0-9a-zA-Z_]+', '',tid)

    return tid


def hpa2tissues(hpa=None):
    '''return a list of tissues if any or empty list'''
    def _split_tissue(k, v):
        '''from tissue dict to rna and protein dicts pair'''
        tid = code_from_tissue(k)
        tlabel = k

        rna = {'id': tid, 'label': tlabel, 'level': v['rna']['level'],
               'unit': v['rna']['unit'],
               'value': v['rna']['value']} if v['rna'] else {}

        protein = {'id': tid, 'label': tlabel,
                   'level': v['protein']['level']} if v['protein'] else {}
        return (rna, protein)

    # generate a list with rna, protein pairs per tissue
    splitted_tissues = [_split_tissue(k, v) for k, v in hpa.tissues.iteritems()
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


class HPAExpression(JSONSerializable):
    def __init__(self, gene=None):
        self.gene = gene
        self.tissues = []

    def get_id(self):
        return self.gene


class HPADataDownloader():
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def _download_data(self, url):
        r = requests.get(url)
        try:
            r.raise_for_status()
        except:
            raise Exception("failed to download data from url: %s. Status code: %i" % (url, r.status_code))
        zipped_data = ZipFile(StringIO(r.content))
        info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)
        return zipped_data.open(info)

    def _get_csv_reader(self, csvfile):
        return csv.DictReader(csvfile)

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
            .addfield('tissue_label', lambda rec: rec['tissue']\
                                                    .replace('1', '')\
                                                    .replace('2', '')\
                                                    .strip() )
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

        # TODO just get the element and return as a dictionary
#         for d in table.data():
#             # yielding dict result
#             yield d[0]
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
            .addfield('tissue_label', lambda rec: rec['tissue']\
                                                    .replace('1', '')\
                                                    .replace('2', '')\
                                                    .strip() )
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

    def retrieve_cancer_data(self):
        self.logger.info('retrieve cancer data from HPA')
        table = (
            petl.fromcsv(URLZSource(Config.HPA_CANCER_URL))
            .rename({'Tumor': 'tumor',
                     'Level': 'level',
                     'Count patients': 'count_patients',
                     'Total patients': 'total_patients',
                     'Gene': 'gene',
                     'Expression type': 'expression_type'})
            .cut('tumor', 'count_patients', 'level', 'total_patients', 'gene',
                 'expression_type')
            )

        for d in petl.dicts(table):
            yield d

    def retrieve_subcellular_location_data(self):
        self.logger.info('retrieve subcellular location data from HPA')
        table = (
            petl.fromcsv(URLZSource(Config.HPA_SUBCELLULAR_LOCATION_URL))
            .rename({'Main location': 'main_location',
                     'Other location': 'other_location',
                     'Gene': 'gene',
                     'Reliability': 'reliability',
                     'Expression type': 'expression_type'})
            .cut('main_location', 'other_location', 'gene', 'reliability',
                 'expression_type')
            )

        for d in table.dicts():
            yield d


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
        self.data = {}
        self.available_genes = set()
        self.set_translations()
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

    def _get_available_genes(self, ):
        return self.available_genes

    def process_normal_tissue(self):
        return self.downloader.retrieve_normal_tissue_data()

    def process_rna(self):
        return self.downloader.retrieve_rna_data()

#             gene = row['gene']
#             if gene not in self.available_genes:
#                 self.init_gene(gene)
#                 self.available_genes.add(gene)
#             if gene not in self.rna_data:
#                 self.rna_data[gene] = []
#             self.rna_data[gene].append(row)
#
#         for gene in self.available_genes:
#             self.data[gene]['expression'].tissues = self.get_rna_data_for_gene(gene)
#         self.logger.info('process_rna completed')
#         return

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
                           serialiser='jsonpickle',
                           max_size=10000,
                           job_timeout=600)

        q_reporter = RedisQueueStatusReporter([queue])
        q_reporter.start()

        workers = [ExpressionObjectStorer(self.loader.es,
                                    None,
                                    queue,
                                    dry_run=dry_run) for _ in range(4)]

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

        if self.data.values()[0]['cancer']:  # if there is cancer data
            pass
        if self.data.values()[0]['subcellular_location']:  # if there is subcellular location data
            pass

    def init_gene(self, gene):
        self.data[gene] = dict(expression=HPAExpression(gene),
                               cancer={},  # TODO
                               subcellular_location={},  # TODO
                               )

    def get_normal_tissue_data_for_gene(self, gene):
        tissue_data = {}
        for row in self.normal_tissue_data[gene]:
            # XXX why do I have to replace on a curated list of tissues
            tissue = row['tissue'].replace('1', '').replace('2', '').strip()
            # tissue = row['tissue']
            code = code_from_tissue(tissue)

            if tissue not in tissue_data:
                tissue_data[tissue] = {'protein': {
                        'cell_type': {},
                        'level': 0,
                        'reliability': False,
                    },

                        'rna': {
                        },
                        'efo_code': code
                    }

            if row['cell_type'] not in tissue_data[tissue]['protein']['cell_type']:
                tissue_data[tissue]['protein']['cell_type'][row['cell_type']] = []
            tissue_data[tissue]['protein']['cell_type'][row['cell_type']].append(
                dict(level=level_from_text(row['level']),
                     reliability=reliability_from_text(row['reliability']),
                     ))
            if level_from_text(row['level']) > tissue_data[tissue]['protein']['level']:
                tissue_data[tissue]['protein']['level'] = level_from_text(row['level'])
                # giving higher priority to reliable annotations over uncertain
            if reliability_from_text(row['reliability']):
                tissue_data[tissue]['protein']['reliability'] = True

        return tissue_data

    def get_rna_data_for_gene(self, gene):
        tissue_data = self.data[gene]['expression'].tissues

        if not tissue_data:
            tissue_data = {}

        if gene in self.rna_data:
            for row in self.rna_data[gene]:
                sample = row['sample'].replace('1', '').replace('2', '').strip()
                code = code_from_tissue(sample)

                if sample not in tissue_data:
                    tissue_data[sample] = {'protein': {
                            'cell_type': {},
                            'level': 0,
                            'reliability': False,
                        },

                            'rna': {
                            },
                            'efo_code': code
                        }

                tissue_data[sample]['rna']['value'] = float(row['value'])
                tissue_data[sample]['rna']['unit'] = row['unit']
                tissue_data[sample]['rna']['level'] = int(row['level'])

        return tissue_data


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
