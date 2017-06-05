from __future__ import absolute_import
import csv
import logging
import functools as ft
from StringIO import StringIO
from zipfile import ZipFile
from tqdm import tqdm

import requests
import petl
from mrtarget.common import URLZSource

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle

from mrtarget.Settings import Config


def hpa2tissues(hpa=None):
    '''return a list of tissues if any or empty list'''
    def _split_tissue(k, v):
        '''from tissue dict to rna and protein dicts pair'''
        t2m = Config.TISSUE_TRANSLATION_MAP
        tid = t2m[k]
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
        self.tissues = {}
        self.cell_lines = {}

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
            )

        for d in petl.dicts(table):
            yield d

    def retrieve_rna_data(self):
        """
        Parse 'rna_tissue' csv file,
        RNA levels in 56 cell lines and 37 tissues based on RNA-seq from HPA.

        :return: dict
        """
        self.logger.info('get rna tissue rows into dicts')
        table = (
            petl.fromcsv(URLZSource(Config.HPA_RNA_URL))
            .rename({'Sample': 'sample',
                     'Unit': 'unit',
                     'Value': 'value',
                     'Gene': 'gene'})
            .cut('sample', 'unit', 'value', 'gene')
            )

        for d in petl.dicts(table):
            yield d

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


class HPAProcess():
    def __init__(self, loader):
        self.loader = loader
        self.data = {}
        self.available_genes = set()
        self.set_translations()
        self.downloader = HPADataDownloader()
        self.logger = logging.getLogger(__name__)

    def process_all(self):

        self.process_normal_tissue()
        self.process_rna()
        self.store_data()
        self.loader.close()

    def _get_available_genes(self, ):
        return self.available_genes

    def process_normal_tissue(self):
        self.normal_tissue_data = dict()
        for row in self.downloader.retrieve_normal_tissue_data():
            gene = row['gene']
            if gene not in self.available_genes:
                self.init_gene(gene)
                self.normal_tissue_data[gene] = []
                self.available_genes.add(gene)
            self.normal_tissue_data[gene].append(row)
        for gene in self.available_genes:
            self.data[gene]['expression'].tissues = self.get_normal_tissue_data_for_gene(gene)
        return

    def process_rna(self):
        self.rna_data = dict()
        for row in self.downloader.retrieve_rna_data():
            gene = row['gene']
            if gene not in self.available_genes:
                self.init_gene(gene)
                self.available_genes.add(gene)
            if gene not in self.rna_data:
                self.rna_data[gene] = []
            self.rna_data[gene].append(row)

        for gene in self.available_genes:
            self.data[gene]['expression'].tissues, \
            self.data[gene]['expression'].cell_lines = self.get_rna_data_for_gene(gene)
        self.logger.info('process_rna completed')
        return

    def store_data(self):
        self.logger.info('store_data called')
        if self.data.values()[0]['expression']:  # if there is expression data

            for gene, data in self.data.items():
                self.loader.put(index_name=Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                                doc_type=Config.ELASTICSEARCH_EXPRESSION_DOC_NAME,
                                ID=gene,
                                body=data['expression'].to_json(),
                                )
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
            tissue = row['tissue'].replace('1', '').replace('2', '').strip()
            if tissue not in tissue_data:
                tissue_data[tissue] = {'protein': {
                    'cell_type': {},
                    'level': 0,
                    'reliability': False,
                },

                    'rna': {
                    },
                    'efo_code': Config.TISSUE_TRANSLATION_MAP[tissue]}
            if row['cell_type'] not in tissue_data[tissue]['protein']['cell_type']:
                tissue_data[tissue]['protein']['cell_type'][row['cell_type']] = []
            tissue_data[tissue]['protein']['cell_type'][row['cell_type']].append(
                dict(level=self.level_translation[row['level']],
                     reliability=self.reliability_translation[row['reliability']],
                     ))
            if self.level_translation[row['level']] > tissue_data[tissue]['protein']['level']:
                tissue_data[tissue]['protein']['level'] = self.level_translation[row['level']]  # TODO: improvable by
                # giving higher priority to reliable annotations over uncertain
            if self.reliability_translation[row['reliability']]:
                tissue_data[tissue]['protein']['reliability'] = True

        return tissue_data

    def get_rna_data_for_gene(self, gene):
        tissue_data = self.data[gene]['expression'].tissues
        cell_line_data = {}
        if not tissue_data:
            tissue_data = {}
            for row in self.rna_data[gene]:
                sample = row['sample']
                is_cell_line = sample not in Config.TISSUE_TRANSLATION_MAP.keys()
                if is_cell_line:
                    if sample not in cell_line_data:
                        cell_line_data[sample] = {'rna': {},
                                                  }
                    cell_line_data[sample]['rna']['value'] = row['value']
                    cell_line_data[sample]['rna']['unit'] = row['unit']
                else:
                    if sample not in tissue_data:
                        tissue_data[sample] = {'protein': {
                            'cell_type': {},
                            'level': 0,
                            'reliability': False,
                        },

                            'rna': {
                            },
                            'efo_code': Config.TISSUE_TRANSLATION_MAP[sample]}
                    tissue_data[sample]['rna']['value'] = row['value']
                    tissue_data[sample]['rna']['unit'] = row['unit']
        return tissue_data, cell_line_data

    def set_translations(self):
        self.level_translation = {'Not detected': 0,
                                  'Low': 1,
                                  'Medium': 2,
                                  'High': 3,
                                  }
        self.reliability_translation = {'Supportive': True,
                                        'Uncertain': False,
                                        ## new types for hpa v16
                                        'Approved' : True,
                                        'Supported': True,
                                        }


class HPALookUpTable(object):
    """
    A redis-based pickable hpa look up table using gene id as table
    id
    """

    def __init__(self,
                 es,
                 namespace=None,
                 r_server=None,
                 ttl=(60*60*24+7)):
        self._table = RedisLookupTablePickle(namespace=namespace,
                                             r_server=r_server,
                                             ttl=ttl)
        self._es = es
        self._es_query = ESQuery(es)
        self.r_server = r_server
        if r_server is not None:
            self._load_hpa_data(r_server)

    def _load_hpa_data(self, r_server=None):
        for el in tqdm(self._es_query.get_all_hpa(),
                       desc='loading hpa',
                       unit=' hpa',
                       unit_scale=True,
                       total=self._es_query.count_all_hpa(),
                       leave=False):
            self._table.set(el['gene'], el,
                            r_server=self._get_r_server(r_server))

    def get_hpa(self, idx, r_server=None):
        return self._table.get(idx, r_server=r_server)

    def set_hpa(self, hpa, r_server=None):
        self._table.set(hpa['gene'], hpa,
                        r_server=self._get_r_server(r_server))

    def get_available_hpa_ids(self, r_server=None):
        return self._table.keys()

    def __contains__(self, key, r_server=None):
        return self._table.__contains__(key,
                                        r_server=self._get_r_server(r_server))

    def __getitem__(self, key, r_server=None):
        return self.get_hpa(key, r_server)

    def __setitem__(self, key, value, r_server=None):
        self._table.set(key, value, r_server=self._get_r_server(r_server))

    def _get_r_server(self, r_server=None):
        if not r_server:
            r_server = self.r_server
        if r_server is None:
            raise AttributeError('A redis server is required either at class'
                                 ' instantation or at the method level')
        return r_server

    def keys(self):
        return self._table.keys()
