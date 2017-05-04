import csv
import logging
from StringIO import StringIO
from zipfile import ZipFile
from tqdm import tqdm

import requests

from mrtarget.common import Actions
from mrtarget.common.DataStructure import JSONSerializable
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.Redis import RedisLookupTablePickle

from mrtarget.Settings import Config


def hpa2tissues(hpa=None):
    '''return a list of tissues if any or empty list'''
    t2m = Config.TISSUE_TRANSLATION_MAP
    return [{'id': t2m[k], 'label': k} for k, _ in hpa.tissues.iteritems()
            if hpa is not None]


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

    def retrieve_all(self):

        self.retrieve_normal_tissue_data()
        self.retrieve_cancer_data()
        self.retrieve_rna_data()
        self.retrieve_subcellular_location_data()

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

    """
        Parse 'normal_tissue' csv file,
        the expression profiles for proteins in human tissues from HPA

    :return: dict
    """
    def retrieve_normal_tissue_data(self):
        reader = self._get_csv_reader(self._download_data(Config.HPA_NORMAL_TISSUE_URL))
        for c, row in enumerate(reader):
            yield dict(tissue=row['Tissue'],
                       cell_type=row['Cell type'],
                       level=row['Level'],
                       reliability=row['Reliability'],
                       gene=row['Gene'],
                       )
            if c + 1 % 10000 == 0:
                logging.debug("%i rows parsed from hpa_normal_tissue" % c)
        logging.info('parsed %i rows from hpa_normal_tissue' % c)

    """
        Parse 'rna_tissue' csv file,
        RNA levels in 56 cell lines and 37 tissues based on RNA-seq from HPA.

    :return: dict
    """
    def retrieve_rna_data(self):
        reader = self._get_csv_reader(self._download_data(Config.HPA_RNA_URL))
        for c, row in enumerate(reader):
            yield dict(sample=row['Sample'],
                       unit=row['Unit'],
                       value=row['Value'],
                       gene=row['Gene'],
                       )

            if c + 1 % 10000 == 0:
                logging.debug("%i rows uploaded to hpa_rna" % c)
        logging.info('inserted %i rows in hpa_rna' % c)

    def retrieve_cancer_data(self):
        logging.info('retrieve cancer data from HPA')
        reader = self._get_csv_reader(self._download_data(Config.HPA_CANCER_URL))
        for c, row in enumerate(reader):
            yield dict(tumor=row['Tumor'],
                       level=row['Level'],
                       count_patients=row['Count patients'],
                       total_patients=row['Total patients'],
                       gene=row['Gene'],
                       expression_type=row['Expression type'],
                       )
            if c + 1 % 10000 == 0:
                logging.debug("%i rows uploaded to hpa_cancer" % c)
            logging.info('inserted %i rows in hpa_cancer' % c)

    def retrieve_subcellular_location_data(self):
        reader = self._get_csv_reader(self._download_data(Config.HPA_SUBCELLULAR_LOCATION_URL))
        for c, row in enumerate(reader):
            yield dict(main_location=row['Main location'],
                       other_location=row['Other location'],
                       gene=row['Gene'],
                       expression_type=row['Expression type'],
                       reliability=row['Reliability'],
                       )
            if c + 1 % 10000 == 0:
                logging.debug("%i rows uploaded to hpa_subcellular_location" % c)
        logging.info('inserted %i rows in hpa_subcellular_location' % c)


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
        logging.info('process_rna completed')
        return

    def store_data(self):
        logging.info('store_data called')
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
                    'efo_code': tissue_translation_map[tissue]}
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
                is_cell_line = sample not in tissue_translation_map.keys()
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
                            'efo_code': tissue_translation_map[sample]}
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
