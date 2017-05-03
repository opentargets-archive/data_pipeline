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
    return [k for k, _ in hpa.tissues.iteritems()
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
                    'efo_code': self.tissue_translation[tissue]}
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
                is_cell_line = sample not in self.tissue_translation.keys()
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
                            'efo_code': self.tissue_translation[sample]}
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

        self.tissue_translation = {
            'adrenal gland': 'CL_0000336',
            'appendix': 'EFO_0000849',
            'bone marrow': 'UBERON_0002371',
            'breast': 'UBERON_0000310',
            'bronchus': 'UBERON_0002185',
            'cerebellum': 'UBERON_0002037',
            'cerebral cortex': 'UBERON_0000956',
            'cervix, uterine': 'EFO_0000979',
            'colon': 'UBERON_0001155',
            'duodenum': 'UBERON_0002114',
            'endometrium': 'UBERON_0001295',
            'epididymis': 'UBERON_0001301',
            'esophagus': 'UBERON_0001043',
            'fallopian tube': 'UBERON_0003889',
            'gallbladder': 'UBERON_0002110',
            'heart muscle': 'UBERON_0002349',
            'hippocampus': 'EFO_0000530',
            'kidney': 'UBERON_0002113',
            'lateral ventricle': 'EFO_0001961',
            'liver': 'UBERON_0002107',
            'lung': 'UBERON_0002048',
            'lymph node': 'UBERON_0000029',
            'nasopharynx': 'nasopharynx',  # TODO: nothing matching except nasopharynx cancers
            'oral mucosa': 'UBERON_0003729',
            'ovary': 'EFO_0000973',
            'pancreas': 'UBERON_0001264',
            'parathyroid gland': 'CL_0000446',
            'placenta': 'UBERON_0001987',
            'prostate': 'UBERON_0002367',
            'rectum': 'UBERON_0001052',
            'salivary gland': 'UBERON_0001044',
            'seminal vesicle': 'UBERON_0000998',
            'skeletal muscle': 'CL_0000188',
            'skin': 'EFO_0000962',
            'small intestine': 'UBERON_0002108',
            'smooth muscle': 'EFO_0000889',
            'soft tissue': 'soft_tissue',
            # TODO: cannot map automatically to anything except: EFO_0000691 that is sarcoma (and includes soft
            # tissue tumor)
            'spleen': 'UBERON_0002106',
            'stomach': 'UBERON_0000945',
            'testis': 'UBERON_0000473',
            'thyroid gland': 'UBERON_0002046',
            'tonsil': 'UBERON_0002372',
            'urinary bladder': 'UBERON_0001255',
            'vagina': 'UBERON_0000996',
            'adipose tissue': 'adipose tissue',
            ## new tissue types added for hpa v16
            'caudate': 'UBERON_0005383',
            'eye': 'UBERON_0000970',
            'hair': 'EFO_0007824',
            'hypothalamus': 'UBERON_0001898',
            'lactating breast': 'lactating breast',
            'pituitary gland': 'UBERON_0000007',
            'retina': 'UBERON_0000966',
            'skin 1': 'UBERON_0000014',
            'skin 2': 'UBERON_0000014',
            'endometrium 1': 'UBERON_0001295',
            'endometrium 2': 'UBERON_0001295',
            'soft tissue 1': 'UBERON_0000916',
            'soft tissue 2': 'UBERON_0000916',
            'stomach 1': 'UBERON_0000945',
            'stomach 2': 'UBERON_0000945',
            ## new tissue types added for atlas baseline rna expression
            'Brodmann(1909) area 24': 'UBERON_0006101',
            'Brodmann(1909) area 9': 'UBERON_0013540',
            'C1 segment of cervical spinal cord': 'UBERON_0006469',
            'EBV - transformed lymphocyte': 'CL_0000542',
            'amygdala': 'UBERON_0001876',
            'aorta': 'UBERON_0000947',
            'atrial appendage of heart': 'UBERON_0006618',
            'blood': 'UBERON_0000178',
            'brain': 'UBERON_0000955',
            'breast(mammary tissue)': 'UBERON_0000310',
            'caudate nucleus': 'UBERON_0001873',
            'cerebellar hemisphere': 'UBERON_0002245',
            'coronary artery': 'UBERON_0001621',
            'cortex of kidney': 'UBERON_0001225',
            'ectocervix': 'UBERON_0012249',
            'endocervix': 'UBERON_0000458',
            'esophagus muscularis mucosa': 'UBERON_0004648',
            'frontal lobe': 'UBERON_0016525',
            'gall bladder': 'UBERON_0002110',
            'gastroesophageal junction': 'UBERON_0007650',
            'heart': 'UBERON_0000948',
            'heart left ventricle': 'UBERON_0002084',
            'hippocampus proper': 'UBERON_0001954',
            'ileum': 'UBERON_0002116',
            'leukocyte': 'CL_0000738',
            'minor salivary gland': 'UBERON_0001830',
            'mucosa of esophagus': 'UBERON_0002469',
            'nucleus accumbens': 'UBERON_0001882',
            'omental fat pad': 'UBERON_0010414',
            'prefrontal cortex': 'UBERON_0000451',
            'prostate gland': 'UBERON_0002367',
            'putamen': 'UBERON_0001874',
            'saliva - secreting gland': 'UBERON_0001044',
            'sigmoid colon': 'UBERON_0001159',
            'skeletal muscle tissue': 'UBERON_0001134',
            'skin of lower leg': 'UBERON_0004264',
            'skin of suprapubic region': 'UBERON_0013203',
            'smooth muscle tissue': 'UBERON_0001135',
            'subcutaneous adipose tissue': 'UBERON_0002190',
            'substantia nigra': 'UBERON_0002038',
            'temporal lobe': 'UBERON_0001871',
            'tibial artery': 'UBERON_0007610',
            'tibial nerve': 'UBERON_0001323',
            'transformed skin fibroblast': 'CL_0000057',
            'transverse colon': 'UBERON_0001157',
            'uterus': 'UBERON_0000995',
            'vermiform appendix': 'UBERON_0001154',
            'zone of skin': 'UBERON_0000014',
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
