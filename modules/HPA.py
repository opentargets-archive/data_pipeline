import csv
import logging
from StringIO import StringIO
from zipfile import ZipFile

import requests

from common import Actions
from common.DataStructure import JSONSerializable
from settings import Config

__author__ = 'andreap'


class HPAActions(Actions):
    PROCESS = 'process'


class HPAExpression(JSONSerializable):
    def __init__(self, gene):
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

    def retrieve_normal_tissue_data(self):
        reader = self._get_csv_reader(self._download_data(Config.HPA_NORMAL_TISSUE_URL))
        for c, row in enumerate(reader):
            yield dict(tissue=row['Tissue'],
                       cell_type=row['Cell type'],
                       level=row['Level'],
                       reliability=row['Reliability'],
                       gene=row['Gene'],
                       expression_type=row['Expression type'],
                       )
            if c + 1 % 10000 == 0:
                logging.info("%i rows parsed from hpa_normal_tissue" % c)
        logging.info('parsed %i rows from hpa_normal_tissue' % c)

    def retrieve_rna_data(self):
        reader = self._get_csv_reader(self._download_data(Config.HPA_RNA_URL))
        for c, row in enumerate(reader):
            yield dict(sample=row['Sample'],
                       abundance=row['Abundance'],
                       unit=row['Unit'],
                       value=row['Value'],
                       gene=row['Gene'],
                       )

            if c + 1 % 10000 == 0:
                logging.info("%i rows uploaded to hpa_rna" % c)
        logging.info('inserted %i rows in hpa_rna' % c)

    def retrieve_cancer_data(self):
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
                logging.info("%i rows uploaded to hpa_cancer" % c)
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
                logging.info("%i rows uploaded to hpa_subcellular_location" % c)
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
        self.process_cancer()
        self.process_subcellular_location()
        self.store_data()

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
        for row in self.downloader.retrieve_normal_tissue_data():
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
        return

    def process_cancer(self):
        pass

    def process_subcellular_location(self):
        pass

    def store_data(self):
        if self.data.values()[0]['expression']:  # if there is expression data

            for data in self.data.values():
                self.loader.put(index_name=Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME,
                                doc_type=Config.ELASTICSEARCH_EXPRESSION_DOC_NAME,
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
                    'expression_type': '',
                    'reliability': False,
                },

                    'rna': {
                    },
                    'efo_code': self.tissue_translation[tissue]}
            if row['cell_type'] not in tissue_data[tissue]['protein']['cell_type']:
                tissue_data[tissue]['protein']['cell_type'][row['cell_type']] = []
            tissue_data[tissue]['protein']['cell_type'][row['cell_type']].append(
                dict(level=self.level_translation[row['level']],
                     expression_type=row['expression_type'],
                     reliability=self.reliability_translation[row['reliability']],
                     ))
            if self.level_translation[row['level']] > tissue_data[tissue]['protein']['level']:
                tissue_data[tissue]['protein']['level'] = self.level_translation[row['level']]  # TODO: improvable by
                # giving higher priority to reliable annotations over uncertain
            if not tissue_data[tissue]['protein']['expression_type']:
                tissue_data[tissue]['protein']['expression_type'] = row['expression_type']
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
                cell_line_data[sample]['rna']['level'] = self.level_translation[row['abundance']]
                cell_line_data[sample]['rna']['value'] = row['value']
                cell_line_data[sample]['rna']['unit'] = row['unit']
            else:
                if sample not in tissue_data:
                    tissue_data[sample] = {'protein': {
                        'cell_type': {},
                        'level': 0,
                        'expression_type': '',
                        'reliability': False,
                    },

                        'rna': {
                        },
                        'efo_code': self.tissue_translation[sample]}
                tissue_data[sample]['rna']['level'] = self.level_translation[row['abundance']]
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
        }
