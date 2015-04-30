import logging
from settings import Config
import requests
from common.PGAdapter import *
from StringIO import StringIO
import csv
from zipfile import ZipFile

__author__ = 'andreap'

logging.basicConfig(level=logging.INFO)


class HPAActions():
    DOWNLOAD='download'
    PROCESS='process'
    UPLOAD='upload'
    ALL='all'


class HPADataDownloader():


    def __init__(self, adapter):
        self.adapter = adapter
        self.session = adapter.session


    def retrieve_all(self):

        self.retrieve_normal_tissue_data()
        self.retrieve_cancer_data()
        self.retrieve_rna_data()
        self.retrieve_subcellular_location_data()


    def _download_data(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            zipped_data = ZipFile(StringIO(r.content))
            info = zipped_data.getinfo(zipped_data.filelist[0].orig_filename)
            return zipped_data.open(info)
        else:
            raise Exception("failed to download data from url: %s. Status code: %i"%(url,r.status_code) )

    def retrieve_normal_tissue_data(self):
        data =  self._download_data(Config.HPA_NORMAL_TISSUE_URL)
        self._load_normal_tissue_data_to_pg(data)

    def retrieve_rna_data(self):
        data = self._download_data(Config.HPA_RNA_URL)
        self._load_rna_data_to_pg(data)

    def retrieve_cancer_data(self):
        data =  self._download_data(Config.HPA_CANCER_URL)
        self._load_cancer_data_to_pg(data)

    def retrieve_subcellular_location_data(self):
        data = self._download_data(Config.HPA_SUBCELLULAR_LOCATION_URL)
        self._load_subcellular_location_data_to_pg(data)

    def _get_csv_reader(self, csvfile):
        return csv.DictReader(csvfile)

    def _load_normal_tissue_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPANormalTissue).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_normal_tissue'%rows_deleted)
        c=0
        for row in reader:
            self.session.add(HPANormalTissue(tissue=row['Tissue'],
                                             cell_type=row['Cell type'],
                                             level=row['Level'],
                                             reliability=row['Reliability'],
                                             gene=row['Gene'],
                                             expression_type=row['Expression type'],
                                             ))
            c+=1
        self.session.commit()
        logging.info('inserted %i rows in hpa_normal_tissue'%c)

    def _load_rna_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPARNA).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_rna'%rows_deleted)
        c=0
        for row in reader:
            self.session.add(HPARNA(sample=row['Sample'],
                                     abundance=row['Abundance'],
                                     unit=row['Unit'],
                                     value=row['Value'],
                                     gene=row['Gene'],
                                     ))
            c+=1
        self.session.commit()
        logging.info('inserted %i rows in hpa_rna'%c)

    def _load_cancer_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPACancer).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_cancer'%rows_deleted)
        c=0
        for row in reader:
            self.session.add(HPACancer(tumor=row['Tumor'],
                                       level=row['Level'],
                                       count_patients=row['Count patients'],
                                       total_patients=row['Total patients'],
                                       gene=row['Gene'],
                                       expression_type=row['Expression type'],
                                       ))
            c+=1
        self.session.commit()
        logging.info('inserted %i rows in hpa_cancer'%c)

    def _load_subcellular_location_data_to_pg(self, data):
        reader= self._get_csv_reader(data)
        rows_deleted= self.session.query(HPASubcellularLocation).delete()
        if rows_deleted:
            logging.info('deleted %i rows from hpa_subcellular_location'%rows_deleted)
        c=0
        for row in reader:
            self.session.add(HPASubcellularLocation(main_location=row['Main location'],
                                       other_location=row['Other location'],
                                       gene=row['Gene'],
                                       expression_type=row['Expression type'],
                                       reliability=row['Reliability'],
                                       ))
            c+=1
        self.session.commit()
        logging.info('inserted %i rows in hpa_subcellular_location'%c)


