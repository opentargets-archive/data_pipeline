
from settings import Config
import requests
from common.PGAdapter import *
from StringIO import StringIO
import csv
from zipfile import ZipFile
import zlib

__author__ = 'andreap'




class HPADataDownloader():


    def __init__(self, adapter):
        self.adapter = adapter


    def retrieve_all(self):

        self.retrieve_normal_tissue_data()
        self.retrieve_cancer_data()
        self.retrieve_rna_data()
        self.retrieve_subcellular_location_data()


    def _download_data(self, url):
        r = requests.get(url)
        if r.status_code == 200:
            # return  zlib.decompress(r.content, 16+zlib.MAX_WBITS)
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
        for row in reader:
            print row
            exit()

    def _load_rna_data_to_pg(self, data):
        pass

    def _load_cancer_data_to_pg(self, data):
        pass

    def _load_subcellular_location_data_to_pg(self, data):
        pass


