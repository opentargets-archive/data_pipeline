from common.PGAdapter import Adapter
from modules.HPA import HPADataDownloader

__author__ = 'andreap'
if __name__ == '__main__':
    adapter = Adapter()
    downloader = HPADataDownloader(adapter)
    downloader.retrieve_all()