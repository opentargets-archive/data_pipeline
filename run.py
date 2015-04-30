from common.PGAdapter import Adapter
from modules.HPA import HPADataDownloader, HPAActions
import argparse

__author__ = 'andreap'
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='CTTV processing pipeline')
    parser.add_argument("--hpad", dest='hpa', help="download data from human protein atlas and store it in postgres",
                        action="append_const", const = HPAActions.DOWNLOAD)
    parser.add_argument("--hpap", dest='hpa', help="process human protein atlas data stored in postgres and create json object",
                        action="append_const", const = HPAActions.PROCESS)
    parser.add_argument("--hpau", dest='hpa', help="upload processed human protein atlas json obects stored in postgres to elasticsearch",
                        action="append_const", const = HPAActions.UPLOAD)
    parser.add_argument("--hpa", dest='hpa', help="download human protein atlas data, process it and upload it to elasticsearch",
                        action="append_const", const = HPAActions.UPLOAD)
    args = parser.parse_args()

    adapter = Adapter()

    if args.hpa:
        do_all = HPAActions.ALL in args.hpa
        if (HPAActions.DOWNLOAD in args.hpa) or do_all:
            downloader = HPADataDownloader(adapter)
            downloader.retrieve_all()
        if (HPAActions.PROCESS in args.hpa) or do_all:
            pass
        if (HPAActions.UPLOAD in args.hpa) or do_all:
            pass