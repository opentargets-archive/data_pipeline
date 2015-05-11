import logging
from elasticsearch import Elasticsearch
from common.ElasticsearchLoader import Loader
from common.PGAdapter import Adapter
from modules.HPA import HPADataDownloader, HPAActions, HPAProcess, HPAUploader
from modules.Uniprot import UniProtActions,UniprotDownloader
import argparse
from settings import Config, ElasticSearchConfiguration


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
                        action="append_const", const = HPAActions.ALL)
    parser.add_argument("--unic", dest='uni', help="cache the live version of uniprot human entries in postgresql",
                        action="append_const", const = UniProtActions.CACHE)
    args = parser.parse_args()

    adapter = Adapter()
    '''init es client'''
    print 'pointing to elasticsearch at:', Config.ELASTICSEARCH_URL
    es = Elasticsearch(Config.ELASTICSEARCH_URL)
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)
    # es = Elasticsearch(["10.0.0.11:9200"],
    # # sniff before doing anything
    #                     sniff_on_start=True,
    #                     # refresh nodes after a node fails to respond
    #                     sniff_on_connection_fail=True,
    #                     # and also every 60 seconds
    #                     sniffer_timeout=60)
    #

    logging.getLogger("requests").setLevel(logging.ERROR)
    with Loader(es, chunk_size=ElasticSearchConfiguration.bulk_load_chunk) as loader:
        if args.hpa:
            do_all = HPAActions.ALL in args.hpa
            if (HPAActions.DOWNLOAD in args.hpa) or do_all:
                HPADataDownloader(adapter).retrieve_all()
            if (HPAActions.PROCESS in args.hpa) or do_all:
                HPAProcess(adapter).process_all()
            if (HPAActions.UPLOAD in args.hpa) or do_all:
                HPAUploader(adapter, loader).upload_all()
        if args.uni:
            do_all = UniProtActions.ALL in args.uni
            if (UniProtActions.CACHE in args.uni) or do_all:
                UniprotDownloader(adapter).cache_human_entries()
