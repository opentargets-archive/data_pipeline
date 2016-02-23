import logging
import os

import sys
from elasticsearch import Elasticsearch
from common import Actions
from common.ElasticsearchLoader import Loader, ElasticsearchActions, JSONObjectStorage
from common.PGAdapter import Adapter
from modules.ECO import EcoActions, EcoProcess, EcoUploader
from modules.EFO import EfoActions, EfoProcess, EfoUploader
from modules.EvidenceString import EvidenceStringActions, EvidenceStringProcess, EvidenceStringUploader
from modules.EvidenceValidation import ValidationActions, EvidenceValidationFileChecker
from modules.GeneData import GeneActions, GeneManager, GeneUploader
from modules.HPA import HPADataDownloader, HPAActions, HPAProcess, HPAUploader
from modules.Reactome import ReactomeActions, ReactomeDataDownloader, ReactomeProcess, ReactomeUploader
from modules.Association import AssociationActions, ScoringProcess, ScoringUploader, ScoringExtract
from modules.SearchObjects import SearchObjectActions, SearchObjectProcess
from modules.Uniprot import UniProtActions,UniprotDownloader
from modules.HGNC import HGNCActions, HGNCUploader
from modules.Ensembl import EnsemblGeneInfo, EnsemblActions, EnsemblProcess
import argparse
from settings import Config, ElasticSearchConfiguration
from redislite import Redis


def clear_redislite_db():
    if os.path.exists(Config.REDISLITE_DB_PATH):
        os.remove(Config.REDISLITE_DB_PATH)

__author__ = 'andreap'
if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='CTTV processing pipeline')
    parser.add_argument("--all", dest='all', help="run the full pipeline (at your own risk)",
                        action="append_const", const = Actions.ALL)
    parser.add_argument("--hpad", dest='hpa', help="download data from human protein atlas and store it in postgres",
                        action="append_const", const = HPAActions.DOWNLOAD)
    parser.add_argument("--hpap", dest='hpa', help="process human protein atlas data stored in postgres and create json object",
                        action="append_const", const = HPAActions.PROCESS)
    parser.add_argument("--hpau", dest='hpa', help="upload processed human protein atlas json obects stored in postgres to elasticsearch",
                        action="append_const", const = HPAActions.UPLOAD)
    parser.add_argument("--hpa", dest='hpa', help="download human protein atlas data, process it and upload it to elasticsearch",
                        action="append_const", const = HPAActions.ALL)
    parser.add_argument("--read", dest='rea', help="download data from reactome and store it in postgres",
                        action="append_const", const = ReactomeActions.DOWNLOAD)
    parser.add_argument("--reap", dest='rea', help="process reactome data stored in postgres and create json object",
                        action="append_const", const = ReactomeActions.PROCESS)
    parser.add_argument("--reau", dest='rea', help="upload processed reactome json obects stored in postgres to elasticsearch",
                        action="append_const", const = ReactomeActions.UPLOAD)
    parser.add_argument("--rea", dest='rea', help="download reactome data, process it and upload it to elasticsearch",
                        action="append_const", const = ReactomeActions.ALL)
    parser.add_argument("--unic", dest='uni', help="cache the live version of uniprot human entries in postgresql",
                        action="append_const", const = UniProtActions.CACHE)
    parser.add_argument("--genm", dest='gen', help="merge the available gene information and store the resulting json objects in postgres",
                        action="append_const", const = GeneActions.MERGE)
    parser.add_argument("--genu", dest='gen', help="upload the stored json gene object to elasticsearch",
                        action="append_const", const = GeneActions.UPLOAD)
    parser.add_argument("--hgncu", dest='hgnc', help="upload the HGNC json file to the lookups schema in postgres",
                        action="append_const", const = HGNCActions.UPLOAD)
    parser.add_argument("--gen", dest='gen', help="merge the available gene information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = GeneActions.ALL)
    parser.add_argument("--efop", dest='efo', help="process the efo information and store the resulting json objects in postgres",
                        action="append_const", const = EfoActions.PROCESS)
    parser.add_argument("--efou", dest='efo', help="upload the stored json efo object to elasticsearch",
                        action="append_const", const = EfoActions.UPLOAD)
    parser.add_argument("--efo", dest='efo', help="process the efo information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EfoActions.ALL)
    parser.add_argument("--ecop", dest='eco', help="process the eco information and store the resulting json objects in postgres",
                        action="append_const", const = EcoActions.PROCESS)
    parser.add_argument("--ecou", dest='eco', help="upload the stored json efo object to elasticsearch",
                        action="append_const", const = EcoActions.UPLOAD)
    parser.add_argument("--eco", dest='eco', help="process the eco information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EcoActions.ALL)
    parser.add_argument("--evsp", dest='evs', help="process and validate the available evidence strings and store the resulting json object in postgres ",
                        action="append_const", const = EvidenceStringActions.PROCESS)
    parser.add_argument("--evsu", dest='evs', help="upload the stored json evidence string object to elasticsearch",
                        action="append_const", const = EvidenceStringActions.UPLOAD)
    parser.add_argument("--evs", dest='evs', help="process and validate the available evidence strings, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EvidenceStringActions.ALL)
    parser.add_argument("--asse", dest='ass', help="extract data relevant to scoring",
                        action="append_const", const = AssociationActions.EXTRACT)
    parser.add_argument("--assp", dest='ass', help="precompute association scores",
                        action="append_const", const = AssociationActions.PROCESS)
    parser.add_argument("--assu", dest='ass', help="upload the stored precomputed score json object to elasticsearch",
                        action="append_const", const = AssociationActions.UPLOAD)
    parser.add_argument("--ass", dest='evs', help="precompute association scores, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = AssociationActions.ALL)
    parser.add_argument("--esr", dest='es', help="clear all data in elasticsearch and load all the data stored in postgres for any index and any doc type",
                        action="append_const", const = ElasticsearchActions.RELOAD)
    parser.add_argument("--valck", dest='val', help="check new json files submitted to ftp site",
                        action="append_const", const = ValidationActions.CHECKFILES)
    parser.add_argument("--valgm", dest='val', help="update gene protein mapping to database",
                        action="append_const", const = ValidationActions.GENEMAPPING)
    parser.add_argument("--val", dest='val', help="check new json files submitted to ftp site, validate them and store them in postgres",
                        action="append_const", const = ValidationActions.ALL)
    parser.add_argument("--ens", dest='ens', help="retrieve and store latest ensembl gene records in elasticsearch",
                        action="append_const", const = EnsemblActions.ALL)
    parser.add_argument("--seap", dest='sea', help="precompute search results",
                        action="append_const", const = SearchObjectActions.PROCESS)
    parser.add_argument("--persist-redis", dest='redisperist', help="use a fresh redislite db",
                        action='store_true', default=False)
    args = parser.parse_args()

    adapter = Adapter()
    '''init es client'''
    es = Elasticsearch(Config.ELASTICSEARCH_URL)
    # es = Elasticsearch(["10.0.0.11:9200"],
    # # sniff before doing anything
    #                     sniff_on_start=True,
    #                     # refresh nodes after a node fails to respond
    #                     sniff_on_connection_fail=True,
    #                     # and also every 60 seconds
    #                     sniffer_timeout=60)
    #
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    for handler in logger.handlers:
        handler.setFormatter(formatter)

    logging.basicConfig(level=logging.INFO)
    logging.getLogger('elasticsearch').setLevel(logging.ERROR)
    logging.getLogger("requests").setLevel(logging.ERROR)
    logging.getLogger("urllib3").setLevel(logging.ERROR)
    logger.info('pointing to elasticsearch at:'+Config.ELASTICSEARCH_URL)
    if not args.redisperist:
        clear_redislite_db()
    r_server= Redis(Config.REDISLITE_DB_PATH)
    with Loader(es, chunk_size=ElasticSearchConfiguration.bulk_load_chunk) as loader:
        run_full_pipeline = False
        if args.all  and (Actions.ALL in args.all):
            run_full_pipeline = True
        if args.hpa or run_full_pipeline:
            do_all = (HPAActions.ALL in args.hpa) or run_full_pipeline
            if (HPAActions.DOWNLOAD in args.hpa) or do_all:
                HPADataDownloader(adapter).retrieve_all()
            if (HPAActions.PROCESS in args.hpa) or do_all:
                HPAProcess(adapter).process_all()
            if (HPAActions.UPLOAD in args.hpa) or do_all:
                HPAUploader(adapter, loader).upload_all()
        if args.rea or run_full_pipeline:
            do_all = (ReactomeActions.ALL in args.rea) or run_full_pipeline
            if (ReactomeActions.DOWNLOAD in args.rea) or do_all:
                ReactomeDataDownloader(adapter).retrieve_all()
            if (ReactomeActions.PROCESS in args.rea) or do_all:
                ReactomeProcess(adapter).process_all()
            if (ReactomeActions.UPLOAD in args.rea) or do_all:
                ReactomeUploader(adapter, loader).upload_all()
        if args.uni or run_full_pipeline:
            do_all = (UniProtActions.ALL in args.uni) or run_full_pipeline
            if (UniProtActions.CACHE in args.uni) or do_all:
                UniprotDownloader(adapter).cache_human_entries()
        if args.hgnc or run_full_pipeline:
            do_all = (HGNCActions.ALL in args.hgnc) or run_full_pipeline
            if (HGNCActions.UPLOAD in args.hgnc) or do_all:
                HGNCUploader(adapter).upload()
        if args.ens or run_full_pipeline:
            do_all = (EnsemblActions.ALL in args.ens) or run_full_pipeline
            if (EnsemblActions.PROCESS in args.ens) or do_all:
                EnsemblProcess(loader).process()

        if args.gen or run_full_pipeline:
            do_all = (GeneActions.ALL in args.gen) or run_full_pipeline
            if (GeneActions.MERGE in args.gen) or do_all:
                GeneManager(adapter).merge_all()
            if (GeneActions.UPLOAD in args.gen) or do_all:
                GeneUploader(adapter, loader).upload_all()
        if args.efo or run_full_pipeline:
            do_all = (EfoActions.ALL in args.efo) or run_full_pipeline
            if (EfoActions.PROCESS in args.efo) or do_all:
                EfoProcess(adapter).process_all()
            if (EfoActions.UPLOAD in args.efo) or do_all:
                EfoUploader(adapter, loader).upload_all()
        if args.eco or run_full_pipeline:
            do_all = (EcoActions.ALL in args.eco) or run_full_pipeline
            if (EcoActions.PROCESS in args.eco) or do_all:
                EcoProcess(adapter).process_all()
            if (EcoActions.UPLOAD in args.eco) or do_all:
                EcoUploader(adapter, loader).upload_all()
        if args.val or run_full_pipeline:
            do_all = (ValidationActions.ALL in args.val) or run_full_pipeline
            if (ValidationActions.GENEMAPPING in args.val) or do_all:
                EvidenceValidationFileChecker(adapter, es).map_genes()
            if (ValidationActions.CHECKFILES in args.val) or do_all:
                EvidenceValidationFileChecker(adapter, es).check_all()
        if args.evs or run_full_pipeline:
            do_all = (EvidenceStringActions.ALL in args.evs) or run_full_pipeline
            if (EvidenceStringActions.PROCESS in args.evs) or do_all:
                EvidenceStringProcess(adapter).process_all()
            # if (EvidenceStringActions.UPLOAD in args.evs) or do_all:
            #     EvidenceStringUploader(adapter, loader).upload_all()
        if args.ass or run_full_pipeline:
            do_all = (AssociationActions.ALL in args.ass) or run_full_pipeline
            if (AssociationActions.EXTRACT in args.ass) or do_all:
                ScoringExtract(adapter).extract()
            if (AssociationActions.PROCESS in args.ass) or do_all:
                ScoringProcess(adapter, loader).process_all()
            # if (AssociationActions.UPLOAD in args.ass):# data will be uploaded also by the proces step
            #     ScoringUploader(adapter, loader).upload_all()
        if args.sea or run_full_pipeline:
            do_all = (SearchObjectActions.ALL in args.sea) or run_full_pipeline
            if (SearchObjectActions.PROCESS in args.sea) or do_all:
                SearchObjectProcess(adapter, loader, r_server).process_all()
        '''only run if explicetely called'''
        if args.es:
            if ElasticsearchActions.RELOAD in args.es:
                JSONObjectStorage.refresh_all_data_in_es(loader,adapter.session)




