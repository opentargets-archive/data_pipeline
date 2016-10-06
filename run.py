import logging
import os
import socket

import sys

import time
from elasticsearch import Elasticsearch
from SPARQLWrapper import SPARQLWrapper, JSON
from common import Actions
from common.ElasticsearchLoader import Loader, ElasticsearchActions, JSONObjectStorage
from common.PGAdapter import Adapter
from modules.DataDrivenRelation import DataDrivenRelationActions, DataDrivenRelationProcess
from modules.Dump import DumpActions, DumpGenerator
from modules.ECO import EcoActions, EcoProcess, EcoUploader
from modules.EFO import EfoActions, EfoProcess, EfoUploader
from modules.EvidenceString import EvidenceStringActions, EvidenceStringProcess
from modules.EvidenceValidation import ValidationActions, EvidenceValidationFileChecker
from modules.GeneData import GeneActions, GeneManager
from modules.HPA import HPADataDownloader, HPAActions, HPAProcess, HPAUploader
from modules.Literature import LiteratureActions, Literature
from modules.QC import QCActions, QCRunner
from modules.Reactome import ReactomeActions, ReactomeDataDownloader, ReactomeProcess, ReactomeUploader
from modules.Association import AssociationActions, ScoringProcess
from modules.SearchObjects import SearchObjectActions, SearchObjectProcess
from modules.Uniprot import UniProtActions,UniprotDownloader
from modules.HGNC import HGNCActions, HGNCUploader
from modules.Ensembl import EnsemblGeneInfo, EnsemblActions, EnsemblProcess
from modules.MouseModels import MouseModelsActions, Phenodigm
from modules.IntOGen import IntOGenActions, IntOGen
from modules.Ontology import OntologyActions, PhenotypeSlim, DiseasePhenotypes
import argparse
from settings import Config
from elasticsearch_config import ElasticSearchConfiguration
from redislite import Redis

__author__ = 'andreap'

'''setup module default logging'''
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logging.basicConfig(filename = 'output.log',
                    format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)
logging.getLogger("urllib3").setLevel(logging.ERROR)

'''logger'''
logger = logging.getLogger(__name__)

class PipelineConnectors():

    def __init__(self):
        """Initialises the class

        Declares the connector parts
        """

        ''' sqlalchemy adapter '''
        self.adapter = None
        ''' Elastic Search connection'''
        self.es = None
        ''' sparql endpoint client'''
        self.sparql = None
        ''' redis '''
        self.r_server = None

    def clear_redislite_db(self):
        if os.path.exists(Config.REDISLITE_DB_PATH):
            os.remove(Config.REDISLITE_DB_PATH)

    def init_services_connections(self, redispersist=False):

        try:
            self.adapter = Adapter()
        except:
            logger.error('Cannot connect to Postgres database. skipping creation of adapter')
            self.adapter= None

        '''init es client'''
        connection_attempt = 1
        hosts=[]
        while 1:
            import socket

            try:#is a valid ip
                socket.inet_aton(Config.ELASTICSEARCH_HOST)
                hosts = [dict(host=Config.ELASTICSEARCH_HOST, port=Config.ELASTICSEARCH_PORT)]
                break
            except socket.error:#resolve nameserver to list of ips
                try:
                    socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)
                    nr_host = set([i[4][0] for i in socket.getaddrinfo(Config.ELASTICSEARCH_HOST, Config.ELASTICSEARCH_PORT)])
                    hosts = [dict(host=h, port=Config.ELASTICSEARCH_PORT) for h in nr_host ]
                    logger.info('Elasticsearch resolved to %i hosts: %s' %(len(hosts), hosts))
                    break
                except socket.gaierror:
                    wait_time = 5 * connection_attempt
                    logger.warn('Cannot resolve Elasticsearch to ip list. retrying in %i' % wait_time)
                    logger.warn('/etc/resolv.conf file: content: \n%s'%file('/etc/resolv.conf').read())
                    time.sleep(wait_time)
                    if connection_attempt > 5:
                        logger.error('Elasticsearch is not resolvable at %s' % Config.ELASTICSEARCH_URL)
                        break
                    connection_attempt+=1

        self.es = Elasticsearch(hosts = hosts,
                           maxsize=50,
                           timeout=1800,
                           sniff_on_connection_fail=True,
                           retry_on_timeout=True,
                           max_retries=10,
                           )
        connection_attempt = 1
        while not self.es.ping():
            wait_time = 3*connection_attempt
            logger.warn('Cannot connect to Elasticsearch retrying in %i'%wait_time)
            time.sleep(wait_time)
            if connection_attempt >5:
                logger.error('Elasticsearch is not reachable at %s'%Config.ELASTICSEARCH_URL)
                sys.exit(1)
                break
            connection_attempt += 1


        # es = Elasticsearch(["10.0.0.11:9200"],
        # # sniff before doing anything
        #                     sniff_on_start=True,
        #                     # refresh nodes after a node fails to respond
        #                     sniff_on_connection_fail=True,
        #                     # and also every 60 seconds
        #                     sniffer_timeout=60)
        #
        '''init sparql endpoint client'''
        self.sparql = SPARQLWrapper(Config.SPARQL_ENDPOINT_URL)


        if not redispersist:
            self.clear_redislite_db()
        self.r_server= Redis(Config.REDISLITE_DB_PATH, serverconfig={'save': []})


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
    parser.add_argument("--evs", dest='evs', help="process and validate the available evidence strings, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EvidenceStringActions.ALL)
    parser.add_argument("--assp", dest='ass', help="precompute association scores",
                        action="append_const", const = AssociationActions.PROCESS)
    parser.add_argument("--ass", dest='ass', help="precompute association scores, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = AssociationActions.ALL)
    parser.add_argument("--valck", dest='val', help="check new json files submitted to ftp site and store the evidence strings to ElasticSearch",
                        action="append_const", const = ValidationActions.CHECKFILES)
    parser.add_argument("--valgm", dest='val', help="update gene protein mapping to database",
                        action="append_const", const = ValidationActions.GENEMAPPING)
    parser.add_argument("--val", dest='val', help="check new json files submitted to ftp site, validate them and store them in postgres",
                        action="append_const", const = ValidationActions.ALL)
    parser.add_argument("--valreset", dest='valreset', help="reset audit table and previously parsed evidencestrings",
                        action="append_const", const=ValidationActions.RESET)
    parser.add_argument("--ens", dest='ens', help="retrieve and store latest ensembl gene records in elasticsearch",
                        action="append_const", const = EnsemblActions.ALL)
    parser.add_argument("--sea", dest='sea', help="precompute search results",
                        action="append_const", const = SearchObjectActions.PROCESS)
    parser.add_argument("--ddr", dest='ddr', help="precompute data driven t2t and d2d relations",
                        action="append_const", const=DataDrivenRelationActions.PROCESS)
    parser.add_argument("--persist-redis", dest='redispersist', help="use a fresh redislite db",
                        action='store_true', default=False)
    parser.add_argument("--musu", dest='mus', help="update mouse model data",
                        action="append_const", const = MouseModelsActions.UPDATE_CACHE)
    parser.add_argument("--musg", dest='mus', help="update mus musculus gene list",
                        action="append_const", const = MouseModelsActions.UPDATE_GENES)
    parser.add_argument("--muse", dest='mus', help="generate mouse model evidence",
                        action="append_const", const = MouseModelsActions.GENERATE_EVIDENCE)
    parser.add_argument("--mus", dest='mus', help="update mouse models data",
                        action="append_const", const = MouseModelsActions.ALL)
    parser.add_argument("--intogen", dest='intogen', help="parse intogen driver gene evidence",
                        action="append_const", const = IntOGenActions.ALL)
    parser.add_argument("--ontodp", dest='onto', help="parse and store disease phenotypes",
                        action="append_const", const= OntologyActions.DISEASEPHENOTYPES)
    parser.add_argument("--ontos", dest='onto', help="create phenotype slim",
                        action="append_const", const = OntologyActions.PHENOTYPESLIM)
    parser.add_argument("--onto", dest='onto', help="all ontology processing steps (phenotype slim, disease phenotypes)",
                        action="append_const", const = OntologyActions.ALL)
    parser.add_argument("--lit", dest='lit', help="fetch and process literature data",
                        action="append_const", const=LiteratureActions.ALL)
    parser.add_argument("--lit-fetch", dest='lit', help="fetch literature data",
                        action="append_const", const=LiteratureActions.FETCH)
    parser.add_argument("--lit-process", dest='lit', help="fetch literature data",
                        action="append_const", const=LiteratureActions.PROCESS)
    parser.add_argument("--qc", dest='qc',
                        help="Run quality control scripts",
                        action="append_const", const=QCActions.ALL)
    parser.add_argument("--qccgc", dest='qc',
                        help="Run quality control scripts",
                        action="append_const", const=QCActions.CGC_ANALYSIS)
    parser.add_argument("--dump", dest='dump',
                        help="dump core data to local gzipped files",
                        action="append_const", const=DumpActions.ALL)
    parser.add_argument("--datasource", dest='datasource', help="just process data for this datasource. Does not work with all the steps!!",
                        action='append', default=[])
    parser.add_argument("--targets", dest='targets', help="just process data for this target. Does not work with all the steps!!",
                        action='append', default=[])
    parser.add_argument("--dry-run", dest='dry_run', help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
                        action='store_true', default=False)
    parser.add_argument("--local-file", dest='local_file',
                        help="pass the path to a local gzipped file to use as input for the data validation step",
                        action='append', default=[])
    args = parser.parse_args()

    targets = args.targets

    connectors = PipelineConnectors()

    try:
        connectors.init_services_connections(redispersist=args.redispersist)
    except Exception, err:
        print Exception, err
        sys.exit(1)

    with Loader(connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = args.dry_run) as loader:
        run_full_pipeline = False
        if args.all  and (Actions.ALL in args.all):
            run_full_pipeline = True
        if args.hpa or run_full_pipeline:
            do_all = (HPAActions.ALL in args.hpa) or run_full_pipeline
            if (HPAActions.DOWNLOAD in args.hpa) or do_all:
                HPADataDownloader(connectors.adapter).retrieve_all()
            if (HPAActions.PROCESS in args.hpa) or do_all:
                HPAProcess(connectors.adapter).process_all()
            if (HPAActions.UPLOAD in args.hpa) or do_all:
                HPAUploader(connectors.adapter, loader).upload_all()
        if args.rea or run_full_pipeline:
            do_all = (ReactomeActions.ALL in args.rea) or run_full_pipeline
            if (ReactomeActions.DOWNLOAD in args.rea) or do_all:
                ReactomeDataDownloader(connectors.adapter).retrieve_all()
            if (ReactomeActions.PROCESS in args.rea) or do_all:
                ReactomeProcess(connectors.adapter).process_all()
            if (ReactomeActions.UPLOAD in args.rea) or do_all:
                ReactomeUploader(connectors.adapter, loader).upload_all()
        if args.uni or run_full_pipeline:
            do_all = (UniProtActions.ALL in args.uni) or run_full_pipeline
            if (UniProtActions.CACHE in args.uni) or do_all:
                UniprotDownloader(loader).cache_human_entries()
        if args.hgnc or run_full_pipeline:
            do_all = (HGNCActions.ALL in args.hgnc) or run_full_pipeline
            if (HGNCActions.UPLOAD in args.hgnc) or do_all:
                HGNCUploader(connectors.adapter).upload()
        if args.ens or run_full_pipeline:
            do_all = (EnsemblActions.ALL in args.ens) or run_full_pipeline
            if (EnsemblActions.PROCESS in args.ens) or do_all:
                EnsemblProcess(loader).process()

        if args.gen or run_full_pipeline:
            do_all = (GeneActions.ALL in args.gen) or run_full_pipeline
            if (GeneActions.MERGE in args.gen) or do_all:
                GeneManager(loader,connectors.r_server).merge_all(dry_run=args.dry_run)
        if args.efo or run_full_pipeline:
            do_all = (EfoActions.ALL in args.efo) or run_full_pipeline
            if (EfoActions.PROCESS in args.efo) or do_all:
                EfoProcess(connectors.adapter).process_all()
            if (EfoActions.UPLOAD in args.efo) or do_all:
                EfoUploader(connectors.adapter, loader).upload_all()
        if args.eco or run_full_pipeline:
            do_all = (EcoActions.ALL in args.eco) or run_full_pipeline
            if (EcoActions.PROCESS in args.eco) or do_all:
                EcoProcess(connectors.adapter).process_all()
            if (EcoActions.UPLOAD in args.eco) or do_all:
                EcoUploader(connectors.adapter, loader).upload_all()
        if args.mus or run_full_pipeline:
            do_all = (MouseModelsActions.ALL in args.mus) or run_full_pipeline
            if (MouseModelsActions.UPDATE_CACHE in args.mus) or do_all:
                Phenodigm(connectors.adapter, connectors.es, connectors.sparql).update_cache()
            if (MouseModelsActions.UPDATE_GENES in args.mus) or do_all:
                Phenodigm(connectors.adapter, connectors.es, connectors.sparql).update_genes()
            if (MouseModelsActions.GENERATE_EVIDENCE in args.mus) or do_all:
                Phenodigm(connectors.adapter, connectors.es, connectors.sparql).generate_evidence()
        if args.lit or run_full_pipeline:
            do_all = (LiteratureActions.ALL in args.lit) or run_full_pipeline
            if (LiteratureActions.FETCH in args.lit) or do_all:
                Literature(connectors.es, loader).fetch()
            if (LiteratureActions.PROCESS in args.lit) or do_all:
                Literature(connectors.es, loader, connectors.r_server).process()
        if args.intogen or run_full_pipeline:
            do_all = (IntOGenActions.ALL in args.intogen) or run_full_pipeline
            if (IntOGenActions.GENERATE_EVIDENCE in args.intogen) or do_all:
                IntOGen(connectors.es, connectors.sparql).process_intogen()
        if args.onto or run_full_pipeline:
            do_all = (OntologyActions.ALL in args.onto) or run_full_pipeline
            if (OntologyActions.PHENOTYPESLIM in args.onto) or do_all:
                PhenotypeSlim(connectors.sparql).create_phenotype_slim(args.local_file)
            if (OntologyActions.DISEASEPHENOTYPES in args.onto) or do_all:
                DiseasePhenotypes().parse_owl_url()
        if args.val or run_full_pipeline:
            do_all = (ValidationActions.ALL in args.val) or run_full_pipeline
            if (ValidationActions.GENEMAPPING in args.val) or do_all:
                EvidenceValidationFileChecker(connectors.adapter, connectors.es, connectors.r_server).map_genes()
            if (ValidationActions.CHECKFILES in args.val) or do_all:
                EvidenceValidationFileChecker(connectors.adapter, connectors.es, connectors.r_server).check_all(args.local_file)
        if args.valreset:
            EvidenceValidationFileChecker(connectors.adapter, connectors.es, connectors.r_server).reset()
        if args.evs or run_full_pipeline:
            do_all = (EvidenceStringActions.ALL in args.evs) or run_full_pipeline
            if (EvidenceStringActions.PROCESS in args.evs) or do_all:
                targets = EvidenceStringProcess(connectors.es, connectors.r_server).process_all(datasources = args.datasource,
                                                                          dry_run=args.dry_run)
        if args.ass or run_full_pipeline:
            do_all = (AssociationActions.ALL in args.ass) or run_full_pipeline
            if (AssociationActions.PROCESS in args.ass) or do_all:
                ScoringProcess(loader, connectors.r_server).process_all(targets = targets,
                                                             dry_run=args.dry_run)
        if args.ddr or run_full_pipeline:
            do_all = (DataDrivenRelationActions.ALL in args.ddr) or run_full_pipeline
            if (DataDrivenRelationActions.PROCESS in args.ddr) or do_all:
                DataDrivenRelationProcess(connectors.es, connectors.r_server).process_all(dry_run = args.dry_run)
        if args.sea or run_full_pipeline:
            do_all = (SearchObjectActions.ALL in args.sea) or run_full_pipeline
            if (SearchObjectActions.PROCESS in args.sea) or do_all:
                SearchObjectProcess(connectors.adapter, loader, connectors.r_server).process_all()
        if args.qc or run_full_pipeline:
            do_all = (QCActions.ALL in args.qc) or run_full_pipeline
            if (QCActions.QC in args.qc) or do_all:
                QCRunner(connectors.es).run_associationQC()
            # if (QCActions.CGC_ANALYSIS in args.qc) or do_all:
            #     QCRunner(es).analyse_cancer_gene_census()
        if args.dump or run_full_pipeline:
            do_all = (DumpActions.ALL in args.dump) or run_full_pipeline
            if (DumpActions.DUMP in args.dump) or do_all:
                DumpGenerator(connectors.es).dump()




