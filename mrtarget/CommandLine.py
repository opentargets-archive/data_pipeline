from __future__ import print_function
import logging
import argparse
import sys
import itertools as it
from logging.config import fileConfig

from mrtarget.common.Redis import enable_profiling
from mrtarget.common import Actions
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.connection import PipelineConnectors
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
from mrtarget.modules.Association import AssociationActions, ScoringProcess
from mrtarget.modules.DataDrivenRelation import DataDrivenRelationActions, DataDrivenRelationProcess
from mrtarget.modules.Dump import DumpActions, DumpGenerator
from mrtarget.modules.ECO import EcoActions, EcoProcess
from mrtarget.modules.EFO import EfoActions, EfoProcess
from mrtarget.modules.HPO import HpoActions, HpoProcess
from mrtarget.modules.MP import MpActions, MpProcess
from mrtarget.modules.Ensembl import EnsemblActions, EnsemblProcess
from mrtarget.modules.EvidenceString import EvidenceStringActions, EvidenceStringProcess
from mrtarget.modules.EvidenceValidation import ValidationActions, EvidenceValidationFileChecker
from mrtarget.modules.GeneData import GeneActions, GeneManager
from mrtarget.modules.HPA import HPAActions, HPAProcess
from mrtarget.modules.IntOGen import IntOGenActions, IntOGen
from mrtarget.modules.SLAPEnrich import SLAPEnrichActions, SLAPEnrich
from mrtarget.modules.MouseModels import MouseModelsActions, Phenodigm
from mrtarget.modules.Hallmarks import HallmarksActions, Hallmarks
from mrtarget.modules.Ontology import OntologyActions, PhenotypeSlim
from mrtarget.modules.QC import QCActions, QCRunner
from mrtarget.modules.Reactome import ReactomeActions, ReactomeProcess
from mrtarget.modules.SearchObjects import SearchObjectActions, SearchObjectProcess
from mrtarget.modules.Uniprot import UniProtActions, UniprotDownloader
from mrtarget.modules.G2P import G2PActions, G2P
from mrtarget.Settings import Config, file_or_resource, update_schema_version


def load_nlp_corpora():
    '''load here all the corpora needed by nlp steps'''
    import nltk
    nltk.download([ 'punkt', 'averaged_perceptron_tagger', 'stopwords']) #'brown' corpora might be needed


def main():

    fileConfig(file_or_resource('logging.ini'),
                              disable_existing_loggers=False)
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Open Targets processing pipeline')
    parser.add_argument('release_tag', nargs='?', default=Config.RELEASE_VERSION,
                        help='The prefix to prepend default: %s' % \
                        Config.RELEASE_VERSION)
    parser.add_argument("--all", dest='all', help="run the full pipeline (at your own risk)",
                        action="append_const", const = Actions.ALL)
    parser.add_argument("--hpa", dest='hpa', help="download human protein atlas data, process it and upload it to elasticsearch",
                        action="append_const", const = HPAActions.ALL)
    parser.add_argument("--rea", dest='rea', help="download reactome data, process it and upload it to elasticsearch",
                        action="append_const", const = ReactomeActions.ALL)
    parser.add_argument("--unic", dest='uni', help="cache the live version of uniprot human entries in elasticsearch",
                        action="append_const", const = UniProtActions.CACHE)
    parser.add_argument("--gen", dest='gen', help="merge the available gene information, store the resulting json objects in elasticsearch",
                        action="append_const", const = GeneActions.ALL)
    parser.add_argument("--efo", dest='efo', help="process the efo information, store the resulting json objects in elasticsearch",
                        action="append_const", const = EfoActions.ALL)
    parser.add_argument("--hpo", dest='hpo',
                         help="process the Human Phenotype Ontology (HPO), store the resulting json objects in elasticsearch",
                         action="append_const", const=HpoActions.ALL)
    parser.add_argument("--mp", dest='mp',
                         help="process the Mammalian Phenotype ontology (MP), store the resulting json objects in elasticsearch",
                         action="append_const", const=MpActions.ALL)
    parser.add_argument("--eco", dest='eco', help="process the eco information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EcoActions.ALL)
    parser.add_argument("--evs", dest='evs', help="process and validate the available evidence strings, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EvidenceStringActions.ALL)
    parser.add_argument("--as", dest='ass', help="precompute association scores, store the resulting json objects in postgres and upload them in elasticsearch",
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
    parser.add_argument("--persist-redis", dest='redispersist',
                        help="the temporary file wont be deleted if True default: False",
                        action='store_true', default=False)
    parser.add_argument("--musu", dest='mus', help="update mouse model data",
                        action="append_const", const = MouseModelsActions.UPDATE_CACHE)
    parser.add_argument("--musg", dest='mus', help="update mus musculus and home sapiens gene list",
                        action="append_const", const = MouseModelsActions.UPDATE_GENES)
    parser.add_argument("--muse", dest='mus', help="generate mouse model evidence",
                        action="append_const", const = MouseModelsActions.GENERATE_EVIDENCE)
    parser.add_argument("--mus", dest='mus', help="update mouse models data",
                        action="append_const", const = MouseModelsActions.ALL)
    parser.add_argument("--intogen", dest='intogen', help="parse intogen driver gene evidence",
                        action="append_const", const=IntOGenActions.GENERATE_EVIDENCE)
    parser.add_argument("--g2p", dest='g2p', help="parse gene2phenotype evidence",
                        action="append_const", const=G2PActions.GENERATE_EVIDENCE)
    parser.add_argument("--slapenrich", dest='slapenrich', help="parse SLAPEnrich pathway evidence",
                        action="append_const", const=SLAPEnrichActions.GENERATE_EVIDENCE)
    parser.add_argument("--hallmark", dest='hallmark', help="generate Hallmark Json",
                        action="append_const", const=HallmarksActions.GENERATE_JSON)
    parser.add_argument("--ontos", dest='onto', help="create phenotype slim",
                        action="append_const", const = OntologyActions.PHENOTYPESLIM)
    parser.add_argument("--onto", dest='onto', help="all ontology processing steps (phenotype slim, disease phenotypes)",
                        action="append_const", const = OntologyActions.ALL)
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
    parser.add_argument("--redis-remote", dest='redis_remote', help="connect to a remote redis",
                        action='store_true', default=False)
    parser.add_argument("--redis-host", dest='redis_host',
                        help="redis host",
                        action='store', default='')
    parser.add_argument("--redis-port", dest='redis_port',
                        help="redis port",
                        action='store', default='')
    parser.add_argument("--lt-reuse", dest='lt_reuse', help="reuse the current lookuptable",
                        action='store_true', default=False)
    parser.add_argument("--lt-namespace", dest='lt_namespace',
                        help="lookuptable namespace to reuse",
                        action='store', default='')
    parser.add_argument("--dry-run", dest='dry_run', help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
                        action='store_true', default=False)
    parser.add_argument("--profile", dest='profile', help="magically profiling process() per process",
                        action='store_true', default=False)
    parser.add_argument("--increment", dest='increment',
                        help="add new evidence from a data source but does not delete existing evidence. Works only for the validation step",
                        action='store_true', default=False)
    parser.add_argument("--input-file", dest='input_file',
                        help="pass the path to a gzipped file to use as input for the data validation step",
                        action='append', default=[])
    parser.add_argument("--log-level", dest='loglevel',
                        help="set the log level",
                        action='store', default='WARNING')
    parser.add_argument("--do-nothing", dest='do_nothing',
                        help="to be used just for test",
                        action='store_true', default=False)
    parser.add_argument("--schema-version", dest='schema_version',
                        help="set the schema version aka 'branch' name",
                        action='store', default='master')

    args = parser.parse_args()

    if not args.release_tag and not args.do_nothing:
        logger.error('A [release-tag] has to be specified.')
        print('A [release-tag] has to be specified.', file=sys.stderr)
        return 1
    else:
        Config.RELEASE_VERSION = args.release_tag

    targets = args.targets

    if args.lt_namespace:
        Config.LT_NAMESPACE = args.lt_namespace

    if args.lt_reuse:
        Config.LT_REUSE = True

    if args.redis_remote:
        Config.REDISLITE_REMOTE = args.redis_remote

    if args.redis_host:
        Config.REDISLITE_DB_HOST = args.redis_host

    if args.redis_port:
        Config.REDISLITE_DB_PORT = args.redis_port

    enable_profiling(args.profile)

    logger.debug('redis remote %s and host %s port %s',
                 str(Config.REDISLITE_REMOTE),
                 Config.REDISLITE_DB_HOST,
                 Config.REDISLITE_DB_PORT)

    connectors = PipelineConnectors()

    if args.loglevel:
        try:
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.getLevelName(args.loglevel))
            logger.setLevel(logging.getLevelName(args.loglevel))
            logger.info('main log level set to: '+ str(args.loglevel))
            root_logger.info('root log level set to: '+ str(args.loglevel))
        except Exception, e:
            root_logger.exception(e)
            sys.exit(1)

    if args.do_nothing:
        print("Exiting. I pity the fool that tells me to 'do nothing'",
              file=sys.stdout)
        return 0

    connected = connectors.init_services_connections(redispersist=args.redispersist)

    logger.debug('Attempting to establish connection to the backend... %s',
                 str(connected))

    logger.info('setting release version %s' % Config.RELEASE_VERSION)


    with Loader(connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = args.dry_run) as loader:
        run_full_pipeline = False

        # get the schema version and change all needed resources
        update_schema_version(Config,args.schema_version)
        logger.info('setting schema version string to %s', args.schema_version)

        if args.all and (Actions.ALL in args.all):
            run_full_pipeline = True
        if args.hpa or run_full_pipeline:
            do_all = (HPAActions.ALL in args.hpa) or run_full_pipeline
            if (HPAActions.PROCESS in args.hpa) or do_all:
                HPAProcess(loader,connectors.r_server).process_all(dry_run=args.dry_run)
        if args.rea or run_full_pipeline:
            do_all = (ReactomeActions.ALL in args.rea) or run_full_pipeline
            if (ReactomeActions.PROCESS in args.rea) or do_all:
                ReactomeProcess(loader).process_all()
        if args.uni or run_full_pipeline:
            do_all = (UniProtActions.ALL in args.uni) or run_full_pipeline
            if (UniProtActions.CACHE in args.uni) or do_all:
                UniprotDownloader(loader).cache_human_entries()
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
                EfoProcess(loader).process_all()
        if args.hpo or run_full_pipeline:
             do_all = (HpoActions.ALL in args.hpo) or run_full_pipeline
             if (HpoActions.PROCESS in args.hpo) or do_all:
                 HpoProcess(loader).process_all()
        if args.mp or run_full_pipeline:
             do_all = (MpActions.ALL in args.mp) or run_full_pipeline
             if (MpActions.PROCESS in args.mp) or do_all:
                 MpProcess(loader).process_all()
        if args.eco or run_full_pipeline:
            do_all = (EcoActions.ALL in args.eco) or run_full_pipeline
            if (EcoActions.PROCESS in args.eco) or do_all:
                EcoProcess(loader).process_all()
        if args.mus or run_full_pipeline:
            do_all = (MouseModelsActions.ALL in args.mus) or run_full_pipeline
            if (MouseModelsActions.UPDATE_CACHE in args.mus) or do_all:
                Phenodigm(connectors.es, connectors.r_server).update_cache()
            if (MouseModelsActions.UPDATE_GENES in args.mus) or do_all:
                Phenodigm(connectors.es, connectors.r_server).update_genes()
            if (MouseModelsActions.GENERATE_EVIDENCE in args.mus) or do_all:
                Phenodigm(connectors.es, connectors.r_server).generate_evidence()
        if args.g2p or run_full_pipeline:
            do_all = (G2PActions.ALL in args.g2p) or run_full_pipeline
            if (G2PActions.GENERATE_EVIDENCE in args.g2p) or do_all:
                G2P(connectors.es).process_g2p()
        if args.intogen or run_full_pipeline:
            do_all = (IntOGenActions.ALL in args.intogen) or run_full_pipeline
            if (IntOGenActions.GENERATE_EVIDENCE in args.intogen) or do_all:
                IntOGen(connectors.es, connectors.r_server).process_intogen()
        if args.slapenrich or run_full_pipeline:
            do_all = (SLAPEnrichActions.ALL in args.slapenrich) or run_full_pipeline
            if (SLAPEnrichActions.GENERATE_EVIDENCE in args.slapenrich) or do_all:
                SLAPEnrich(connectors.es, connectors.r_server).process_slapenrich()
        if args.hallmark or run_full_pipeline:
            do_all = (HallmarksActions.ALL in args.hallmark) or run_full_pipeline
            if (HallmarksActions.GENERATE_JSON in args.hallmark) or do_all:
                Hallmarks(loader, connectors.es, connectors.r_server).process_hallmarks()
        if args.onto or run_full_pipeline:
            do_all = (OntologyActions.ALL in args.onto) or run_full_pipeline
            if (OntologyActions.PHENOTYPESLIM in args.onto) or do_all:
                PhenotypeSlim().create_phenotype_slim(args.input_file)

        if args.input_file:
            input_files = list(it.chain.from_iterable([el.split(",") for el in args.input_file]))
        else:
            #default behaviour: use all the data sources listed in the evidences_sources.txt file
            logger.debug('reading the evidences sources URLs from evidence_sources.txt')
            with open(file_or_resource('evidences_sources.txt')) as f:
                input_files = [x.rstrip() for x in f.readlines()]

        if args.val or run_full_pipeline:
            do_all = (ValidationActions.ALL in args.val) or run_full_pipeline
            if (ValidationActions.CHECKFILES in args.val) or do_all:
                EvidenceValidationFileChecker(connectors.es,
                                              connectors.r_server,
                                              dry_run=args.dry_run).check_all(input_files=input_files,
                                                                              increment=args.increment)
        if args.valreset:
            EvidenceValidationFileChecker(connectors.es, connectors.r_server).reset()
        if args.evs or run_full_pipeline:
            do_all = (EvidenceStringActions.ALL in args.evs) or run_full_pipeline
            if (EvidenceStringActions.PROCESS in args.evs) or do_all:
                targets = EvidenceStringProcess(connectors.es,
                                                connectors.r_server,
                                                es_pub=connectors.es_pub).process_all(datasources = args.datasource,
                                                                                      dry_run=args.dry_run,
                                                                                      inject_literature=True)
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
                SearchObjectProcess(loader, connectors.r_server).process_all()
        if args.qc or run_full_pipeline:
            do_all = (QCActions.ALL in args.qc) or run_full_pipeline
            if (QCActions.QC in args.qc) or do_all:
                QCRunner(connectors.es).run_associationQC()
            # if (QCActions.CGC_ANALYSIS in args.qc) or do_all:
            #     QCRunner(es).analyse_cancer_gene_census()
        if args.dump or run_full_pipeline:
            do_all = (DumpActions.ALL in args.dump) or run_full_pipeline
            if (DumpActions.DUMP in args.dump) or do_all:
                DumpGenerator().dump()

    logger.debug('close connectors')
    connectors.close()

    logger.info('it was correctly executed - finished')
    return 0


if __name__ == '__main__':
    sys.exit(main())
