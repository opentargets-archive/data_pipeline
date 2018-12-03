from __future__ import print_function, absolute_import
import logging
import argparse
import sys
import os
import itertools
from logging.config import fileConfig

from mrtarget.modules.Evidences import process_evidences_pipeline
from mrtarget.common.Redis import enable_profiling
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.connection import PipelineConnectors
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
from mrtarget.modules.Association import ScoringProcess
from mrtarget.modules.DataDrivenRelation import DataDrivenRelationProcess
from mrtarget.modules.ECO import EcoProcess
from mrtarget.modules.EFO import EfoProcess
from mrtarget.modules.Ensembl import EnsemblProcess
from mrtarget.modules.GeneData import GeneManager
from mrtarget.modules.HPA import HPAProcess
from mrtarget.modules.QC import QCRunner,QCMetrics
from mrtarget.modules.Reactome import ReactomeProcess
from mrtarget.modules.SearchObjects import SearchObjectProcess
from mrtarget.modules.Uniprot import UniprotDownloader
from mrtarget.modules.Metrics import Metrics
from mrtarget.Settings import Config, file_or_resource, update_schema_version

def main():

    #set up logging
    fileConfig(file_or_resource('logging.ini'),  disable_existing_loggers=False)
    logger = logging.getLogger(__name__+".main()")

    parser = argparse.ArgumentParser(description='Open Targets processing pipeline')

    #take the release tag from the command line, but fall back to environment or ini files
    parser.add_argument('release_tag', nargs='?', default=Config.RELEASE_VERSION,
                        help='The prefix to prepend default: %s' % \
                        Config.RELEASE_VERSION)
    
    #handle stage-specific QC
    parser.add_argument("--qc-out", help="TSV file to write/update qc information",
                        action="store")
    parser.add_argument("--qc-in", help="TSV file to read qc information for comparison",
                        action="store")
    parser.add_argument("--qc-only", help="only run the qc and not the stage itself",
                        action="store_true")
    parser.add_argument("--skip-qc", help="do not run the qc for this stage",
                        action="store_true")

    #load supplemental and genetic informtaion from various external resources
    parser.add_argument("--hpa", help="download human protein atlas, process, and store in elasticsearch",
                        action="store_true")
    parser.add_argument("--ens", help="retrieve the latest ensembl gene records, store in elasticsearch",
                        action="store_true")
    parser.add_argument("--unic", help="cache the uniprot human entries in elasticsearch",
                        action="store_true")
    parser.add_argument("--rea", help="download reactome data, process it, and store elasticsearch",
                        action="store_true")

    #use the sources to combine the gene information into a single new index
    parser.add_argument("--gen", help="merge the available gene information, store in elasticsearch",
                        action="store_true")

    #load various ontologies into various indexes
    parser.add_argument("--efo", help="process Experimental Factor Ontology (EFO), store in elasticsearch",
                        action="store_true")
    parser.add_argument("--eco", help="process Evidence and Conclusion Ontology (ECO), store in elasticsearch",
                        action="store_true")

    #this generates a elasticsearch index from a source json file
    parser.add_argument("--val", help="check json file, validate, and store in elasticsearch",
                        action="store_true")
    parser.add_argument("--valreset", help="reset audit table and previously parsed evidencestrings",
                        action="store_true")
    parser.add_argument("--enable-fs", help="write to files instead elasticsearch use it with --output-folder",
                        action="store_true", default=False)
    parser.add_argument("--output-folder", help="write output to a folder. Default is './'",
                        action='store', default='./')
    parser.add_argument("--input-file", help="pass the path to a gzipped file to use as input for the data validation step",
                        action='append', default=[])
    parser.add_argument("--schema-version", help="set the schema version aka 'branch' name. Default is 'master'",
                        action='store', default='master')

    #this is related to generating a combine evidence index from all the inidividual datasource indicies
    parser.add_argument("--evs", help="process and validate the available evidence strings, store in elasticsearch",
                        action="store_true")
    parser.add_argument("--datasource", help="just process data for this datasource. Does not work with all the steps!!",
                        action='append', default=[])

    #this has to be stored as "ass" instead of "as" because "as" is a reserved name when accessing it later e.g. `args.as`
    parser.add_argument("--as", help="compute association scores, store in elasticsearch",
                        action="store_true", dest="assoc")
    parser.add_argument("--targets", help="just process data for this target. Does not work with all the steps!!",
                        action='append', default=[])
                        
    #these are related to generated in a search index
    parser.add_argument("--sea", help="compute search results, store in elasticsearch",
                        action="store_true")
    parser.add_argument("--skip-diseases", help="Skip adding diseases to the search index",
                        action='store_true', default=False)
    parser.add_argument("--skip-targets", help="Skip adding targets to the search index",
                        action='store_true', default=False)

    #additional information to add
    parser.add_argument("--ddr", help="compute data driven t2t and d2d relations, store in elasticsearch",
                        action="store_true")

    #generate some high-level summary metrics over the release
    parser.add_argument("--metric", help="generate metrics", 
                        action="store_true")

    #quality control steps
    parser.add_argument("--qc", help="Run quality control scripts",
                        action="store_true")
                       
    #use an external redis rather than spawning one ourselves
    parser.add_argument("--persist-redis", help="the temporary file wont be deleted if True default: False",
                        action='store_true', default=False)
    parser.add_argument("--redis-remote", help="connect to a remote redis",
                        action='store_true', default=False)
    parser.add_argument("--redis-host", help="redis host",
                        action='store', default='')
    parser.add_argument("--redis-port", help="redis port",
                        action='store', default='')
    parser.add_argument("--num-workers", help="num proc workers",
                        action='store', default=4, type=int)
    parser.add_argument("--num-writers", help="num proc writers",
                        action='store', default=2, type=int)

    parser.add_argument("--first-n", help="num first lines to process default to 0 (all)",
                        action='store', default=0, type=int)

    parser.add_argument("--max-queued-events", help="max number of events to put per queue. Default to 1000",
                        action='store', default=1000, type=int)

    #tweak how lookup tables are managed
    parser.add_argument("--lt-reuse", help="reuse the current lookuptable",
                        action='store_true', default=False)
    parser.add_argument("--lt-namespace", help="lookuptable namespace to reuse",
                        action='store', default='')

    #for debugging
    parser.add_argument("--dump", help="dump core data to local gzipped files",
                        action="store_true")
    parser.add_argument("--dry-run", help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
                        action='store_true', default=False)
    parser.add_argument("--profile", help="magically profiling process() per process",
                        action='store_true', default=False)
    parser.add_argument("--log-level", help="set the log level",
                        action='store', default='WARNING')

    parser.add_argument("--log-http", help="log all HTTP protocol requests to the specified file",
                        action='store')

    args = parser.parse_args()

    if not args.release_tag:
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

    if args.log_level:
        try:
            root_logger = logging.getLogger()
            root_logger.setLevel(logging.getLevelName(args.log_level))
            logger.setLevel(logging.getLevelName(args.log_level))
            logger.info('main log level set to: '+ str(args.log_level))
            root_logger.info('root log level set to: '+ str(args.log_level))
        except Exception, e:
            root_logger.exception(e)
            return 1

    if args.log_http:
        logger.info("Will log all HTTP requests to %s" % args.log_http)
        requests_log = logging.getLogger("urllib3")
        requests_log.setLevel(logging.DEBUG)
        requests_log.propagate = False  # Change to True to log to main log as well
        #ensure the directory for the log file exists
        logdir = os.path.dirname(os.path.abspath(args.log_http))
        if not os.path.isdir(logdir):
            logger.debug("log directory %s does not exist, creating", logdir)
            os.makedirs(logdir)
        requests_log.addHandler(logging.FileHandler(args.log_http))


    connected = connectors.init_services_connections(redispersist=args.persist_redis)

    logger.debug('Attempting to establish connection to the backend... %s',
                 str(connected))

    logger.info('setting release version %s' % Config.RELEASE_VERSION)

    #create a single query object for future use
    esquery = ESQuery(connectors.es)

    #create something to accumulate qc metrics into over various steps
    qc_metrics = QCMetrics()

    with Loader(connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = args.dry_run) as loader:

        # get the schema version and change all needed resources
        update_schema_version(Config,args.schema_version)
        logger.info('setting schema version string to %s', args.schema_version)



        if args.rea:
            process = ReactomeProcess(loader)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.ens:
            process = EnsemblProcess(loader)
            if not args.qc_only:
                process.process(Config.ENSEMBL_FILENAME)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.unic:
            process = UniprotDownloader(loader, dry_run=args.dry_run)
            if not args.qc_only:
                process.cache_human_entries()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))            
        if args.hpa:
            process = HPAProcess(loader,connectors.r_server)
            if not args.qc_only:
                process.process_all(dry_run=args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))     

        if args.gen:
            process = GeneManager(loader,connectors.r_server)
            if not args.qc_only:
                process.merge_all(dry_run=args.dry_run)

            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))     
 
        if args.efo:
            process = EfoProcess(loader)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.eco:
            process = EcoProcess(loader)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))

        input_files = []
        if args.val:
            if args.input_file:
                input_files = list(itertools.chain.from_iterable([el.split(",") for el in args.input_file]))
            else:
                logger.info('reading the evidences sources URLs from evidence_sources.txt')
                with open(file_or_resource('evidences_sources.txt')) as f:
                    input_files = [x.rstrip() for x in f.readlines()]

            num_workers = Config.WORKERS_NUMBER
            num_writers = min(1, max(16, Config.WORKERS_NUMBER))
            process_evidences_pipeline(filenames=input_files,
                                       first_n=args.first_n,
                                       es_client=connectors.es,
                                       redis_client=connectors.r_server,
                                       dry_run=args.dry_run,
                                       enable_output_to_es=(not args.enable_fs),
                                       output_folder=args.output_folder,
                                       num_workers=num_workers,
                                       num_writers=num_writers,
                                       max_queued_events=args.max_queued_events)


            #TODO qc

        if args.assoc:
            process = ScoringProcess(loader, connectors.r_server)
            if not args.qc_only:
                process.process_all(targets = targets, dry_run=args.dry_run)
            if not args.skip_qc:
                #qc_metrics.update(process.qc(esquery))
                pass
            
        if args.ddr:
            process = DataDrivenRelationProcess(connectors.es, connectors.r_server)
            if not args.qc_only:
                process.process_all(dry_run = args.dry_run)
            #TODO qc

        if args.sea:
            process = SearchObjectProcess(loader, connectors.r_server)
            if not args.qc_only:
                process.process_all(skip_targets=args.skip_targets, skip_diseases=args.skip_diseases)
            #TODO qc

        if args.metric:
            process = Metrics(connectors.es).generate_metrics()

        if args.qc:
            QCRunner(connectors.es).run_associationQC()

    if args.qc_in:
        #handle reading in previous qc from filename provided, and adding comparitive metrics
        qc_metrics.compare_with(args.qc_in)

    if args.qc_out:
        #handle writing out to a tsv file
        qc_metrics.write_out(args.qc_out)

    logger.debug('close connectors')
    connectors.close()

    logger.info('`'+" ".join(sys.argv)+'` - finished')
    return 0


if __name__ == '__main__':
    sys.exit(main())
