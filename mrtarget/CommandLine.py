from __future__ import print_function
import logging
import argparse
import sys
import os
import itertools

from mrtarget.common.Redis import enable_profiling
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.connection import PipelineConnectors
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
from mrtarget.modules.Association import ScoringProcess
from mrtarget.modules.DataDrivenRelation import DataDrivenRelationProcess
from mrtarget.modules.ECO import EcoProcess
from mrtarget.modules.EFO import EfoProcess
from mrtarget.modules.HPO import HpoProcess
from mrtarget.modules.MP import MpProcess
from mrtarget.modules.Ensembl import EnsemblProcess
from mrtarget.modules.EvidenceString import EvidenceStringProcess
from mrtarget.modules.EvidenceValidation import EvidenceValidationFileChecker
from mrtarget.modules.GeneData import GeneManager
from mrtarget.modules.HPA import HPAProcess
from mrtarget.modules.QC import QCRunner,QCMetrics
from mrtarget.modules.Reactome import ReactomeProcess
from mrtarget.modules.SearchObjects import SearchObjectProcess
from mrtarget.modules.Uniprot import UniprotDownloader
from mrtarget.modules.Metrics import Metrics
from mrtarget.Settings import Config, file_or_resource, update_schema_version

import mrtarget.cfg
import configargparse

def main():

    #parse config file, environment, and command line arguments
    args = mrtarget.cfg.Configuration().args

    #set up logging
    if args.log_config:
        logging.config.fileConfig(args.log_config,  disable_existing_loggers=False)

    logger = logging.getLogger(__name__+".main()")


    if not args.release_tag:
        logger.error('A [release-tag] has to be specified.')
        print('A [release-tag] has to be specified.', file=sys.stderr)
        return 1
    else:
        Config.RELEASE_VERSION = args.release_tag

    targets = args.targets

    enable_profiling(args.profile)

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
                process.process()
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

        if args.mp:
            process = MpProcess(loader)
            if not args.qc_only:
                process.process_all()
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
        if args.hpo:
            process = HpoProcess(loader)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))



        if args.val:
            if args.input_file:
                input_files = list(itertools.chain.from_iterable([el.split(",") for el in args.input_file]))
            else:
                #default behaviour: use all the data sources listed in the evidences_sources.txt file
                logger.info('reading the evidences sources URLs from evidence_sources.txt')
                with open(file_or_resource('evidences_sources.txt')) as f:
                    input_files = [x.rstrip() for x in f.readlines()]

            process = EvidenceValidationFileChecker(connectors.es, connectors.r_server, 
                dry_run=args.dry_run)
            if not args.qc_only:
                process.check_all(input_files=input_files, increment=False)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery, input_files))


        if args.valreset:
            EvidenceValidationFileChecker(connectors.es, connectors.r_server).reset()

        if args.evs:
            process = EvidenceStringProcess(connectors.es, connectors.r_server)
            if not args.qc_only:
                process.process_all(datasources = args.datasource, dry_run=args.dry_run)
            if not args.skip_qc:
                #qc_metrics.update(process.qc(esquery))
                pass

        if args.ass:
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
