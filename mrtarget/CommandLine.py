from __future__ import print_function, absolute_import
import logging
import logging.config
import argparse
import sys
import os
import os.path
import itertools

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
from mrtarget.Settings import Config, file_or_resource

import mrtarget.cfg

def main():
    #parse config file, environment, and command line arguments
    mrtarget.cfg.setup_parser()
    args = mrtarget.cfg.get_args()

    #set up logging
    logger = None
    if args.log_config:
        if os.path.isfile(args.log_config) and os.access(args.log_config, os.R_OK):
            #read a log configuration file
            logging.config.fileConfig(args.log_config,  disable_existing_loggers=False)
            logger = logging.getLogger(__name__+".main()")
        else:
            #unable to read the logging config file, abort
            logging.basicConfig()
            logger = logging.getLogger(__name__+".main()")
            logger.error("unable to read file {}".format(args.log_config))
            return 1
    else:
        #no logging config specified, fall back to default
        logging.basicConfig()
        logger = logging.getLogger(__name__+".main()")


    if not args.release_tag:
        logger.error('A [release-tag] has to be specified.')
        print('A [release-tag] has to be specified.', file=sys.stderr)
        return 1
    else:
        Config.RELEASE_VERSION = args.release_tag
        logger.info('setting release version %s' % Config.RELEASE_VERSION)

    enable_profiling(args.profile)

    connectors = PipelineConnectors()
    connected = connectors.init_services_connections(es_hosts=args.elasticseach_nodes,
        redispersist=args.redis_remote)

    if not connected:
        logger.error("Unable to connect to services")
        return 1


    #create a single query object for future use
    esquery = ESQuery(connectors.es)

    #create something to accumulate qc metrics into over various steps
    qc_metrics = QCMetrics()

    with Loader(connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = args.dry_run) as loader:

        if args.rea:
            process = ReactomeProcess(loader, 
                args.reactome_pathway_data, args.reactome_pathway_relation)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.ens:
            process = EnsemblProcess(loader)
            if not args.qc_only:
                process.process(args.ensembl_filename)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.unic:
            process = UniprotDownloader(loader, dry_run=args.dry_run)
            if not args.qc_only:
                process.cache_human_entries(args.uniprot_uri)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.hpa:
            process = HPAProcess(loader,connectors.r_server, 
                args.tissue_translation_map, args.tissue_curation_map,
                args.hpa_normal_tissue, args.hpa_rna_level, 
                args.hpa_rna_value, args.hpa_rna_zscore)
            if not args.qc_only:
                process.process_all(dry_run=args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))     

        if args.gen:
            process = GeneManager(loader, connectors.r_server,
                args.gene_data_plugin_places, args.gene_data_plugin_names
)
            if not args.qc_only:
                process.merge_all(dry_run=args.dry_run)

            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))     
 
        if args.efo:
            process = EfoProcess(loader, args.ontology_efo, args.ontology_hpo, 
                args.ontology_mp, args.disease_phenotype)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))
        if args.eco:
            process = EcoProcess(loader, args.ontology_eco, args.ontology_so)
            if not args.qc_only:
                process.process_all()
            if not args.skip_qc:
                qc_metrics.update(process.qc(esquery))

        if args.val:
            num_workers = Config.WORKERS_NUMBER
            num_writers = max(1, min(16, Config.WORKERS_NUMBER))
            
            es_output = True
            es_output_folder = None
            if "elasticsearch_folder" in vars(args) and args.elasticsearch_folder is not None:
                es_output = False
                es_output_folder = args.elasticsearch_folder

            process_evidences_pipeline(filenames=args.input_file,
                first_n=args.val_first_n,
                es_client=connectors.es,
                redis_client=connectors.r_server,
                dry_run=args.dry_run,
                enable_output_to_es=es_output,
                output_folder=es_output_folder,
                num_workers=num_workers,
                num_writers=num_writers,
                max_queued_events=args.max_queued_events,
                eco_scores_uri=args.eco_scores,
                schema_uri = args.schema,
                es_hosts=args.elasticseach_nodes)


            #TODO qc

        if args.assoc:
            process = ScoringProcess(loader, connectors.r_server)
            if not args.qc_only:
                process.process_all(targets = args.targets, 
                    dry_run=args.dry_run)
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
                process.process_all(
                    args.chembl_target, 
                    args.chembl_mechanism, 
                    args.chembl_component, 
                    args.chembl_protein, 
                    args.chembl_molecule_set_uri_pattern,
                    dry_run = args.dry_run,
                    skip_targets=args.skip_targets, 
                    skip_diseases=args.skip_diseases)
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
