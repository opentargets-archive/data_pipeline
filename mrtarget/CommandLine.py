from __future__ import print_function, absolute_import
import logging
import logging.config
import argparse
import sys
import os
import os.path
import itertools

from mrtarget.modules.Evidences import process_evidences_pipeline
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.ElasticsearchQuery import ESQuery
from mrtarget.common.connection import RedisManager, new_es_client, new_redis_client
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
from mrtarget.modules.Association import ScoringProcess
from mrtarget.modules.DataDrivenRelation import DataDrivenRelationProcess
from mrtarget.modules.ECO import EcoProcess
from mrtarget.modules.EFO import EfoProcess
from mrtarget.modules.Ensembl import EnsemblProcess
from mrtarget.modules.GeneData import GeneManager
from mrtarget.modules.HPA import HPAProcess
from mrtarget.modules.QC import QCMetrics
from mrtarget.modules.Reactome import ReactomeProcess
from mrtarget.modules.SearchObjects import SearchObjectProcess
from mrtarget.modules.Uniprot import UniprotDownloader
from mrtarget.Settings import Config, file_or_resource

import mrtarget.cfg

def main():
    #parse config file, environment, and command line arguments
    mrtarget.cfg.setup_ops_parser()
    args = mrtarget.cfg.get_ops_args()

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

    logger.info('`'+" ".join(sys.argv)+'` - starting')

    if not args.release_tag:
        logger.error('A [release-tag] has to be specified.')
        print('A [release-tag] has to be specified.', file=sys.stderr)
        return 1
    else:
        Config.RELEASE_VERSION = args.release_tag
        logger.info('setting release version %s' % Config.RELEASE_VERSION)



    
    
    with RedisManager(args.redis_remote,args.redis_host, args.redis_port):

        es = new_es_client(args.elasticseach_nodes)
        redis = new_redis_client(args.redis_host, args.redis_port)

        #create a single query object for future use
        esquery = ESQuery(es)

        #read the data configuration
        data_config = mrtarget.cfg.get_data_config(args.data_config)

        #create something to accumulate qc metrics into over various steps
        qc_metrics = QCMetrics()

        with Loader(es,
                    chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                    dry_run = args.dry_run) as loader:

            if args.rea:
                process = ReactomeProcess(loader, 
                    data_config.reactome_pathway_data, data_config.reactome_pathway_relation)
                if not args.qc_only:
                    process.process_all(args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))
            if args.ens:
                process = EnsemblProcess(loader)
                if not args.qc_only:
                    process.process(data_config.ensembl_filename, args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))
            if args.unic:
                process = UniprotDownloader(loader)
                if not args.qc_only:
                    process.process(data_config.uniprot_uri, args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))
            if args.hpa:
                process = HPAProcess(loader,redis, args.elasticseach_nodes,
                    data_config.tissue_translation_map, data_config.tissue_curation_map,
                    data_config.hpa_normal_tissue, data_config.hpa_rna_level, 
                    data_config.hpa_rna_value, data_config.hpa_rna_zscore)
                if not args.qc_only:
                    process.process_all(args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))     

            if args.gen:
                process = GeneManager(loader, redis,
                    args.gen_plugin_places, data_config.gene_data_plugin_names,
                    )
                if not args.qc_only:
                    process.merge_all(data_config, dry_run=args.dry_run)

                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))     
    
            if args.efo:
                process = EfoProcess(loader, data_config.ontology_efo, data_config.ontology_hpo, 
                    data_config.ontology_mp, data_config.disease_phenotype)
                if not args.qc_only:
                    process.process_all(args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))
            if args.eco:
                process = EcoProcess(loader, data_config.ontology_eco, data_config.ontology_so)
                if not args.qc_only:
                    process.process_all(args.dry_run)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))

            if args.val:
                process_evidences_pipeline(data_config.input_file, args.val_first_n,
                    es, redis, args.dry_run, 
                    args.val_workers_validator, args.val_queue_validator,
                    args.val_workers_writer, args.val_queue_validator_writer,
                    data_config.eco_scores, data_config.schema,
                    data_config.excluded_biotypes, data_config.datasources_to_datatypes)

                #TODO qc

            if args.assoc:
                process = ScoringProcess(args.redis_host, args.redis_port,
                    args.elasticseach_nodes, args.as_workers_writer, args.as_queue_write)
                if not args.qc_only:
                    process.process_all(data_config.scoring_weights, 
                        data_config.is_direct_do_not_propagate,
                        data_config.datasources_to_datatypes,
                        args.dry_run,
                        args.as_workers_production,
                        args.as_workers_score,
                        args.as_queue_production_score)
                if not args.skip_qc:
                    qc_metrics.update(process.qc(esquery))
                    pass
                
            if args.ddr:
                process = DataDrivenRelationProcess(es)
                if not args.qc_only:
                    process.process_all(args.dry_run,
                        args.ddr_workers_production,
                        args.ddr_workers_score,
                        args.ddr_workers_write,
                        args.ddr_queue_production_score,
                        args.ddr_queue_score_result,
                        args.ddr_queue_write)
                #TODO qc

            if args.sea:
                process = SearchObjectProcess(loader, redis)
                if not args.qc_only:
                    process.process_all(
                        data_config.chembl_target, 
                        data_config.chembl_mechanism, 
                        data_config.chembl_component, 
                        data_config.chembl_protein, 
                        data_config.chembl_molecule_set_uri_pattern,
                        args.dry_run)
                #TODO qc

    if args.qc_in:
        #handle reading in previous qc from filename provided, and adding comparitive metrics
        qc_metrics.compare_with(args.qc_in)

    if args.qc_out:
        #handle writing out to a tsv file
        qc_metrics.write_out(args.qc_out)

    logger.info('`'+" ".join(sys.argv)+'` - finished')
    return 0


if __name__ == '__main__':
    sys.exit(main())
