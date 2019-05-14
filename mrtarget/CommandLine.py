from __future__ import print_function, absolute_import
import logging
import logging.config
import argparse
import sys
import os
import os.path
import itertools

from mrtarget.modules.Evidences import process_evidences_pipeline
from mrtarget.common.connection import RedisManager, new_es_client, new_redis_client
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
   
    
    with RedisManager(args.redis_remote,args.redis_host, args.redis_port):

        #read the data configuration
        data_config = mrtarget.cfg.get_config(args.data_config)

        #read the es configuration
        es_config = mrtarget.cfg.get_config(args.es_config)

        es = new_es_client(args.elasticseach_nodes)
        redis = new_redis_client(args.redis_host, args.redis_port)
        #es clients can't be pased around

        #create something to accumulate qc metrics into over various steps
        qc_metrics = QCMetrics()


        if args.rea:
            process = ReactomeProcess(args.elasticseach_nodes, es_config.rea.name, 
                es_config.rea.doc, es_config.rea.mapping, es_config.rea.setting,
                data_config.reactome_pathway_data, data_config.reactome_pathway_relation,
                args.rea_workers_writer, args.rea_queue_write)
            if not args.qc_only:
                process.process_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.rea.name))
        if args.ens:
            process = EnsemblProcess(args.elasticseach_nodes, es_config.ens.name, 
                es_config.ens.doc, es_config.ens.mapping, es_config.ens.setting,
                data_config.ensembl_filename, args.ens_workers_writer, args.ens_queue_write)
            if not args.qc_only:
                process.process(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.ens.name))
        if args.unic:
            process = UniprotDownloader(args.elasticseach_nodes, es_config.uni.name, 
                es_config.uni.doc, es_config.uni.mapping, es_config.uni.setting,
                data_config.uniprot_uri, args.uni_workers_writer, args.uni_queue_write)
            if not args.qc_only:
                process.process(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.uni.name))

        if args.gen:
            process = GeneManager(args.elasticseach_nodes, es_config.gen.name, 
                es_config.gen.doc, es_config.gen.mapping, es_config.gen.setting, 
                redis,
                args.gen_plugin_places, data_config.gene_data_plugin_names,
                data_config, es_config,
                args.gen_workers_writer, args.gen_queue_write )
            if not args.qc_only:
                process.merge_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.gen.name))     

        if args.efo:
            process = EfoProcess(args.elasticseach_nodes, es_config.efo.name, 
                es_config.efo.doc, es_config.efo.mapping, es_config.efo.setting, 
                data_config.ontology_efo, data_config.ontology_hpo, 
                data_config.ontology_mp, data_config.disease_phenotype,
                args.efo_workers_writer, args.efo_queue_write)
            if not args.qc_only:
                process.process_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.efo.name))
        if args.eco:
            process = EcoProcess(args.elasticseach_nodes, es_config.eco.name, 
                es_config.eco.doc, es_config.eco.mapping, es_config.eco.setting,
                data_config.ontology_eco, data_config.ontology_so,
                args.eco_workers_writer, args.eco_queue_write)
            if not args.qc_only:
                process.process_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.eco.name))

        if args.val:
            process_evidences_pipeline(data_config.input_file, args.val_first_n,
                args.elasticseach_nodes, es_config.val_right.name, es_config.val_wrong.name, 
                es_config.val_right.doc, es_config.val_wrong.doc, 
                es_config.val_right.mapping, es_config.val_wrong.mapping, 
                es_config.val_right.setting, es_config.val_wrong.setting, 
                es_config.gen.name, es_config.eco.name, es_config.efo.name,
                redis, args.dry_run, 
                args.val_workers_validator, args.val_queue_validator,
                args.val_workers_writer, args.val_queue_validator_writer,
                data_config.eco_scores, data_config.schema,
                data_config.excluded_biotypes, data_config.datasources_to_datatypes)

            #TODO qc

        if args.hpa:
            process = HPAProcess(args.elasticseach_nodes, es_config.hpa.name, es_config.hpa.doc, 
                    es_config.hpa.mapping, es_config.hpa.setting,
                    redis, 
                    data_config.tissue_translation_map, data_config.tissue_curation_map,
                    data_config.hpa_normal_tissue, data_config.hpa_rna_level, 
                    data_config.hpa_rna_value, data_config.hpa_rna_zscore,
                    args.hpa_workers_writer, args.hpa_queue_write)
            if not args.qc_only:
                process.process_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.hpa.name))     

        if args.assoc:
            process = ScoringProcess(args.elasticseach_nodes, es_config.asc.name, 
                    es_config.asc.doc, es_config.asc.mapping, es_config.asc.setting,
                    es_config.gen.name, es_config.eco.name, es_config.val_right.name, es_config.hpa.name, es_config.efo.name,
                    args.redis_host, args.redis_port,
                    args.as_workers_writer, args.as_workers_production, args.as_workers_score, 
                    args.as_queue_score, args.as_queue_production, args.as_queue_write,
                    data_config.scoring_weights, data_config.is_direct_do_not_propagate,
                    data_config.datasources_to_datatypes)
            if not args.qc_only:
                process.process_all(args.dry_run)
            if not args.skip_qc:
                qc_metrics.update(process.qc(es, es_config.asc.name))
                pass
            
        if args.ddr:
            process = DataDrivenRelationProcess(args.elasticseach_nodes, 
                    es_config.ddr.name, es_config.ddr.doc, 
                    es_config.ddr.mapping, es_config.ddr.setting,
                    es_config.efo.name, es_config.gen.name, es_config.asc.name, 
                    args.ddr_workers_production,
                    args.ddr_workers_score,
                    args.ddr_workers_write,
                    args.ddr_queue_production_score,
                    args.ddr_queue_score_result,
                    args.ddr_queue_write,
                    data_config.ddr["score-threshold"],
                    data_config.ddr["evidence-count"])
            if not args.qc_only:
                process.process_all(args.dry_run)
            #TODO qc

        if args.sea:
            process = SearchObjectProcess(args.elasticseach_nodes, 
                    es_config.sea.name, es_config.sea.doc, 
                    es_config.sea.mapping, es_config.sea.setting, 
                    es_config.gen.name, es_config.efo.name, es_config.val_right.name,
                    es_config.asc.name,  
                    redis,
                    args.sea_workers_writer, 
                    args.sea_queue_write, 
                    data_config.chembl_target, 
                    data_config.chembl_mechanism, 
                    data_config.chembl_component, 
                    data_config.chembl_protein, 
                    data_config.chembl_molecule_set_uri_pattern)
            if not args.qc_only:
                process.process_all(args.dry_run)
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
