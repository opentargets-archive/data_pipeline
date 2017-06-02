import argparse
import logging
import sys
import itertools as it

from mrtarget.common import Actions
from mrtarget.common.ElasticsearchLoader import Loader
from mrtarget.common.connection import PipelineConnectors
from mrtarget.ElasticsearchConfig import ElasticSearchConfiguration
from mrtarget.modules.Association import AssociationActions, ScoringProcess
from mrtarget.modules.DataDrivenRelation import DataDrivenRelationActions, DataDrivenRelationProcess
from mrtarget.modules.Dump import DumpActions, DumpGenerator
from mrtarget.modules.ECO import EcoActions, EcoProcess
from mrtarget.modules.EFO import EfoActions, EfoProcess
from mrtarget.modules.Ensembl import  EnsemblActions, EnsemblProcess
from mrtarget.modules.EvidenceString import EvidenceStringActions, EvidenceStringProcess
from mrtarget.modules.EvidenceValidation import ValidationActions, EvidenceValidationFileChecker
from mrtarget.modules.GeneData import GeneActions, GeneManager
from mrtarget.modules.HPA import HPAActions, HPAProcess
from mrtarget.modules.IntOGen import IntOGenActions, IntOGen
from mrtarget.modules.Literature import LiteratureActions, MedlineRetriever
from mrtarget.modules.LiteratureNLP import LiteratureNLPProcess, LiteratureNLPActions
from mrtarget.modules.MouseModels import MouseModelsActions, Phenodigm
from mrtarget.modules.Ontology import OntologyActions, PhenotypeSlim
from mrtarget.modules.QC import QCActions, QCRunner
from mrtarget.modules.Reactome import ReactomeActions, ReactomeProcess
from mrtarget.modules.SearchObjects import SearchObjectActions, SearchObjectProcess
from mrtarget.modules.Uniprot import UniProtActions, UniprotDownloader
from mrtarget.Settings import Config, file_or_resource


logging.config.fileConfig(file_or_resource('logging.ini'),
                          disable_existing_loggers=False)


def load_nlp_corpora():
    '''load here all the corpora needed by nlp steps'''
    import nltk
    nltk.download([ 'punkt', 'averaged_perceptron_tagger']) #'brown' corpora might be needed


def main():
    logger = logging.getLogger(__name__)

    parser = argparse.ArgumentParser(description='Open Targets processing pipeline')
    parser.add_argument("--all", dest='all', help="run the full pipeline (at your own risk)",
                        action="append_const", const = Actions.ALL)
    parser.add_argument("--hpa", dest='hpa', help="download human protein atlas data, process it and upload it to elasticsearch",
                        action="append_const", const = HPAActions.ALL)
    parser.add_argument("--rea", dest='rea', help="download reactome data, process it and upload it to elasticsearch",
                        action="append_const", const = ReactomeActions.ALL)
    parser.add_argument("--unic", dest='uni', help="cache the live version of uniprot human entries in postgresql",
                        action="append_const", const = UniProtActions.CACHE)
    parser.add_argument("--gen", dest='gen', help="merge the available gene information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = GeneActions.ALL)
    parser.add_argument("--efo", dest='efo', help="process the efo information, store the resulting json objects in postgres and upload them in elasticsearch",
                        action="append_const", const = EfoActions.ALL)
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
    parser.add_argument("--persist-redis", dest='redispersist', help="use a fresh redislite db",
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
                        action="append_const", const = IntOGenActions.ALL)
    parser.add_argument("--ontos", dest='onto', help="create phenotype slim",
                        action="append_const", const = OntologyActions.PHENOTYPESLIM)
    parser.add_argument("--onto", dest='onto', help="all ontology processing steps (phenotype slim, disease phenotypes)",
                        action="append_const", const = OntologyActions.ALL)
    parser.add_argument("--lit", dest='lit', help="fetch and process literature data",
                        action="append_const", const=LiteratureActions.ALL)
    parser.add_argument("--lit-fetch", dest='lit', help="fetch literature data",
                        action="append_const", const=LiteratureActions.FETCH)
    parser.add_argument("--lit-process", dest='lit', help="process literature data",
                        action="append_const", const=LiteratureNLPActions.PROCESS)
    parser.add_argument("--lit-update", dest='lit', help="update literature data",
                        action="append_const", const=LiteratureActions.UPDATE)
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
    parser.add_argument("--increment", dest='increment',
                        help="add new evidence from a data source but does not delete existing evidence. Works only for the validation step",
                        action='store_true', default=False)
    parser.add_argument("--local-file", dest='local_file',
                        help="pass the path to a local gzipped file to use as input for the data validation step",
                        action='append', default=[])
    parser.add_argument("--remote-file", dest='remote_file',
                        help="pass the url to a remote gzipped file to use as input for the data validation step",
                        action='append', default=[])
    parser.add_argument("--log-level", dest='loglevel',
                        help="set the log level",
                        action='store', default='INFO')
    parser.add_argument("--do-nothing", dest='do_nothing',
                        help="to be used just for test",
                        action='store_true', default=False)
    parser.add_argument("--inject-literature", dest='inject_literature',
                        help="inject literature data in the evidence-string, default set to False",
                        action='store_true', default=False)

    args = parser.parse_args()

    targets = args.targets

    connectors = PipelineConnectors()

    if args.loglevel:
        try:
            logger = logging.getLogger()
            logger.setLevel(logging.getLevelName(args.loglevel))
        except Exception, e:
            logger.exception(e)

    if args.do_nothing:
        sys.exit("Exiting. I pity the fool that tells me to 'do nothing'")

    logger.info('Attempting to establish connection to the backend...')
    db_connected = connectors.init_services_connections(redispersist=args.redispersist)



    logger.info('setting release version %s' % Config.RELEASE_VERSION)

    if args.inject_literature or args.lit:
        load_nlp_corpora()

    with Loader(connectors.es,
                chunk_size=ElasticSearchConfiguration.bulk_load_chunk,
                dry_run = args.dry_run) as loader:
        run_full_pipeline = False
        if args.all and (Actions.ALL in args.all):
            run_full_pipeline = True
        if args.hpa or run_full_pipeline:
            do_all = (HPAActions.ALL in args.hpa) or run_full_pipeline
            if (HPAActions.PROCESS in args.hpa) or do_all:
                HPAProcess(loader).process_all()
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
        if args.lit or run_full_pipeline:
            if LiteratureActions.FETCH in args.lit :
                MedlineRetriever(connectors.es, loader, args.dry_run, connectors.r_server).fetch(args.local_file)
            if LiteratureActions.UPDATE in args.lit:
                MedlineRetriever(connectors.es, loader, args.dry_run, connectors.r_server).fetch(update=True)
            if LiteratureNLPActions.PROCESS in args.lit :
                LiteratureNLPProcess(connectors.es, loader, connectors.r_server).process()
        if args.intogen or run_full_pipeline:
            do_all = (IntOGenActions.ALL in args.intogen) or run_full_pipeline
            if (IntOGenActions.GENERATE_EVIDENCE in args.intogen) or do_all:
                IntOGen(connectors.es, connectors.sparql).process_intogen()
        if args.onto or run_full_pipeline:
            do_all = (OntologyActions.ALL in args.onto) or run_full_pipeline
            if (OntologyActions.PHENOTYPESLIM in args.onto) or do_all:
                PhenotypeSlim().create_phenotype_slim(args.local_file)

        l_files = list(it.chain.from_iterable([el.split(",") for el in args.local_file]))
        r_files = list(it.chain.from_iterable([el.split(",") for el in args.remote_file]))

        if args.val or run_full_pipeline:
            do_all = (ValidationActions.ALL in args.val) or run_full_pipeline
            if (ValidationActions.CHECKFILES in args.val) or do_all:
                EvidenceValidationFileChecker(connectors.es,
                                              connectors.r_server,
                                              dry_run=args.dry_run).check_all(input_files=l_files + r_files,
                                                                              increment=args.increment)
        if args.valreset:
            EvidenceValidationFileChecker(connectors.es, connectors.r_server).reset()
        if args.evs or run_full_pipeline:
            do_all = (EvidenceStringActions.ALL in args.evs) or run_full_pipeline
            if (EvidenceStringActions.PROCESS in args.evs) or do_all:
                targets = EvidenceStringProcess(connectors.es, connectors.r_server).process_all(datasources = args.datasource,
                                                                          dry_run=args.dry_run,inject_literature=args.inject_literature)
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


if __name__ == '__main__':
    main()
