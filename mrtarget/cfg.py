import os

import configargparse
from Settings import Config


"""
This will create a singleton argument parser that is appropriately configured
with the various command line, environment, and ini/yaml file options.

Note that backwards compatibility of arguments is not guaranteed. To ensure
legacy arguments are interpreted, use get_args()
"""

def setup_parser():
    p = configargparse.get_argument_parser(config_file_parser_class=configargparse.YAMLConfigFileParser)
    p.description = 'Open Targets processing pipeline'

    # argument to read config file
    p.add('-c', '--config', is_config_file=True,
        env_var="CONFIG", help='path to config file (YAML)')

    # logging
    p.add("--log-level", help="set the log level",
        env_var="LOG_LEVEL", action='store', default='INFO')
    p.add("--log-config", help="logging configuration file",
        env_var="LOG_CONFIG", action='store', default='mrtarget/resources/logging.ini')
    #TODO remove this as it can be captured inside a custom log config instead
    p.add("--log-http", help="log HTTP(S) requests in this file",
        env_var="LOG_HTTP", action='store')

    # take the release tag from the command line, but fall back to environment or ini files
    p.add('release_tag', nargs='?')

    # handle stage-specific QC
    p.add("--qc-out", help="TSV file to write/update qc information")
    p.add("--qc-in", help="TSV file to read qc information for comparison")
    p.add("--qc-only", help="only run the qc and not the stage itself",
        action="store_true")
    p.add("--skip-qc", help="do not run the qc for this stage",
        action="store_true")

    # load supplemental and genetic informtaion from various external resources
    p.add("--hpa", help="download human protein atlas, process, and store in elasticsearch",
        action="store_true")
    p.add("--ens", help="retrieve the latest ensembl gene records, store in elasticsearch",
        action="store_true")
    p.add("--unic", help="cache the uniprot human entries in elasticsearch",
        action="store_true")

    p.add("--rea", help="download reactome data, process it, and store elasticsearch",
        action="store_true")

    # use the sources to combine the gene information into a single new index
    p.add("--gen", help="merge the available gene information, store in elasticsearch",
        action="store_true")

    # load various ontologies into various indexes
    p.add("--mp", help="process Mammalian Phenotype (MP), store the resulting json objects in elasticsearch",
        action="store_true")
    p.add("--efo", help="process Experimental Factor Ontology (EFO), store in elasticsearch",
        action="store_true")
    p.add("--eco", help="process Evidence and Conclusion Ontology (ECO), store in elasticsearch",
        action="store_true")

    # this generates a elasticsearch index from a source json file
    p.add("--val", help="check json file, validate, and store in elasticsearch",
        action="store_true")
    p.add("--input-file", help="pass the path to a gzipped file to use as input for the data validation step",
        action='append')
    p.add("--schema-version", help="set the schema version aka 'branch' name. Default is 'master'",
        env_var="SCHEMA_VERSION", default='master')

    # this is related to generating a combine evidence index from all the inidividual datasource indicies
    p.add("--evs", help="process and validate the available evidence strings, store in elasticsearch",
        action="store_true")
    p.add("--datasource", help="just process data for this datasource. Does not work with all the steps!!",
        action='append')

    # this has to be stored as "assoc" instead of "as" because "as" is a reserved name when accessing it later e.g. `args.as`
    p.add("--as", help="compute association scores, store in elasticsearch",
        action="store_true", dest="assoc")
    p.add("--targets", help="just process data for this target. Does not work with all the steps!!",
        action='append')

    # these are related to generated in a search index
    p.add("--sea", help="compute search results, store in elasticsearch",
        action="store_true")
    p.add("--skip-diseases", help="Skip adding diseases to the search index",
        action='store_true', default=False)
    p.add("--skip-targets", help="Skip adding targets to the search index",
        action='store_true', default=False)

    # additional information to add
    p.add("--ddr", help="compute data driven t2t and d2d relations, store in elasticsearch",
        action="store_true")

    # generate some high-level summary metrics over the release
    #TODO cleanup and possibly delete eventually
    p.add("--metric", help="generate metrics", action="store_true")
    p.add("--metric-file", help="generate metrics", 
        env_var="METRIC_FILE", default='release_metrics.txt')

    # quality control steps
    #TODO cleanup and possibly delete eventually
    p.add("--qc", help="Run quality control scripts",
        action="store_true")

    # use an external redis rather than spawning one ourselves
    p.add("--redis-remote", help="connect to a remote redis, instead of starting an embedded one",
        action='store_true', default=False,
        env_var='CTTV_REDIS_REMOTE')  # TODO use a different env variable
    p.add("--redis-host", help="redis host",
        action='store', default='localhost',
        env_var='REDIS_HOST')
    p.add("--redis-port", help="redis port",
        action='store', default='35000',
        env_var='REDIS_PORT')

    # elasticsearch
    p.add("--elasticseach-nodes", help="elasticsearch host(s)",
        action='append', default=['localhost:9200'],
        env_var='ELASTICSEARCH_NODES')

    # for debugging
    p.add("--dump", help="dump core data to local gzipped files",
        action="store_true")
    p.add("--dry-run", help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
        action='store_true', default=False)
    p.add("--profile", help="magically profiling process() per process",
        action='store_true', default=False)

    # process handling
    #note this is the number of workers for each parallel operation
    #if there are multiple parallel operations happening at once, then 
    #this could be many more than that
    p.add("--num-workers", help="num worker processess for a parallel operation",
        env_var="NUM_WORKERS", action='store', default=4, type=int)
    p.add("--max-queued-events", help="max number of events to put per queue",
        env_var="MAX_QUEUED_EVENTS", action='store', default=10000, type=int)


    #reactome
    p.add("--reactome-pathway-data", help="location of reactome pathway file",
        env_var="REACTOME_PATHWAY_DATA", action='store')
    p.add("--reactome-pathway-relation", help="location of reactome pathway relationships file",
        env_var="REACTOME_PATHWAY_RELACTION", action='store')

    #gene plugins are configured in each plugin
    #helps separate the plugins from the rest of the pipeline
    #and makes it easier to manage custom plugins

    return p

def get_args():
    p = configargparse.get_argument_parser()
    #dont use parse_args because that will error
    #if there are extra arguments e.g. for plugins
    args = p.parse_known_args()

    #output all configuration values, useful for debugging
    p.print_values()

    # check legacy environment variables for backwards compatibility
    # note these will not be documented via --help !

#        if not args.redis_host and not args.redis_port and 'CTTV_REDIS_SERVER' in os.environ:
#            args.redis_host, args.redis_port = os.environ['CTTV_REDIS_REMOTE'].split(":")

    if args.redis_remote:
        Config.REDISLITE_REMOTE = args.redis_remote

    if args.redis_host:
        Config.REDISLITE_DB_HOST = args.redis_host

    if args.redis_port:
        Config.REDISLITE_DB_PORT = args.redis_port


    return args
