import os
import yaml
import configargparse
import addict
import mrtarget.common.connection
from opentargets_urlzsource import URLZSource

def setup_ops_parser():
    p = configargparse.get_argument_parser(config_file_parser_class=configargparse.YAMLConfigFileParser)
    p.description = 'Open Targets processing pipeline'

    # read config file for other argument values
    p.add('--ops-config', is_config_file=True,
        env_var="OPS_CONFIG", help='path to ops config file')

    # configuration file with data related settings
    p.add('--data-config', help='path to data config file (YAML)',
        env_var="DATA_CONFIG", action='store', default="mrtarget.data.yml")

    # logging
    p.add("--log-config", help="logging configuration file",
        env_var="LOG_CONFIG", action='store', default='mrtarget/resources/logging.ini')

    # take the release tag from the command line, but fall back to environment or ini files
    p.add('--release-tag', help="identifier for data storage for this release",
        env_var="ES_PREFIX", action='store', default="master")

    # handle stage-specific QC
    p.add("--qc-out", help="TSV file to write/update qc information")
    p.add("--qc-in", help="TSV file to read qc information for comparison")
    p.add("--qc-only", help="only run the qc and not the stage itself",
        action="store_true", default=False)
    p.add("--skip-qc", help="do not run the qc for this stage",
        action="store_true", default=False)

    # use an external redis rather than spawning one ourselves
    p.add("--redis-remote", help="connect to a remote redis, instead of starting an embedded one",
        action='store_true', default=False,
        env_var='REDIS_REMOTE')
    p.add("--redis-host", help="redis host",
        action='store', default='localhost',
        env_var='REDIS_HOST')
    p.add("--redis-port", help="redis port",
        action='store', default='6379',
        env_var='REDIS_PORT')

    # elasticsearch
    p.add("--elasticseach-nodes", help="elasticsearch host(s)",
        action='append', default=['localhost:9200'],
        env_var='ELASTICSEARCH_NODES')
    p.add("--elasticsearch-folder", help="write to files instead of a live elasticsearch server",
        action='store') #this only applies to --val at the moment

    # process handling
    #note this is the number of workers for each parallel operation
    #if there are multiple parallel operations happening at once, and
    #there usually are, then this could be many more than that

    p.add("--val-workers-validator", help="# of procs for validation workers",
        env_var="VAL_WORKERS_VALIDATOR", action='store', default=4, type=int)
    p.add("--val-workers-writer", help="# of procs for validation writers",
        env_var="VAL_WORKERS_WRITER", action='store', default=4, type=int)
    p.add("--val-queue-validator-writer", help="size of validation writer to worker queue",
        env_var="VAL_QUEUE_VALIDATOR_WRITER", action='store', default=1000, type=int)

    p.add("--as-workers-production", help="# of procs for assocation pair producers",
        env_var="AS_WORKERS_PRODUCTION", action='store', default=4, type=int)
    p.add("--as-workers-score", help="# of procs for assocation pair scoring",
        env_var="AS_WORKERS_SCORE", action='store', default=4, type=int)
    p.add("--as-queue-production-score", help="size of assocation producer to scorer queue",
        env_var="AS_QUEUE_PRODUCTION_SCORE", action='store', default=1000, type=int)

    p.add("--ddr-workers-production", help="# of procs for relation pair producers",
        env_var="DDR_WORKERS_PRODUCTION", action='store', default=4, type=int)
    p.add("--ddr-workers-score", help="# of procs for relation pair scoring",
        env_var="DDR_WORKERS_SCORE", action='store', default=4, type=int)
    p.add("--ddr-workers-write", help="# of threads for relation writing",
        env_var="DDR_WORKERS_WRITE", action='store', default=8, type=int)
    p.add("--ddr-queue-production-score", help="size of relation producer to scorer queue",
        env_var="DDR_QUEUE_PRODUCTION_SCORE", action='store', default=1000, type=int)
    p.add("--ddr-queue-score-result", help="size of relation scorer result queue",
        env_var="DDR_QUEUE_SCORE_RESULT", action='store', default=1000, type=int)
    p.add("--ddr-queue-write", help="size of relation writer queue (in chunks)",
        env_var="DDR_QUEUE_WRITE", action='store', default=8, type=int)

    # for debugging
    p.add("--dry-run", help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
        action='store_true', default=False)

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
    #paths to plugins to ensure discoverability
    p.add("--gen-plugin-places", help="paths to check for gene plugins",
        action='append', default=["mrtarget/plugins/gene"])


    # load various ontologies into various indexes
    p.add("--efo", help="process Experimental Factor Ontology (EFO), store in elasticsearch",
        action="store_true")
    p.add("--eco", help="process Evidence and Conclusion Ontology (ECO), store in elasticsearch",
        action="store_true")

    # this generates a elasticsearch index from source json evidence file(s)
    p.add("--val", help="check json file, validate, and store in elasticsearch",
        action="store_true")
    p.add("--val-first-n", help="read only the first n lines from each input file",
        env_var="VAL_FIRST_N", type=int)

    # this has to be stored as "assoc" instead of "as" because "as" is a reserved name when accessing it later e.g. `args.as`
    p.add("--as", help="compute association scores, store in elasticsearch",
        action="store_true", dest="assoc")

    # these are related to generated in a search index
    p.add("--sea", help="compute search results, store in elasticsearch",
        action="store_true")

    # additional information to add
    p.add("--ddr", help="compute data driven t2t and d2d relations, store in elasticsearch",
        action="store_true")

    return p

def get_ops_args():
    p = configargparse.get_argument_parser()
    #dont use parse_args because that will error
    #if there are extra arguments e.g. for plugins
    args = p.parse_known_args()[0]

    #output all configuration values, useful for debugging
    p.print_values()

    #WARNING
    #this is a horrible hack and should be removed 
    #once lookup and queues are handled better
    mrtarget.common.connection.default_host = args.redis_host
    mrtarget.common.connection.default_port = args.redis_port

    return args


def get_data_config(data_url):  
    with URLZSource(data_url).open() as r_file:
        #note us safe loading as described at https://pyyaml.org/wiki/PyYAMLDocumentation
        #TL;DR - only dicts and lists and primitives
        data_config = yaml.safe_load(r_file)

        #replace hyphens with underscores in variable 
        #this is because we want to use addict to 
        #access config as config.foo_bar instead of config["foo-bar"]
        data_config_underscores = {}
        for key in data_config:
            key_underscore = key.replace("-","_")
            data_config_underscores[key_underscore] = data_config[key]

        return addict.Dict(data_config_underscores)