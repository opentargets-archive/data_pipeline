import os
import yaml
import configargparse
import addict
from mrtarget.common import URLZSource

def setup_ops_parser():
    p = configargparse.get_argument_parser(config_file_parser_class=configargparse.YAMLConfigFileParser)
    p.description = 'Open Targets processing pipeline'

    # read config file for other argument values
    p.add('--ops-config', is_config_file=True,
        env_var="OPS_CONFIG", help='path to ops config file')

    # configuration file with data related settings
    p.add('--data-config',
        env_var="DATA_CONFIG", help='path to data config file (YAML)')

    # logging
    p.add("--log-config", help="logging configuration file",
        env_var="LOG_CONFIG", action='store', default='mrtarget/resources/logging.ini')

    # take the release tag from the command line, but fall back to environment or ini files
    p.add('--release-tag', help="identifier for data storage for this release",
        env_var="ES_PREFIX", action='store')

    # handle stage-specific QC
    p.add("--qc-out", help="TSV file to write/update qc information")
    p.add("--qc-in", help="TSV file to read qc information for comparison")
    p.add("--qc-only", help="only run the qc and not the stage itself",
        action="store_true")
    p.add("--skip-qc", help="do not run the qc for this stage",
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
    p.add("--elasticsearch-folder", help="write to files instead of a live elasticsearch server",
        action='store') #this only applies to --val at the moment

    # process handling
    #note this is the number of workers for each parallel operation
    #if there are multiple parallel operations happening at once, then 
    #this could be many more than that
    p.add("--num-workers", help="num worker processess for a parallel operation",
        env_var="NUM_WORKERS", action='store', default=4, type=int)
    p.add("--max-queued-events", help="max number of events to put per queue",
        env_var="MAX_QUEUED_EVENTS", action='store', default=10000, type=int)

    # for debugging
    p.add("--dump", help="dump core data to local gzipped files",
        action="store_true")
    p.add("--dry-run", help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
        action='store_true', default=False)
    p.add("--profile", help="magically profiling process() per process",
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

    # load various ontologies into various indexes
    p.add("--efo", help="process Experimental Factor Ontology (EFO), store in elasticsearch",
        action="store_true")
    p.add("--eco", help="process Evidence and Conclusion Ontology (ECO), store in elasticsearch",
        action="store_true")

    # this generates a elasticsearch index from source json evidence file(s)
    p.add("--val", help="check json file, validate, and store in elasticsearch",
        action="store_true")
    p.add("--val-first-n", help="read only the first n lines from each input file",
        env_var="VAL_FIRST_N")

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

    return p

def get_ops_args():
    p = configargparse.get_argument_parser()
    #dont use parse_args because that will error
    #if there are extra arguments e.g. for plugins
    args = p.parse_known_args()[0]

    #output all configuration values, useful for debugging
    p.print_values()

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