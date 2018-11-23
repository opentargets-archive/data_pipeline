import os

import configargparse


"""
Singleton implemntation based on https://www.python.org/download/releases/2.2/descrintro/#__new__
"""
class Configuration(object):



    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it


    """
    One-time intiialization of singleton
    """
    def init(self, *args, **kwds):
        self.args = self._get_args()

    """
    This will create a singleton argument parser that is appropriately configured
    with the various command line, environment, and ini/yaml file options.

    Note that backwards compatibility of arguments is not guaranteed. To ensure 
    legacy arguments are interpreted, use get_args() 
    """
    def _setup_parser(self):
        p = configargparse.get_argument_parser()
        p.description = 'Open Targets processing pipeline'

        #argument to read config file
        p.add('-c', '--config', is_config_file=True, help='path to config file')

        #take the release tag from the command line, but fall back to environment or ini files
        p.add('release_tag', nargs='?')
        
        #handle stage-specific QC
        p.add("--qc-out", help="TSV file to write/update qc information",
                            action="store")
        p.add("--qc-in", help="TSV file to read qc information for comparison",
                            action="store")
        p.add("--qc-only", help="only run the qc and not the stage itself",
                            action="store_true")
        p.add("--skip-qc", help="do not run the qc for this stage",
                            action="store_true")

        #load supplemental and genetic informtaion from various external resources
        p.add("--hpa", help="download human protein atlas, process, and store in elasticsearch",
                            action="store_true")
        p.add("--ens", help="retrieve the latest ensembl gene records, store in elasticsearch",
                            action="store_true")
        p.add("--unic", help="cache the uniprot human entries in elasticsearch",
                            action="store_true")
        p.add("--rea", help="download reactome data, process it, and store elasticsearch",
                            action="store_true")

        #use the sources to combine the gene information into a single new index
        p.add("--gen", help="merge the available gene information, store in elasticsearch",
                            action="store_true")

        #load various ontologies into various indexes
        p.add("--mp", help="process Mammalian Phenotype (MP), store the resulting json objects in elasticsearch",
                            action="store_true")
        p.add("--efo", help="process Experimental Factor Ontology (EFO), store in elasticsearch",
                            action="store_true")
        p.add("--eco", help="process Evidence and Conclusion Ontology (ECO), store in elasticsearch",
                            action="store_true")
        p.add("--hpo", help="process Human Phenotype Ontology (HPO), store in elasticsearch",
                            action="store_true")

        #this generates a elasticsearch index from a source json file
        p.add("--val", help="check json file, validate, and store in elasticsearch",
                            action="store_true")
        p.add("--valreset", help="reset audit table and previously parsed evidencestrings",
                            action="store_true")
        p.add("--input-file", help="pass the path to a gzipped file to use as input for the data validation step",
                            action='append', default=[])
        p.add("--schema-version", help="set the schema version aka 'branch' name. Default is 'master'",
                            action='store', default='master')

        #this is related to generating a combine evidence index from all the inidividual datasource indicies
        p.add("--evs", help="process and validate the available evidence strings, store in elasticsearch",
                            action="store_true")
        p.add("--datasource", help="just process data for this datasource. Does not work with all the steps!!",
                            action='append', default=[])

        #this has to be stored as "ass" instead of "as" because "as" is a reserved name when accessing it later e.g. `args.as`
        p.add("--as", help="compute association scores, store in elasticsearch",
                            action="store_true", dest="ass")                        
        p.add("--targets", help="just process data for this target. Does not work with all the steps!!",
                            action='append', default=[])
                            
        #these are related to generated in a search index
        p.add("--sea", help="compute search results, store in elasticsearch",
                            action="store_true")
        p.add("--skip-diseases", help="Skip adding diseases to the search index",
                            action='store_true', default=False)
        p.add("--skip-targets", help="Skip adding targets to the search index",
                            action='store_true', default=False)

        #additional information to add
        p.add("--ddr", help="compute data driven t2t and d2d relations, store in elasticsearch",
                            action="store_true")

        #generate some high-level summary metrics over the release
        p.add("--metric", help="generate metrics", 
                            action="store_true")
        p.add("--metric-file", help="generate metrics", 
                            action="store", default='release_metrics.txt')

        #quality control steps
        p.add("--qc", help="Run quality control scripts",
                            action="store_true")
                        
        #use an external redis rather than spawning one ourselves
        p.add("--persist-redis", help="the temporary file wont be deleted if True default: False",
                            action='store_true', default=False)
        p.add("--redis-remote", help="connect to a remote redis, instead of starting an embedded one",
                            action='store_true', default=False, 
                            env_var='CTTV_REDIS_REMOTE') #TODO use a different env variable
        p.add("--redis-host", help="redis host",
                            action='store', default='',
                            env_var='REDIS_HOST')
        p.add("--redis-port", help="redis port",
                            action='store', default='',
                            env_var='REDIS_PORT')

        #for debugging
        p.add("--dump", help="dump core data to local gzipped files",
                            action="store_true")
        p.add("--dry-run", help="do not store data in the backend, useful for dev work. Does not work with all the steps!!",
                            action='store_true', default=False)
        p.add("--profile", help="magically profiling process() per process",
                            action='store_true', default=False)

        #logging
        p.add("--log-level", help="set the log level",
                            action='store', default='WARNING')
        p.add("--log-config", help="logging configuration file",
                            action='store', default='logging.ini')
        p.add("--log-http", help="log HTTP(S) requests in this file",
                            action='store')


        return p



    def _get_args(self):
        p = self._setup_parser()
        args = p.parse_args()

        #check legacy environment variables for backwards compatibility
        #note these will not be documented via --help !

        if not args.redis_remote and 'CTTV_REDIS_REMOTE' in os.environ:
            args.redis_remote = os.environ['CTTV_REDIS_REMOTE']

        if not args.redis_host and not args.redis_port and 'CTTV_REDIS_SERVER' in os.environ:
            args.redis_host, args.redis_port  = os.environ['CTTV_REDIS_REMOTE'].split(":")

        return args

