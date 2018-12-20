import uuid
from collections import defaultdict
import os
import re
import json
import ConfigParser
import pkg_resources as res
from envparse import env, ConfigurationError
import mrtarget
import multiprocessing as mp
import logging

logger = logging.getLogger(__name__)


def build_uniprot_query(l):
    return '+or+'.join(l)


def ini_from_file_or_resource(*filenames):
    '''load the ini files using file_or_resource an
    return the configuration object or None
    '''
    f = [file_or_resource(fname) for fname in filenames if fname]
    cfg = ConfigParser.ConfigParser()
    if cfg.read(f):
        # read() returns list of successfully parsed filenames
        return cfg
    else:
        # the function return none in case no file was found
        return None


def file_or_resource(fname=None):
    '''get filename and check if in getcwd then get from
    the package resources folder
    '''
    filename = os.path.expanduser(fname)

    resource_package = mrtarget.__name__
    resource_path = '/'.join(('resources', filename))

    if filename is not None:
        abs_filename = os.path.join(os.path.abspath(os.getcwd()), filename) \
                       if not os.path.isabs(filename) else filename

        return abs_filename if os.path.isfile(abs_filename) \
            else res.resource_filename(resource_package, resource_path)


def file_to_list(filename):
    '''read the whole file and returns a list of lines'''
    with open(filename) as f:
        return f.read().splitlines()


# loading all ini files into the same configuration
ini = ini_from_file_or_resource('db.ini', 'uris.ini',
                                'es_custom_idxs.ini')


def read_option(option, cast=None, ini=ini, section='dev',
                **kwargs):
    '''helper method to read value from environmental variable and ini files, in
    that order. Relies on envparse and accepts its parameters.
    The goal is to have ENV var > ini files > defaults

    Lists and dict in the ini file are parsed as JSON strings.
    '''
    # if passing 'default' as parameter, we don't want envparse to return
    # succesfully without first check if there is anything in the ini file
    try:
        default_value = kwargs.pop('default')
    except KeyError:
        default_value = None

    try:
        # reading the environment variable with envparse
        return env(option, cast=cast, **kwargs)
    except ConfigurationError:
        if not ini:
            return default_value

        try:
            # TODO: go through all sections available
            if cast is bool:
                return ini.getboolean(section, option)
            elif cast is int:
                return ini.getint(section, option)
            elif cast is float:
                return ini.getint(section, option)
            elif cast is dict or cast is list:
                # if you want list and dict variables in the ini file,
                # this function will accept json formatted lists.
                return json.loads(ini.get(section, option))
            else:
                return ini.get(section, option)

        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            return default_value

def update_schema_version(config, schema_version_string):
    config.EVIDENCEVALIDATION_SCHEMA = schema_version_string
    for el in config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS:
        config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS[el] = \
            config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS[el].replace('master',
                                                                    schema_version_string)


class Config():
    ONTOLOGY_CONFIG = ConfigParser.ConfigParser()
    # TODO: an ontology section in the main db.ini file should suffice
    ONTOLOGY_CONFIG.read(file_or_resource('ontology_config.ini'))

    RELEASE_VERSION = read_option('CTTV_DATA_VERSION', default='')

    # [elasticsearch]

    # each node in the cluster has to be specified to the client, unless we use
    # Sniffing, but we'd prefer not to do that. The problem arises when you
    # allow nodes with SSL or not. A simple solution is to force full URLs to be
    # specified, protocol and port included and passed as a list.

    # The client accepts host lists such as these:
    # es = Elasticsearch(
    # [
    #     'http://user:secret@localhost:9200/',
    #     'https://user:secret@other_host:443/production'
    # ],
    # verify_certs=True
    # )

    ELASTICSEARCH_NODES = read_option('ELASTICSEARCH_NODES', cast=list,
                                      default=[])

    ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME = 'invalid-evidence-data'
    ELASTICSEARCH_VALIDATED_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_DATA_INDEX_NAME = 'evidence-data'
    ELASTICSEARCH_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_EFO_LABEL_INDEX_NAME = 'efo-data'
    ELASTICSEARCH_EFO_LABEL_DOC_NAME = 'efo'
    ELASTICSEARCH_ECO_INDEX_NAME = 'eco-data'
    ELASTICSEARCH_ECO_DOC_NAME = 'eco'
    ELASTICSEARCH_GENE_NAME_INDEX_NAME = 'gene-data'
    ELASTICSEARCH_GENE_NAME_DOC_NAME = 'genedata'
    ELASTICSEARCH_EXPRESSION_INDEX_NAME = 'expression-data'
    ELASTICSEARCH_EXPRESSION_DOC_NAME = 'expression'
    ELASTICSEARCH_REACTOME_INDEX_NAME = 'reactome-data'
    ELASTICSEARCH_REACTOME_REACTION_DOC_NAME = 'reactome-reaction'
    ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME = 'association-data'
    ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME = 'association'
    ELASTICSEARCH_DATA_SEARCH_INDEX_NAME = 'search-data'
    ELASTICSEARCH_DATA_SEARCH_DOC_NAME = 'search-object'
    ELASTICSEARCH_ENSEMBL_INDEX_NAME = 'ensembl-data'
    ELASTICSEARCH_ENSEMBL_DOC_NAME = 'ensembl-gene'
    ELASTICSEARCH_UNIPROT_INDEX_NAME = 'uniprot-data'
    ELASTICSEARCH_UNIPROT_DOC_NAME = 'uniprot-gene'
    ELASTICSEARCH_RELATION_INDEX_NAME = 'relation-data'
    ELASTICSEARCH_RELATION_DOC_NAME = 'relation'

    # to generate this file you have to call
    # https://www.uniprot.org/uniprot/?query=reviewed%3Ayes%2BAND%2Borganism%3A9606&compress=yes&format=xml
    UNIPROT_URI = "https://storage.googleapis.com/ot-releases/18.12/annotations/uniprot-20181130.xml.gz"

    GENE_DATA_PLUGIN_PLACES = [ 'mrtarget' + os.path.sep + 'plugins' + os.path.sep + 'gene' ]
    GENE_DATA_PLUGIN_ORDER = ['HGNC', 'Orthologs', 'Ensembl', 'Uniprot', 'ChEMBL', 'MousePhenotypes', 'Hallmarks',
                              'CancerBiomarkers', 'ChemicalProbes', 'Tractability']


    BIOMARKER_FILENAME = "https://storage.googleapis.com/ot-releases/18.12/annotations/cgi_biomarkers_per_variant.tsv"
    CHEMICALPROBES_FILENAME1 = "https://storage.googleapis.com/ot-releases/18.12/annotations/chemicalprobes_portalprobes_20181130.tsv"
    CHEMICALPROBES_FILENAME2 = "https://storage.googleapis.com/ot-releases/18.12/annotations/chemicalprobes_probeminer_20181015.tsv"
    HALLMARK_FILENAME = "https://storage.googleapis.com/ot-releases/18.12/annotations/cosmic-v87_hallmark_export.tsv"
    TRACTABILITY_FILENAME = "https://storage.googleapis.com/ot-releases/18.12/annotations/tractability_buckets-30-11-2018.tsv"

    TISSUE_TRANSLATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/expression_hierarchy/master/process/map_with_efos.json'
    TISSUE_CURATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/expression_hierarchy/master/process/curation.tsv'

    ENSEMBL_FILENAME = "https://storage.googleapis.com/ot-releases/18.12/annotations/homo_sapiens_core_94_38_genes.json.gz"

    HPA_NORMAL_TISSUE_URL = ini.get('full_dataset', 'hpa_normal')
    HPA_CANCER_URL = ini.get('full_dataset', 'hpa_cancer')
    HPA_SUBCELLULAR_LOCATION_URL = ini.get('full_dataset', 'hpa_subcellular')
    HPA_RNA_LEVEL_URL = ini.get('full_dataset', 'hpa_rna_level')
    HPA_RNA_VALUE_URL = ini.get('full_dataset', 'hpa_rna_value')
    HPA_RNA_ZSCORE_URL = ini.get('full_dataset', 'hpa_rna_zscore')

    EVIDENCEVALIDATION_SCHEMA = 'master'
    EVIDENCEVALIDATION_DATATYPES = ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']

    EVIDENCEVALIDATION_VALIDATOR_SCHEMAS = {
        'genetic_association': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/genetics.json',
        'rna_expression':      'https://raw.githubusercontent.com/opentargets/json_schema/master/src/expression.json',
        'genetic_literature':  'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'affected_pathway':    'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'somatic_mutation':    'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'known_drug':          'https://raw.githubusercontent.com/opentargets/json_schema/master/src/drug.json',
        'literature':          'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_mining.json',
        'animal_model':        'https://raw.githubusercontent.com/opentargets/json_schema/master/src/animal_models.json'
    }


    # setup the number of workers to use for data processing. if None defaults
    # to the number of CPUs available
    WORKERS_NUMBER = read_option('WORKERS_NUMBER',cast=int,
                                 default=mp.cpu_count())

    CHEMBL_TARGET_BY_UNIPROT_ID = ini.get('full_dataset', 'chembl_target')
    CHEMBL_MECHANISM = ini.get('full_dataset', 'chembl_mechanism')
    CHEMBL_MOLECULE_SET = '''https://www.ebi.ac.uk/chembl/api/data/molecule/set/{}.json'''
    CHEMBL_PROTEIN_CLASS = ini.get('full_dataset', 'chembl_protein')
    CHEMBL_TARGET_COMPONENT = ini.get('full_dataset', 'chembl_component')

    DATASOURCE_TO_DATATYPE_MAPPING = {}
    DATASOURCE_TO_DATATYPE_MAPPING['expression_atlas'] = 'rna_expression'
    DATASOURCE_TO_DATATYPE_MAPPING['phenodigm'] = 'animal_model'
    DATASOURCE_TO_DATATYPE_MAPPING['chembl'] = 'known_drug'
    DATASOURCE_TO_DATATYPE_MAPPING['europepmc'] = 'literature'
    DATASOURCE_TO_DATATYPE_MAPPING['reactome'] = 'affected_pathway'
    DATASOURCE_TO_DATATYPE_MAPPING['slapenrich'] = 'affected_pathway'
    DATASOURCE_TO_DATATYPE_MAPPING['intogen'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['eva_somatic'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot_somatic'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['cancer_gene_census'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['eva'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['gwas_catalog'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['postgap'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot_literature'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['gene2phenotype'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['phewas_catalog'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['genomics_england'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['progeny'] = 'affected_pathway'
    DATASOURCE_TO_DATATYPE_MAPPING['sysbio'] = 'affected_pathway'

    EXCLUDED_BIOTYPES_BY_DATASOURCE = {
        'expression_atlas': ["IG_C_pseudogene",
                             "IG_J_pseudogene",
                             "IG_pseudogene",
                             "IG_V_pseudogene",
                             "polymorphic_pseudogene",
                             "processed_pseudogene",
                             "pseudogene",
                             "rRNA",
                             "rRNA_pseudogene",
                             "snoRNA",
                             "snRNA",
                             "transcribed_processed_pseudogene",
                             "transcribed_unitary_pseudogene",
                             "transcribed_unprocessed_pseudogene",
                             "TR_J_pseudogene",
                             "TR_V_pseudogene",
                             "unitary_pseudogene",
                             "unprocessed_pseudogene"]
    }


    # setup the weights for evidence strings score
    SCORING_WEIGHTS = defaultdict(lambda: 1)
    SCORING_WEIGHTS['phenodigm'] = 0.2
    SCORING_WEIGHTS['expression_atlas'] = 0.2
    SCORING_WEIGHTS['europepmc'] = 0.2
    SCORING_WEIGHTS['slapenrich'] = 0.5
    SCORING_WEIGHTS['progeny'] = 0.5
    SCORING_WEIGHTS['sysbio'] = 0.5
    # SCORING_WEIGHTS['gwas_catalog'] = 1.5

    # setup a minimum score value for an evidence string to be accepted.
    SCORING_MIN_VALUE_FILTER = defaultdict(lambda: 0)
    SCORING_MIN_VALUE_FILTER['phenodigm'] = 0.4

    IS_DIRECT_DO_NOT_PROPAGATE = ['expression_atlas']

    LT_REUSE = False
    LT_NAMESPACE = ""
    REDISLITE_REMOTE = read_option('CTTV_REDIS_REMOTE',
                                   cast=bool, default=False)
    REDISLITE_DB_HOST, REDISLITE_DB_PORT = \
        read_option('CTTV_REDIS_SERVER', cast=str, default='127.0.0.1:35000').split(':')

    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]

    # This config file is like this and no prefixes or version will be
    # appended
    #
    # [indexes]
    # gene-data=new-gene-data-index-name
    # ...
    #
    # if no index field or config file is found then a default
    # composed index name will be returned
    ES_CUSTOM_IDXS = read_option('CTTV_ES_CUSTOM_IDXS',
                                 default=False, cast=bool)
    ES_CUSTOM_IDXS_INI = ini if ES_CUSTOM_IDXS else None
    