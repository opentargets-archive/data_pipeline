import uuid
from collections import defaultdict
import os
import re
import json
import ConfigParser
import pkg_resources as res
from envparse import env, ConfigurationError
import mrtarget
import petl
import multiprocessing as mp
import logging

from mrtarget.common import URLZSource

logger = logging.getLogger(__name__)


def build_uniprot_query(l):
    return '+or+'.join(l)


def build_ensembl_sql(l):
    return """SELECT stable_id FROM gene where stable_id IN ('{0}')"""\
        .format("', '".join(l))


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
        config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS[el].replace('master',
                                                              schema_version_string)


class Config():
    EV_LIMIT = read_option('CTTV_EV_LIMIT', cast=bool, default=False)
    MINIMAL = read_option('CTTV_MINIMAL', default=False, cast=bool)
    MINIMAL_ENSEMBL = file_to_list(file_or_resource('minimal_ensembl.txt'))

    INI_SECTION = 'minimal_dataset' if MINIMAL else 'full_dataset'

    HAS_PROXY = ini is not None and ini.has_section('proxy')
    if HAS_PROXY:
        PROXY = ini.get('proxy', 'protocol') + "://" + ini.get('proxy', 'username') + \
                ":" + ini.get('proxy', 'password') + "@" + \
                ini.get('proxy', 'host') + ":" + \
                ini.get('proxy', 'port')

        PROXY_PROTOCOL = ini.get('proxy', 'protocol')
        PROXY_USERNAME = ini.get('proxy', 'username')
        PROXY_PASSWORD = ini.get('proxy', 'password')
        PROXY_HOST = ini.get('proxy', 'host')
        PROXY_PORT = int(ini.get('proxy', 'port'))

    TEMP_DIR = os.path.sep + 'tmp'

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
    ELASTICSEARCH_NODES_PUB = read_option('ELASTICSEARCH_NODES_PUB', cast=list,
                                      default=[])

    ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME = 'validated-data'
    ELASTICSEARCH_VALIDATED_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME = 'submission-audit'
    ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME = 'submission'
    ELASTICSEARCH_DATA_INDEX_NAME = 'evidence-data'
    ELASTICSEARCH_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_EFO_LABEL_INDEX_NAME = 'efo-data'
    ELASTICSEARCH_EFO_LABEL_DOC_NAME = 'efo'
    ELASTICSEARCH_HPO_LABEL_INDEX_NAME = 'hpo-data'
    ELASTICSEARCH_HPO_LABEL_DOC_NAME = 'hpo'
    ELASTICSEARCH_MP_LABEL_INDEX_NAME = 'mp-data'
    ELASTICSEARCH_MP_LABEL_DOC_NAME = 'mp'
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
    ELASTICSEARCH_HALLMARK_INDEX_NAME = 'hallmark-data'
    ELASTICSEARCH_HALLMARK_DOC_NAME = 'hallmark-gene'
    ELASTICSEARCH_RELATION_INDEX_NAME = 'relation-data'
    ELASTICSEARCH_RELATION_DOC_NAME = 'relation'
    ELASTICSEARCH_PUBLICATION_INDEX_NAME = '!publication-data'
    ELASTICSEARCH_PUBLICATION_DOC_NAME = 'publication'
    ELASTICSEARCH_PUBLICATION_DOC_ANALYSIS_SPACY_NAME = 'publication-analysis-spacy'
    ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME = '!lit-entities-test'
    ELASTICSEARCH_LITERATURE_ENTITY_DOC_NAME = 'litentity'
    DEBUG = True
    PROFILE = False
    ERROR_IDS_FILE = 'errors.txt'


    HGNC_COMPLETE_SET = 'http://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json'
    HGNC_ORTHOLOGS = 'http://ftp.ebi.ac.uk/pub/databases/genenames/hcop/human_all_hcop_sixteen_column.txt.gz'
    HGNC_ORTHOLOGS_SPECIES = {
        '9606':'human',
        '9598':'chimpanzee',
        '9544':'macaque',
        '10090':'mouse',
        '10116':'rat',
        '9615':'dog',
        '9823':'pig',
        '8364':'frog',
        '7955':'zebrafish',
        '7227':'fly',
        '6239':'worm',
        '4932':'yeast'
    }

    OMIM_TO_EFO_MAP_URL = 'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/xref_mappings/omim_to_efo.txt'
    ZOOMA_TO_EFO_MAP_URL = 'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/zooma/cttv_indications_3.txt'

    # TISSUE_TRANSLATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/mappings/master/expression_uberon_mapping.csv'
    # TISSUE_TRANSLATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/mappings/dev/expression_uberon_mapping.csv'
    TISSUE_TRANSLATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/expression_hierarchy/master/process/map_with_efos.json'
    TISSUE_CURATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/expression_hierarchy/master/process/curation.tsv'

    HPA_NORMAL_TISSUE_URL = ini.get(INI_SECTION, 'hpa_normal')
    HPA_CANCER_URL = ini.get(INI_SECTION, 'hpa_cancer')
    HPA_SUBCELLULAR_LOCATION_URL = ini.get(INI_SECTION, 'hpa_subcellular')
    HPA_RNA_LEVEL_URL = ini.get(INI_SECTION, 'hpa_rna_level')
    HPA_RNA_VALUE_URL = ini.get(INI_SECTION, 'hpa_rna_value')
    HPA_RNA_ZSCORE_URL = ini.get(INI_SECTION, 'hpa_rna_zscore')
    #HPA_RNA_URL = 'http://v16.proteinatlas.org/download/rna_tissue.csv.zip'
    REACTOME_ENSEMBL_MAPPINGS = ini.get(INI_SECTION, 'ensembl_reactome')
    # REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome_All_Levels.txt'
    REACTOME_PATHWAY_DATA = ini.get(INI_SECTION, 'reactome_pathways')
    REACTOME_PATHWAY_RELATION = ini.get(INI_SECTION, 'reactome_pathways_rel')
    REACTOME_SBML_REST_URI = 'http://www.reactome.org/ReactomeRESTfulAPI/RESTfulWS/sbmlExporter/{0}'
    EVIDENCEVALIDATION_SCHEMA = 'master'
    EVIDENCEVALIDATION_DATATYPES = ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']

    EVIDENCEVALIDATION_VALIDATOR_SCHEMAS = {
        'genetic_association': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/genetics.json',
        'rna_expression': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/expression.json',
        'genetic_literature': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'affected_pathway': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'somatic_mutation': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_curated.json',
        'known_drug': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/drug.json',
        'literature_mining': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_mining.json',
        'literature': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/literature_mining.json',
        'animal_model': 'https://raw.githubusercontent.com/opentargets/json_schema/master/src/animal_models.json'
    }

    EVIDENCEVALIDATION_MAX_NB_ERRORS_REPORTED = 1000
    EVIDENCEVALIDATION_NB_TOP_DISEASES = 20
    EVIDENCEVALIDATION_NB_TOP_TARGETS = 20
    EVIDENCEVALIDATION_PERCENT_SCALE = 20
    EVIDENCEVALIDATION_FORCE_VALIDATION = True
    # Current genome Assembly
    EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY = 'GRCh38'
    # Change this if you don't want to send e-mails
    EVIDENCEVALIDATION_SEND_EMAIL = False
    EVIDENCEVALIDATION_SENDER_ACCOUNT = 'no_reply@targetvalidation.org'
    MAILGUN_DOMAIN = "https://api.mailgun.net/v3/mg.targetvalidation.org"
    MAILGUN_MESSAGES = MAILGUN_DOMAIN+'/messages'
    MAILGUN_API_KEY = "key-b7986f9a29fe234733b0af3b1206b146"
    EVIDENCEVALIDATION_BCC_ACCOUNT = [ 'andreap@targetvalidation.org', ]
    # Change this if you want to change the list of recipients
    EVIDENCEVALIDATION_PROVIDER_EMAILS = defaultdict(lambda: "other")
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv001"] = [ 'gautier.x.koscielny@gsk.com', 'andreap@targetvalidation.org', 'ckong@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv006"] = [ 'fabregat@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv007"] = [ 'zs1@sanger.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv008"] = [ 'mpaulam@ebi.ac.uk', 'patricia@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv009"] = [ 'olgavrou@ebi.ac.uk', 'tburdett@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv010"] = [ 'mkeays@ebi.ac.uk', 'irenep@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv011"] = [ 'eddturner@ebi.ac.uk', 'bpalka@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv012"] = [ 'tsmith@ebi.ac.uk', 'garys@ebi.ac.uk', 'cyenyxe@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv025"] = [ 'kafkas@ebi.ac.uk', 'ftalo@ebi.ac.uk' ]
    EVIDENCEVALIDATION_FILENAME_REGEX = re.compile(r"""
    (?P<datasource>[a-zA-Z0-9_]+)(\-
    (?P<d1>\d{2})\-
    (?P<d2>\d{2})\-
    (?P<d3>\d{4}))?
    (?P<suffix>\.json\.gz|\.json|\.json\.zip)$""", re.VERBOSE)

    # setup the number of workers to use for data processing. if None defaults
    # to the number of CPUs available
    WORKERS_NUMBER = read_option('WORKERS_NUMBER',cast=int,
                                 default=mp.cpu_count())

    # mouse models
    MOUSEMODELS_PHENODIGM_SOLR = 'solrclouddev.sanger.ac.uk'
    # TODO remove refs to user directories
    MOUSEMODELS_CACHE_DIRECTORY = '/Users/otvisitor/.phenodigmcache'

    # put the path to the file where you want to get the list of HP terms to be included in our ontology
    PHENOTYPE_SLIM_INPUT_URLS = [
        'https://raw.githubusercontent.com/opentargets/platform_semantic/master/resources/eva/hpo_mappings.txt'
    ]
    #  put the path to the file where you want to write the SLIM file (turtle format)
    PHENOTYPE_SLIM_OUTPUT_FILE = TEMP_DIR + os.path.sep + 'opentargets_disease_phenotype_slim.ttl'

    CHEMBL_TARGET_BY_UNIPROT_ID = ini.get(INI_SECTION, 'chembl_target')
    CHEMBL_MECHANISM = ini.get(INI_SECTION, 'chembl_mechanism')
    CHEMBL_MOLECULE_SET = '''https://www.ebi.ac.uk/chembl/api/data/molecule/set/{}.json'''
    CHEMBL_PROTEIN_CLASS = ini.get(INI_SECTION, 'chembl_protein')
    CHEMBL_TARGET_COMPONENT = ini.get(INI_SECTION, 'chembl_component')

    # Mouse/Human Orthology with Phenotype Annotations (tab-delimited)
    GENOTYPE_PHENOTYPE_MGI_REPORT_ORTHOLOGY = "http://www.informatics.jax.org/downloads/reports/HMD_HumanPhenotype.rpt"
    # All Genotypes and Mammalian Phenotype Annotations (tab-delimited)
    GENOTYPE_PHENOTYPE_MGI_REPORT_PHENOTYPES = "http://www.informatics.jax.org/downloads/reports/MGI_PhenoGenoMP.rpt"
    # data dump location if you want to merge the data without running all the steps again
    GENOTYPE_PHENOTYPE_OUTPUT = TEMP_DIR + os.path.sep + 'genotype_phenotype.json'

    DATASOURCE_EVIDENCE_SCORE_WEIGHT=dict(
        # gwas_catalog=2.5
        )
    DATASOURCE_EVIDENCE_SCORE_AUTO_EXTEND_RANGE=dict(
                                                     # phenodigm=dict(min=0.4, max= 1),
                                                    )

    DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED = dict(cttv006 = 'reactome',
                                                         cttv008 = 'chembl',
                                                         cttv009 = 'gwas_catalog',
                                                         cttv011 = 'uniprot',
                                                         cttv012 = 'eva',
                                                         cttv018 = 'gwas_ibd',
                                                         cttv007 = 'cancer_gene_census',
                                                         cttv025 = 'europepmc',
                                                         cttv005 = 'rare2common',
                                                         cttv010 = 'expression_atlas',
                                                         cttv026 = 'phewas_catalog',
                                                         cttv027 = '23andme'

                                                )

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
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot_literature'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['gene2phenotype'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['phewas_catalog'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['genomics_england'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['twentythreeandme'] = 'genetic_association'


    # use specific index for a datasource
    DATASOURCE_TO_INDEX_KEY_MAPPING = defaultdict(lambda: "generic")
    # DATASOURCE_TO_INDEX_KEY_MAPPING['europepmc'] = 'europepmc'
    # DATASOURCE_TO_INDEX_KEY_MAPPING['phenodigm'] = DATASOURCE_TO_DATATYPE_MAPPING['phenodigm']
    # DATASOURCE_TO_INDEX_KEY_MAPPING['expression_atlas'] = DATASOURCE_TO_DATATYPE_MAPPING['expression_atlas']

    # setup the weights for evidence strings score
    SCORING_WEIGHTS = defaultdict(lambda: 1)
    SCORING_WEIGHTS['phenodigm'] = 0.2
    SCORING_WEIGHTS['expression_atlas'] = 0.5
    SCORING_WEIGHTS['europepmc'] = 0.2
    SCORING_WEIGHTS['slapenrich'] = 0.5
    # SCORING_WEIGHTS['gwas_catalog'] = 1.5

    # setup a minimum score value for an evidence string to be accepted.
    SCORING_MIN_VALUE_FILTER = defaultdict(lambda: 0)
    SCORING_MIN_VALUE_FILTER['phenodigm'] = 0.4


    ENSEMBL_RELEASE_VERSION = 90
    ENSEMBL_CHUNK_SIZE = 100

    REDISLITE_REMOTE = read_option('CTTV_REDIS_REMOTE',
                                   cast=bool, default=False)
    REDISLITE_DB_HOST, REDISLITE_DB_PORT = \
        read_option('CTTV_REDIS_SERVER', cast=str, default='127.0.0.1:35000').split(':')

    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]


    # dump file names
    DUMP_FILE_FOLDER = read_option('CTTV_DUMP_FOLDER', default=TEMP_DIR)
    DUMP_FILE_EVIDENCE = RELEASE_VERSION+'_evidence_data.json.gz'
    DUMP_FILE_ASSOCIATION = RELEASE_VERSION + '_association_data.json.gz'
    DUMP_PAGE_SIZE = 10000
    DUMP_BATCH_SIZE = 10

    DUMP_REMOTE_API = read_option('DUMP_REMOTE_API_URL', default='http://beta.opentargets.io')
    DUMP_REMOTE_API_PORT = read_option('DUMP_REMOTE_API_PORT', default='80')
    DUMP_REMOTE_API_SECRET = read_option('DUMP_REMOTE_API_SECRET')
    DUMP_REMOTE_API_APPNAME = read_option('DUMP_REMOTE_API_APPNAME')

    # Literature Pipeline -- Pubmed/Medline FTP server
    PUBMED_TEMP_DIR = os.path.join(TEMP_DIR, 'medline')
    PUBMED_FTP_SERVER = 'ftp.ncbi.nlm.nih.gov'
    PUBMED_XML_LOCN = os.path.join(PUBMED_TEMP_DIR, 'baseline')
    PUBMED_XML_UPDATE_LOCN = os.path.join(PUBMED_TEMP_DIR, 'update')

    PUBMED_HTTP_MIRROR = 'https://storage.googleapis.com/pubmed-medline'
    BIOLEXICON_GENE_XML_LOCN = 'geneProt (1).xml'
    BIOLEXICON_DISEASE_XML_LOCN = 'umlsDisease.xml'
    GENE_LEXICON_JSON_LOCN = 'gene_lexicon.json'
    DISEASE_LEXICON_JSON_LOCN = 'disease_lexicon.json'

    # GE Pipeline

    GE_EVIDENCE_STRING = TEMP_DIR + os.path.sep + 'genomics_england_evidence_string.json'
    GE_LINKOUT_URL = 'https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/GeneReview'
    GE_ZOOMA_DISEASE_MAPPING = TEMP_DIR + os.path.sep + 'zooma_disease_mapping.csv'
    GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT = TEMP_DIR + os.path.sep + 'zooma_disease_mapping_low_confidence.csv'

    # for developers
    DRY_RUN_OUTPUT = read_option('DRY_RUN_OUTPUT_ENABLE',
                                 cast=bool, default=False)
    DRY_RUN_OUTPUT_DELETE = read_option('DRY_RUN_OUTPUT_DELETE',
                                        cast=bool, default=False)
    DRY_RUN_OUTPUT_COUNT = read_option('DRY_RUN_OUTPUT_COUNT',
                                       cast=int, default=10000)

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
