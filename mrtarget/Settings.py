import uuid
from collections import defaultdict
import os
import ConfigParser
import pkg_resources as res
import mrtarget
import petl
import multiprocessing as mp
import tempfile

from mrtarget.common import URLZSource


def build_uniprot_query(l):
    return '+or+'.join(l)


def build_ensembl_sql(l):
    return """SELECT stable_id FROM gene where stable_id IN ('{0}')"""\
        .format("', '".join(l))


def parse_bool(var):
    return True if str(var) in ['True', 'true', '1', 't', 'y', 'yes', 'Yes'] \
        else False


def ini_from_file_or_resource(filename=None):
    '''load the ini file using file_or_resource an
    return the configuration object or None
    '''
    try:
        # trying to load file from somewhere
        f = file_or_resource(filename)
        cfg = ConfigParser.ConfigParser()
        cfg.read(f)
        return cfg
    except Exception:
        # the function return none in case file wasnt found
        return None


def from_env(var, default_val=None, t=str, parse_func=None):
    '''it helps to parse correctly the env vars as `var`

    By default is var to str to default type is `t` and default value
    if `os.getenv` returns None is `default_val`. In case you need
    to parse the string into another type but the default parsing is
    not of your taste (by ex. t=bool) then `parse_func` will be used if
    not None

    returns t(parse_func(var)) else default_val or None
    '''
    env_var = os.getenv(var)
    if env_var:
        env_var = t(parse_func(env_var)) if parse_func else t(env_var)
    else:
        env_var = default_val

    return env_var


def file_or_resource(filename=None):
    '''get filename and check if in getcwd then get from
    the package resources folder
    '''
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


# loading the ES db ini configuration file
iniparser = ini_from_file_or_resource('db.ini')
uris = ini_from_file_or_resource('uris.ini')


class Config():
    MINIMAL = from_env('CTTV_MINIMAL', False, t=bool, parse_func=parse_bool)
    MINIMAL_ENSEMBL = file_to_list(file_or_resource('minimal_ensembl.txt'))

    URIS_SECTION = 'minimal' if MINIMAL else 'default'

    HAS_PROXY = iniparser is not None and iniparser.has_section('proxy')
    if HAS_PROXY:
        PROXY = iniparser.get('proxy', 'protocol') + "://" + iniparser.get('proxy', 'username') + ":" + iniparser.get(
            'proxy', 'password') + "@" + iniparser.get('proxy', 'host') + ":" + iniparser.get('proxy', 'port')
        PROXY_PROTOCOL = iniparser.get('proxy', 'protocol')
        PROXY_USERNAME = iniparser.get('proxy', 'username')
        PROXY_PASSWORD = iniparser.get('proxy', 'password')
        PROXY_HOST = iniparser.get('proxy', 'host')
        PROXY_PORT = int(iniparser.get('proxy', 'port'))

    ONTOLOGY_CONFIG = ConfigParser.ConfigParser()
    ONTOLOGY_CONFIG.read(file_or_resource('ontology_config.ini'))

    RELEASE_VERSION = from_env('CTTV_DATA_VERSION', '17.04')
    ENV = from_env('CTTV_EL_LOADER', 'dev')
    ELASTICSEARCH_URL, ELASTICSEARCH_NODES = None, []
    ELASTICSEARCH_HOST = from_env('ELASTICSEARCH_HOST')
    ELASTICSEARCH_PORT = from_env('ELASTICSEARCH_PORT')

    if ELASTICSEARCH_HOST is None and iniparser is not None:
        try:
            ELASTICSEARCH_HOST = iniparser.get(ENV, 'elurl')
            ELASTICSEARCH_PORT = iniparser.get(ENV, 'elport')
        except (ConfigParser.NoOptionError, ConfigParser.NoSectionError):
            pass
    if ELASTICSEARCH_HOST is not None and ELASTICSEARCH_PORT is not None:
        if ',' in ELASTICSEARCH_HOST:
            ELASTICSEARCH_NODES = ELASTICSEARCH_HOST.split(',')
            ELASTICSEARCH_HOST = ELASTICSEARCH_NODES[0]
        else:
            ELASTICSEARCH_NODES = [ELASTICSEARCH_HOST]
        ELASTICSEARCH_URL = 'http://' + ELASTICSEARCH_HOST
        if ELASTICSEARCH_PORT:
            ELASTICSEARCH_URL = ELASTICSEARCH_URL+':'+ELASTICSEARCH_PORT+'/'

    ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME = 'validated-data'
    ELASTICSEARCH_VALIDATED_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME = 'submission-audit'
    ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME = 'submission'
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
    ELASTICSEARCH_PUBLICATION_INDEX_NAME = '!publication-data'
    ELASTICSEARCH_PUBLICATION_DOC_NAME = 'publication'
    ELASTICSEARCH_PUBLICATION_DOC_ANALYSIS_SPACY_NAME = 'publication-analysis-spacy'
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

    TISSUE_TRANSLATION_MAP_URL = 'https://raw.githubusercontent.com/opentargets/mappings/master/expression_uberon_mapping.csv'
    TISSUE_TRANSLATION_MAP = dict(petl.fromcsv(URLZSource(TISSUE_TRANSLATION_MAP_URL),
                                                delimiter='|').data().tol())
    HPA_NORMAL_TISSUE_URL = uris.get(URIS_SECTION,'hpa_normal')
    HPA_CANCER_URL = uris.get(URIS_SECTION, 'hpa_cancer')
    HPA_SUBCELLULAR_LOCATION_URL = uris.get(URIS_SECTION, 'hpa_subcellular')
    HPA_RNA_URL = uris.get(URIS_SECTION, 'hpa_baseline')
    #HPA_RNA_URL = 'http://v16.proteinatlas.org/download/rna_tissue.csv.zip'
    REACTOME_ENSEMBL_MAPPINGS = uris.get(URIS_SECTION, 'ensembl_reactome')
    # REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome_All_Levels.txt'
    REACTOME_PATHWAY_DATA = uris.get(URIS_SECTION, 'reactome_pathways')
    REACTOME_PATHWAY_RELATION = uris.get(URIS_SECTION, 'reactome_pathways_rel')
    REACTOME_SBML_REST_URI = 'http://www.reactome.org/ReactomeRESTfulAPI/RESTfulWS/sbmlExporter/{0}'
    EVIDENCEVALIDATION_SCHEMA = "1.2.5"
    EVIDENCEVALIDATION_DATATYPES = ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']
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
    EVIDENCEVALIDATION_FILENAME_REGEX = r".*cttv[0-9]{3}.*\-\d{2}\-\d{2}\-\d{4}(\.json\.gz|\.json)$"

    # setup the number of workers to use for data processing.
    # if None defaults to the number of CPUs available
    WORKERS_NUMBER = from_env('CTTV_WORKERS_NUMBER',
                              default_val=mp.cpu_count(), t=int)

    # mouse models
    MOUSEMODELS_PHENODIGM_SOLR = 'solrclouddev.sanger.ac.uk'
    MOUSEMODELS_CACHE_DIRECTORY = '/Users/koscieln/.phenodigmcache'

    # hardcoded folder of json file to be preprocessed to extract
    # HP and MP terms not in EFO but that will be combined in a SLIM
    ONTOLOGY_PREPROCESSING_DATASOURCES = [
        'cttv008-14-03-2016.json.gz',
        ''
    ]

    ONTOLOGY_PREPROCESSING_FTP_ACCOUNTS = ["cttv008", "cttv012"]

    # put the path to the file where you want to write the SLIM file (turtle format)
    ONTOLOGY_SLIM_FILE = '/Users/koscieln/Documents/work/gitlab/remote_reference_data_import/bin_import_nonEFO_terms/opentargets_disease_phenotype_slim.ttl'

    CHEMBL_TARGET_BY_UNIPROT_ID = uris.get(URIS_SECTION, 'chembl_target')
    CHEMBL_MECHANISM = uris.get(URIS_SECTION, 'chembl_mechanism')
    CHEMBL_MOLECULE_SET = '''https://www.ebi.ac.uk/chembl/api/data/molecule/set/{}.json'''
    CHEMBL_PROTEIN_CLASS = uris.get(URIS_SECTION, 'chembl_protein')
    CHEMBL_TARGET_COMPONENT = uris.get(URIS_SECTION, 'chembl_component')

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
                                                         cttv010 = 'expression_atlas'
                                                )

    DATASOURCE_TO_DATATYPE_MAPPING = defaultdict(lambda: "other")
    DATASOURCE_TO_DATATYPE_MAPPING['expression_atlas'] = 'rna_expression'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['reactome'] = 'affected_pathway'
    DATASOURCE_TO_DATATYPE_MAPPING['eva'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['phenodigm'] = 'animal_model'
    DATASOURCE_TO_DATATYPE_MAPPING['gwas_catalog'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['cancer_gene_census'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['eva_somatic'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['chembl'] = 'known_drug'
    DATASOURCE_TO_DATATYPE_MAPPING['europepmc'] = 'literature'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot_literature'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['intogen'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['gene2phenotype'] = 'genetic_association'

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
    # SCORING_WEIGHTS['gwas_catalog'] = 1.5

    # setup a minimum score value for an evidence string to be accepted.
    SCORING_MIN_VALUE_FILTER = defaultdict(lambda: 0)
    SCORING_MIN_VALUE_FILTER['phenodigm'] = 0.4


    ENSEMBL_RELEASE_VERSION=88
    ENSEMBL_CHUNK_SIZE=100

    # see http://stackoverflow.com/a/847866
    TEMP_DIR = tempfile.gettempdir()
    REDISLITE_DB_PATH = os.path.join(TEMP_DIR, 'opentargets_redislite.rdb')

    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]


    #dump file names
    DUMP_FILE_FOLDER = from_env('CTTV_DUMP_FOLDER', TEMP_DIR)
    DUMP_FILE_EVIDENCE=RELEASE_VERSION+'_evidence_data.json.gz'
    DUMP_FILE_ASSOCIATION = RELEASE_VERSION + '_association_data.json.gz'
    DUMP_PAGE_SIZE = 10000
    DUMP_BATCH_SIZE = 10
    DUMP_REMOTE_API = from_env('DUMP_REMOTE_API_URL', 'http://beta.opentargets.io')
    DUMP_REMOTE_API_PORT = from_env('DUMP_REMOTE_API_PORT', 80, t=int)
    DUMP_REMOTE_API_SECRET = from_env('DUMP_REMOTE_API_SECRET')
    DUMP_REMOTE_API_APPNAME = from_env('DUMP_REMOTE_API_APPNAME')

    #Literature Pipeline -- Pubmed/Medline FTP server
    PUBMED_TEMP_DIR = os.path.join(TEMP_DIR, 'medline')
    PUBMED_FTP_SERVER = 'ftp.ncbi.nlm.nih.gov'
    PUBMED_XML_LOCN = os.path.join(PUBMED_TEMP_DIR, 'baseline')
    PUBMED_XML_UPDATE_LOCN = os.path.join(PUBMED_TEMP_DIR, 'update')

    PUBMED_HTTP_MIRROR = 'https://storage.googleapis.com/pubmed-medline'

    #GE Pipeline

    GE_EVIDENCE_STRING = '/tmp/genomics_england_evidence_string.json'
    GE_LINKOUT_URL = 'https://bioinfo.extge.co.uk/crowdsourcing/PanelApp/GeneReview'
    GE_ZOOMA_DISEASE_MAPPING = '/tmp/zooma_disease_mapping.csv'
    GE_ZOOMA_DISEASE_MAPPING_NOT_HIGH_CONFIDENT = '/tmp/zooma_disease_mapping_low_confidence.csv'

    # for developers
    DRY_RUN_OUTPUT = from_env('CTTV_DRY_RUN_OUTPUT', False, t=bool,
                                     parse_func=parse_bool)
    DRY_RUN_OUTPUT_DELETE = from_env('CTTV_DRY_RUN_OUTPUT_DELETE', False,
                                     t=bool, parse_func=parse_bool)
    DRY_RUN_OUTPUT_COUNT = from_env('CTTV_DRY_RUN_OUTPUT_COUNT', 1000, t=int)

    # This config file is like this and no prefixes or version will be
    # appended
    #
    # [indexes]
    # gene-data=new-gene-data-index-name
    # ...
    #
    # if no index field or config file is found then a default
    # composed index name will be returned
    ES_CUSTOM_IDXS_FILENAME = 'es_custom_idxs.ini'
    ES_CUSTOM_IDXS = from_env('CTTV_ES_CUSTOM_IDXS', False, t=bool,
                              parse_func=parse_bool)
    ES_CUSTOM_IDXS_INI = ini_from_file_or_resource(ES_CUSTOM_IDXS_FILENAME) \
        if ES_CUSTOM_IDXS else None
