import uuid
from collections import defaultdict, OrderedDict


__author__ = 'andreap'
import os
import ConfigParser


iniparser = ConfigParser.ConfigParser()
iniparser.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'db.ini'))

class Config():

    HAS_PROXY = iniparser.has_section('proxy')
    if HAS_PROXY:
        PROXY = iniparser.get('proxy', 'protocol') + "://" + iniparser.get('proxy', 'username') + ":" + iniparser.get(
            'proxy', 'password') + "@" + iniparser.get('proxy', 'host') + ":" + iniparser.get('proxy', 'port')
        PROXY_PROTOCOL = iniparser.get('proxy', 'protocol')
        PROXY_USERNAME = iniparser.get('proxy', 'username')
        PROXY_PASSWORD = iniparser.get('proxy', 'password')
        PROXY_HOST = iniparser.get('proxy', 'host')
        PROXY_PORT = int(iniparser.get('proxy', 'port'))

    ONTOLOGY_CONFIG = ConfigParser.ConfigParser()
    ONTOLOGY_CONFIG.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ontology_config.ini'))

    RELEASE_VERSION=os.environ.get('CTTV_DATA_VERSION') or'16.12'
    ENV=os.environ.get('CTTV_EL_LOADER') or 'dev'
    try:
        ELASTICSEARCH_HOST = iniparser.get(ENV, 'elurl')
        ELASTICSEARCH_PORT = iniparser.get(ENV, 'elport')
        ELASTICSEARCH_URL = 'http://'+ELASTICSEARCH_HOST
        if ELASTICSEARCH_PORT:
            ELASTICSEARCH_URL= ELASTICSEARCH_URL+':'+ELASTICSEARCH_PORT+'/'
    except ConfigParser.NoOptionError:
        ELASTICSEARCH_HOST = None
        ELASTICSEARCH_PORT = None
        ELASTICSEARCH_URL = None

    # ELASTICSEARCH_URL = [{"host": iniparser.get(ENV, 'elurl'), "port": iniparser.get(ENV, 'elport')}]
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
    DEBUG = ENV == 'dev'
    PROFILE = False
    ERROR_IDS_FILE = 'errors.txt'
    try:
        SPARQL_ENDPOINT_URL = 'http://'+ iniparser.get(ENV, 'virtuoso_host') + ':' + iniparser.get(ENV, 'virtuoso_port') + '/sparql'
    except ConfigParser.NoOptionError:
        print 'no virtuoso instance'
        SPARQL_ENDPOINT_URL = ''

    try:
        POSTGRES_DATABASE = {'drivername': 'postgres',
            'host': iniparser.get(ENV, 'host'),
            'port': iniparser.get(ENV, 'port'),
            'username': iniparser.get(ENV, 'username'),
            'password': iniparser.get(ENV, 'password'),
            'database': iniparser.get(ENV, 'database')}
    except ConfigParser.NoOptionError:
        # the official logger is not loaded yet. not moving things around as we
        # will likely put all of this in a config file.
        print 'no postgres instance'
        POSTGRES_DATABASE = {}

    # HGNC_COMPLETE_SET = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/json/hgnc_complete_set.json'
    HGNC_COMPLETE_SET = 'https://4.hidemyass.com/ip-1/encoded/Oi8vZnRwLmViaS5hYy51ay9wdWIvZGF0YWJhc2VzL2dlbmVuYW1lcy9uZXcvanNvbi9oZ25jX2NvbXBsZXRlX3NldC5qc29u&f=norefer'
    # HGNC_ORTHOLOGS = 'http://ftp.ebi.ac.uk/pub/databases/genenames/hcop/human_all_hcop_sixteen_column.txt.gz'
    HGNC_ORTHOLOGS = 'https://5.hidemyass.com/ip-1/encoded/Oi8vZnRwLmViaS5hYy51ay9wdWIvZGF0YWJhc2VzL2dlbmVuYW1lcy9oY29wL2h1bWFuX2FsbF9oY29wX3NpeHRlZW5fY29sdW1uLnR4dC5neg%3D%3D&f=norefer'
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

    CHEMBL_TARGET_BY_UNIPROT_ID = '''https://www.ebi.ac.uk/chembl/api/data/target.json'''
    CHEMBL_MECHANISM = '''https://www.ebi.ac.uk/chembl/api/data/mechanism.json'''
    CHEMBL_DRUG_SYNONYMS = '''https://www.ebi.ac.uk/chembl/api/data/molecule/{}.json'''

    HPA_NORMAL_TISSUE_URL = 'http://v15.proteinatlas.org/download/normal_tissue.csv.zip'
    HPA_CANCER_URL = 'http://v15.proteinatlas.org/download/cancer.csv.zip'
    HPA_SUBCELLULAR_LOCATION_URL = 'http://v15.proteinatlas.org/download/subcellular_location.csv.zip'
    HPA_RNA_URL = 'http://v15.proteinatlas.org/download/rna_tissue.csv.zip'
    REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome.txt'
    # REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome_All_Levels.txt'
    REACTOME_PATHWAY_DATA = 'http://www.reactome.org/download/current/ReactomePathways.txt'
    REACTOME_PATHWAY_RELATION = 'http://www.reactome.org/download/current/ReactomePathwaysRelation.txt'
    REACTOME_SBML_REST_URI = 'http://www.reactome.org/ReactomeRESTfulAPI/RESTfulWS/sbmlExporter/{0}'
    EVIDENCEVALIDATION_SCHEMA = "1.2.3"
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
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv001"] = [ 'gautier.x.koscielny@gsk.com', 'andreap@targetvalidation.org', ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv006"] = [ 'fabregat@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv007"] = [ 'zs1@sanger.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv008"] = [ 'mpaulam@ebi.ac.uk', 'patricia@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv009"] = [ 'cleroy@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv010"] = [ 'mkeays@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv011"] = [ 'eddturner@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv012"] = [ 'tsmith@ebi.ac.uk', 'garys@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv025"] = [ 'kafkas@ebi.ac.uk', 'ftalo@ebi.ac.uk' ]
    # ftp user and passwords
    EVIDENCEVALIDATION_FTP_HOST= dict( host = 'ftp.targetvalidation.org',
                                       port = 22)
    EVIDENCEVALIDATION_FILENAME_REGEX = r".*cttv[0-9]{3}.*\-\d{2}\-\d{2}\-\d{4}\.json\.gz$"
    EVIDENCEVALIDATION_FTP_ACCOUNTS =OrderedDict()
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv001"] = '576f89aa'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv006"] = '7e2a0135'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv009"] = '2b72891d'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv010"] = 'c2a64557'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv011"] = 'bde373ca'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv012"] = '10441b6b'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv008"] = '409a0d21'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv007"] = 'a6052a3b'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv025"] = 'd2b315fa'
    # EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv018"] = 'a8059a72'


    # setup the number of workers to use for data processing. if None defaults to the number of CPUs available
    WORKERS_NUMBER = None

    # mouse models
    MOUSEMODELS_PHENODIGM_SOLR = 'solrclouddev.sanger.ac.uk'
    MOUSEMODELS_CACHE_DIRECTORY = '~/.phenodigmcache'

    # hardcoded folder of json file to be preprocessed to extract
    # HP and MP terms not in EFO but that will be combined in a SLIM
    ONTOLOGY_PREPROCESSING_DATASOURCES = [
        'cttv008-14-03-2016.json.gz',
        ''
    ]

    ONTOLOGY_PREPROCESSING_FTP_ACCOUNTS = ["cttv008", "cttv012"]

    # put the path to the file where you want to write the SLIM file (turtle format)
    ONTOLOGY_SLIM_FILE = '/Users/koscieln/Documents/work/gitlab/remote_reference_data_import/bin_import_nonEFO_terms/opentargets_disease_phenotype_slim.ttl'

    CHEMBL_URIS = dict(
        protein_class='https://www.ebi.ac.uk/chembl/api/data/protein_class',
        target_component='https://www.ebi.ac.uk/chembl/api/data/target_component',
    )

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
    DATASOURCE_TO_DATATYPE_MAPPING['disgenet'] = 'literature'
    DATASOURCE_TO_DATATYPE_MAPPING['uniprot_literature'] = 'genetic_association'
    DATASOURCE_TO_DATATYPE_MAPPING['intogen'] = 'somatic_mutation'
    DATASOURCE_TO_DATATYPE_MAPPING['gene2phenotype'] = 'genetic_association'

    # use specific index for a datasource
    DATASOURCE_TO_INDEX_KEY_MAPPING = defaultdict(lambda: "generic")
    # DATASOURCE_TO_INDEX_KEY_MAPPING['disgenet'] = 'disgenet'
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


    ENSEMBL_RELEASE_VERSION=85

    REDISLITE_DB_PATH = '/tmp/cttv-redislite.rdb'

    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]


    #dump file names
    DUMP_FILE_FOLDER = os.environ.get('CTTV_DUMP_FOLDER') or '/tmp'
    DUMP_FILE_EVIDENCE=RELEASE_VERSION+'_evidence_data.json.gz'
    DUMP_FILE_ASSOCIATION = RELEASE_VERSION + '_association_data.json.gz'
    DUMP_PAGE_SIZE = 10000
    DUMP_BATCH_SIZE = 10
    DUMP_REMOTE_API = 'https://beta.targetvalidation.org'
    DUMP_REMOTE_API_SECRET = '1RT6L519zkcTH9i3F99OjeYn13k79Wep'
    DUMP_REMOTE_API_APPNAME = 'load-test'
