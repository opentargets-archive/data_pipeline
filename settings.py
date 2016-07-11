import uuid
from collections import defaultdict, OrderedDict

from common.DataStructure import RelationType

__author__ = 'andreap'
import os
import ConfigParser

iniparser = ConfigParser.ConfigParser()
iniparser.read('db.ini')

class Config():



    RELEASE_VERSION=os.environ.get('CTTV_DATA_VERSION') or'07.16'
    ENV=os.environ.get('CTTV_EL_LOADER') or 'dev'
    ELASTICSEARCH_URL = 'http://'+iniparser.get(ENV, 'elurl')+':'+iniparser.get(ENV, 'elport')+'/'
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
    ELASTICSEARCH_RELATION_INDEX_NAME = 'relation-data.test2'
    ELASTICSEARCH_RELATION_DOC_NAME = 'relation'
    DEBUG = ENV == 'dev'
    PROFILE = False
    ERROR_IDS_FILE = 'errors.txt'
    SPARQL_ENDPOINT_URL = 'http://'+ iniparser.get(ENV, 'virtuoso_host') + ':' + iniparser.get(ENV, 'virtuoso_port') + '/sparql'
    POSTGRES_DATABASE = {'drivername': 'postgres',
            'host': iniparser.get(ENV, 'host'),
            'port': iniparser.get(ENV, 'port'),
            'username': iniparser.get(ENV, 'username'),
            'password': iniparser.get(ENV, 'password'),
            'database': iniparser.get(ENV, 'database')}
    HPA_NORMAL_TISSUE_URL = 'http://v13.proteinatlas.org/download/normal_tissue.csv.zip'
    HPA_CANCER_URL = 'http://v13.proteinatlas.org/download/cancer.csv.zip'
    HPA_SUBCELLULAR_LOCATION_URL = 'http://v13.proteinatlas.org/download/subcellular_location.csv.zip'
    HPA_RNA_URL = 'http://v13.proteinatlas.org/download/rna.csv.zip'
    REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome.txt'
    # REACTOME_ENSEMBL_MAPPINGS = 'http://www.reactome.org/download/current/Ensembl2Reactome_All_Levels.txt'
    REACTOME_PATHWAY_DATA = 'http://www.reactome.org/download/current/ReactomePathways.txt'
    REACTOME_PATHWAY_RELATION = 'http://www.reactome.org/download/current/ReactomePathwaysRelation.txt'
    REACTOME_SBML_REST_URI = 'http://www.reactome.org/ReactomeRESTfulAPI/RESTfulWS/sbmlExporter/{0}'
    EVIDENCEVALIDATION_SCHEMA = "1.2.2"
    EVIDENCEVALIDATION_DATATYPES = ['genetic_association', 'rna_expression', 'genetic_literature', 'affected_pathway', 'somatic_mutation', 'known_drug', 'literature', 'animal_model']
    # path to the FTP files on the processing machine
    EVIDENCEVALIDATION_FTP_SUBMISSION_PATH = '/Users/koscieln/Documents/data/ftp' #'/opt/share/data/ftp' # '/home/gk680303/windows/data/ftp',
    EVIDENCEVALIDATION_FILENAME_REGEX = '^(cttv[0-9]{3}|cttv_external_mousemodels|cttv006_Networks_Reactome)\-\d{2}\-\d{2}\-\d{4}\.json\.gz$'
    EVIDENCEVALIDATION_MAX_NB_ERRORS_REPORTED = 1000
    EVIDENCEVALIDATION_NB_TOP_DISEASES = 20
    EVIDENCEVALIDATION_NB_TOP_TARGETS = 20
    EVIDENCEVALIDATION_PERCENT_SCALE = 20
    EVIDENCEVALIDATION_JSON_SCHEMA_VERSION = '1.2.2'
    # Current genome Assembly
    EVIDENCEVALIDATION_ENSEMBL_ASSEMBLY = 'GRCh38'
    # Change this if you don't want to send e-mails
    EVIDENCEVALIDATION_SEND_EMAIL = True
    EVIDENCEVALIDATION_SENDER_ACCOUNT = 'no_reply@targetvalidation.org'
    MAILGUN_DOMAIN = "https://api.mailgun.net/v3/mg.targetvalidation.org"
    MAILGUN_API_KEY = "key-b7986f9a29fe234733b0af3b1206b146"
    EVIDENCEVALIDATION_BCC_ACCOUNT = [ 'gautier.x.koscielny@gsk.com', 'andreap@targetvalidation.org', 'eliseop@targetvalidation.org' ]
    # Change this if you want to change the list of recipients
    EVIDENCEVALIDATION_PROVIDER_EMAILS = defaultdict(lambda: "other")
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv001"] = [ 'gautier.x.koscielny@gsk.com', 'mmaguire@ebi.ac.uk', 'andreap@targetvalidation.org', 'eliseop@targetvalidation.org' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv006"] = [ 'fabregat@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv007"] = [ 'zs1@sanger.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv008"] = [ 'mpaulam@ebi.ac.uk', 'patricia@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv009"] = [ 'cleroy@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv010"] = [ 'mkeays@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv011"] = [ 'eddturner@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv012"] = [ 'tsmith@ebi.ac.uk', 'garys@ebi.ac.uk' ]
    EVIDENCEVALIDATION_PROVIDER_EMAILS["cttv025"] = [ 'kafkas@ebi.ac.uk', 'ftalo@ebi.ac.uk' ]
    # ftp user and passwords
    EVIDENCEVALIDATION_FTP_HOST= dict( host = '192.168.1.150',
                                       port = 22)
    EVIDENCEVALIDATION_FTP_ACCOUNTS =OrderedDict()
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv001"] = '576f89aa'
    #EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv018"] = 'a8059a72'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv006"] = '7e2a0135'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv009"] = '2b72891d'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv010"] = 'c2a64557'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv011"] = 'bde373ca'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv012"] = '10441b6b'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv008"] = '409a0d21'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv007"] = 'a6052a3b'
    EVIDENCEVALIDATION_FTP_ACCOUNTS["cttv025"] = 'd2b315fa'

    # setup the number of workers to use for data processing. if None defaults to the number of CPUs available
    WORKERS_NUMBER = None

    # mouse models
    MOUSEMODELS_PHENODIGM_SOLR = 'solrclouddev.sanger.ac.uk'
    MOUSEMODELS_CACHE_DIRECTORY = '~/.phenodigmcache'

    DATASOURCE_ASSOCIATION_SCORE_WEIGHT=dict(gwas_catalog=2.5)
    DATASOURCE_ASSOCIATION_SCORE_AUTO_EXTEND_RANGE=dict(
                                                        #phenodigm=dict(min=0.4, max= 1),
                                                        )
    DATASOURCE_INTERNAL_NAME_TRANSLATION = dict(reactome = 'CTTV006_Networks_Reactome',
                                                intact = 'CTTV006_Networks_IntAct',
                                                chembl = 'CTTV008_ChEMBL',
                                                gwas_catalog = 'CTTV009_GWAS_Catalog',
                                                uniprot = 'CTTV011_UniProt',
                                                eva = 'CTTV012_Variation',
                                                # gwas_ibd = 'CTTV018_IBD_GWAS',
                                                phenodigm = 'CTTV001_External_MouseModels',
                                                cancer_gene_census = 'CTTV007_Cancer_Gene_Census',
                                                europepmc = 'CTTV025_Literature',
                                                disgenet = 'CTTV_External_DisGeNet',
                                                rare2common = 'CTTV005_Rare2Common',
                                                expression_atlas = 'CTTV010_Tissue_Specificity'
                                                )

    DATASOURCE_INTERNAL_NAME_TRANSLATION_REVERSED = dict(cttv006 = 'reactome',
                                                         cttv008 = 'chembl',
                                                         cttv009 = 'gwas_catalog',
                                                         cttv011 = 'uniprot',
                                                         cttv012 = 'eva',
                                                         cttv018 = 'gwas_ibd',
                                                         cttv001 = 'phenodigm',
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

    # use specific index for a datasource
    DATASOURCE_TO_INDEX_KEY_MAPPING = defaultdict(lambda: "generic")
    # DATASOURCE_TO_INDEX_KEY_MAPPING['disgenet'] = 'disgenet'
    # DATASOURCE_TO_INDEX_KEY_MAPPING['europepmc'] = 'europepmc'
    # DATASOURCE_TO_INDEX_KEY_MAPPING['phenodigm'] = DATASOURCE_TO_DATATYPE_MAPPING['phenodigm']
    # DATASOURCE_TO_INDEX_KEY_MAPPING['expression_atlas'] = DATASOURCE_TO_DATATYPE_MAPPING['expression_atlas']

    # setup the weights for evidence strings score
    SCORING_WEIGHTS = defaultdict(lambda: 1)
    SCORING_WEIGHTS['phenodigm'] = 0.2
    # SCORING_WEIGHTS['expression_atlas'] = 0.2
    SCORING_WEIGHTS['europepmc'] = 0.2
    SCORING_WEIGHTS['gwas_catalog'] = 1.5

    # setup a minimum score value for an evidence string to be accepted.
    SCORING_MIN_VALUE_FILTER = defaultdict(lambda: 0)
    SCORING_MIN_VALUE_FILTER['phenodigm'] = 0.4


    ENSEMBL_RELEASE_VERSION=84

    REDISLITE_DB_PATH = '/tmp/cttv-redislite.rdb'

    UNIQUE_RUN_ID = str(uuid.uuid4()).replace('-', '')[:16]


    #dump file names
    DUMP_FILE_FOLDER = '/tmp'
    DUMP_FILE_EVIDENCE=RELEASE_VERSION+'_evidence_data.json.gz'
    DUMP_FILE_ASSOCIATION = RELEASE_VERSION + '_association_data.json.gz'
    DUMP_PAGE_SIZE = 10000
    DUMP_BATCH_SIZE = 10



def _get_evidence_string_generic_mapping():
    return {
            "_all" : {"enabled" : True},
            "_routing":{ "required":True},
            "properties" : {
                "target" : {
                     "properties" : {
                         "id" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         "target_type" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         "activity" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },

                     }
                },
                "disease" : {
                     "properties" : {
                         "id" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         "efo_info" : {
                             "properties" : {
                                 "path": {
                                    "type": "string",
                                    "index": "not_analyzed",
                                 }
                             }
                         }

                     }
                },
                "private" : {
                     "properties" : {
                         "efo_codes" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         "facets" : {
                            "properties" : {
                                "uniprot_keywords": {
                                    "type" : "string",
                                    "index" : "not_analyzed",
                                },
                                "reactome": {
                                      "properties" : {
                                           "pathway_type_code": {
                                                "type" : "string",
                                                "index" : "not_analyzed",
                                           },
                                           "pathway_code": {
                                                "type" : "string",
                                                "index" : "not_analyzed",
                                           },
                                      }
                                }
                            }
                         }
                     }
                },
                "evidence" : {
                     "properties" : {
                         "evidence_codes" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         "provenance_type": {
                             "enabled": False,
                             # "properties" : {
                             #    "database" : {
                             #        "properties": {
                             #            "version": {
                             #                "type": "string",
                             #                "index": "not_analyzed",
                             #            },
                             #        },
                             #    },
                             # },
                         },
                     }
                }
            },
        "dynamic_templates" : [
            {
                "scores" : {
                    "path_match" : "scores.*",
                    "mapping" : {
                         "type" : "double",
                    }
                }
            },


            {
                "do_not_index_evidence" : {
                    "path_match" : "evidence.*",
                    "path_unmatch" : "evidence.evidence_codes*",
                    "mapping" : {
                        "enabled" : False,
                    }
                }
            },

            {
                "do_not_index_drug" : {
                    "path_match" : "drug.*",
                    "mapping" : {
                        "enabled" : False,
                    }
                }
            },
            {
                "do_not_index_unique_ass" : {
                    "path_match" : "unique_association_fields.*",
                    "mapping" : {
                        "enabled" : False,
                    }
                }
            },

        ]
       }

def _get_relation_generic_mapping():
    return {
            "_all" : {"enabled" : True},
            "_routing":{ "required":True},
            "properties" : {
                "subject" : {
                     "properties" : {
                         "id" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         # "target_type" : {
                         #      "type" : "string",
                         #      "index" : "not_analyzed",
                         # },
                         # "activity" : {
                         #      "type" : "string",
                         #      "index" : "not_analyzed",
                         # },

                     }
                },
                "object" : {
                     "properties" : {
                         "id" : {
                              "type" : "string",
                              "index" : "not_analyzed",
                         },
                         # "efo_info" : {
                         #     "properties" : {
                         #         "path": {
                         #            "type": "string",
                         #            "index": "not_analyzed",
                         #         }
                         #     }
                         # }

                     }
                },
                "type": {
                    "type": "string",
                    "index": "not_analyzed",
                },
                "id": {
                    "type": "string",
                    "index": "not_analyzed",
                },
                "shared_targets": {
                    "type": "string",
                    "index": "not_analyzed",
                },
                "shared_diseases": {
                    "type": "string",
                    "index": "not_analyzed",
                },


            },
        "dynamic_templates" : [
            {
                "scores" : {
                    "path_match" : "scores.*",
                    "mapping" : {
                         "type" : "double",
                    }
                }
            },

        ]
       }



class ElasticSearchConfiguration():


    available_databases = Config().DATASOURCE_TO_DATATYPE_MAPPING.keys()
    available_databases.append('other')

    if os.environ.get('CTTV_EL_LOADER')== 'prod' or \
            os.environ.get('CTTV_EL_LOADER')== 'stag':
        generic_shard_number = 3
        generic_replicas_number = 0
        evidence_shard_number = 3
        evidence_replicas_number = 1
        relation_shard_number = 10
        relation_replicas_number = 0

        bulk_load_chunk =1000
    else:
        generic_shard_number = 3
        generic_replicas_number = 0
        evidence_shard_number = 3
        evidence_replicas_number = 0
        relation_shard_number = 6
        relation_replicas_number = 0
        bulk_load_chunk =1000

    uniprot_data_mapping = eco_data_mapping = {"mappings": {
                    Config.ELASTICSEARCH_UNIPROT_DOC_NAME: {
                        "properties": {
                            "entry": {
                                "type": "string",
                                "index": "no"
                            },
                                                 }
                    }
                },
                    "settings": {"number_of_shards": 1,
                                 "number_of_replicas": 0,
                                 "refresh_interval": "60s",
                                 },
                }

    eco_data_mapping = {"mappings": {
                           Config.ELASTICSEARCH_ECO_DOC_NAME : {
                                "properties" : {
                                    "code" : {
                                        "type" : "string",
                                        "index" : "not_analyzed"
                                        },
                                    "path_codes" : {
                                        "type" : "string",
                                        "index" : "not_analyzed"
                                        },
                                    "path" : {
                                        "properties" : {
                                            "url" : {
                                                "type" : "string",
                                                "index" : "not_analyzed"
                                                },
                                            },
                                        },
                                    }
                                }
                           },
                        "settings":  {"number_of_shards" : generic_shard_number,
                                      "number_of_replicas" : generic_replicas_number,
                                      "refresh_interval" : "60s",
                                      },
                        }

    efo_data_mapping = {
        "mappings": {
            Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME : {
                "properties" : {
                    "code" : {
                        "type" : "string",
                        "index" : "not_analyzed"
                        },
                    "path_codes" : {
                        "type" : "string",
                        "index" : "not_analyzed"
                        },
                    "path" : {
                        "properties" : {
                            "url" : {
                                "type" : "string",
                                "index" : "not_analyzed"
                                },
                            },
                        },
                    "private" : {
                        # "type" : "object",
                        "properties" : {
                            "suggestions" : {
                                "type" : "completion",
                                "index_analyzer" : "whitespace_analyzer",
                                "search_analyzer" : "edgeNGram_analyzer",
                                "payloads" : True
                                },
                            },
                        },
                    }
                }
            },
        "settings":  {
            "number_of_shards" : generic_shard_number,
            "number_of_replicas" : generic_replicas_number,
            "refresh_interval" : "60s",
            "analysis": {
                 "filter": {
                    "edgeNGram_filter": {
                       "type": "edgeNGram",
                       "min_gram": 2,
                       "max_gram": 20,
                       "token_chars": [
                          "letter",
                          "digit"
                       ]
                    },
                    "simple_filter": {
                       "type": "standard",
                       "token_chars": [
                          "letter",
                          "digit"
                       ]
                    }
                 },
                 "analyzer": {
                    "edgeNGram_analyzer": {
                       "type": "custom",
                       "tokenizer": "whitespace",
                       "filter": [
                          "lowercase",
                          "asciifolding",
                          "edgeNGram_filter"
                       ]
                    },
                    "whitespace_analyzer": {
                       "type": "custom",
                       "tokenizer": "whitespace",
                       "filter": [
                          "lowercase",
                          "asciifolding",
                          "simple_filter",
                       ]
                    }
                 }
               }
            },
        }

    gene_data_mapping = {
         "settings": {
                "number_of_shards" : generic_shard_number,
                "number_of_replicas" : generic_replicas_number,
                "refresh_interval" : "60s",
                "analysis": {
                    "filter": {
                         "edgeNGram_filter": {
                            "type": "edgeNGram",
                            "min_gram": 2,
                            "max_gram": 20,
                            "token_chars": [
                               "letter",
                               "digit"
                            ]
                         },
                         "simple_filter": {
                            "type": "standard",
                            "token_chars": [
                               "letter",
                               "digit"
                            ]
                         }
                    },
                    "analyzer": {
                         "edgeNGram_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                               "lowercase",
                               "asciifolding",
                               "edgeNGram_filter"
                            ]
                         },
                        "whitespace_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                               "lowercase",
                               "asciifolding",
                               "simple_filter",
                            ]
                        }
                    }
                }
            },
        "mappings": {
            Config.ELASTICSEARCH_GENE_NAME_DOC_NAME : {
                "properties" : {
        #             "symbol_synonyms" : {
        #                 "type" : "string",
        #                 "index" : "not_analyzed"
        #                 },
        #             "approved_symbol" : {
        #                 "type" : "string",
        #                 "index" : "not_analyzed"
        #                 },
        #             "ensembl_external_name" : {
        #                 "properties" : {
        #                     "type" : "string",
        #                     "index" : "not_analyzed"
        #                     },
        #                 },
                    "_private" : {
                        # "type" : "object",
                        "properties" : {
                            "suggestions" : {
                                "type" : "completion",
                                "index_analyzer" : "whitespace_analyzer",
                                "search_analyzer" : "whitespace_analyzer",
                                "payloads" : True
                                },
                            "facets":{
                                # "type" : "object",
                                "properties" : {
                                    "reactome" : {
                                        # "type" : "object",
                                        "properties" : {
                                            "pathway_type_code" : {
                                                "type" : "string",
                                                "index" : "not_analyzed"
                                            },
                                            "pathway_code" : {
                                                "type" : "string",
                                                "index" : "not_analyzed"
                                            }
                                        }
                                    },
                                    "uniprot_keywords" : {
                                        "type" : "string",
                                        "index" : "not_analyzed"
                                    }

                                }
                            },
                        },
                    },
                },
                # "dynamic_templates" : [

                    # {
                    #     "do_not_index_drugbank" : {
                    #         "path_match" : "drugbank.*",
                    #         "mapping" : {
                    #             "index" : "no"
                    #         }
                    #     }
                    # },
                    #
                    # {
                    #     "do_not_index_go" : {
                    #         "path_match" : "go.*",
                    #         "mapping" : {
                    #             "index" : "no"
                    #         }
                    #     }
                    # },
                    # {
                    #     "do_not_index_interpro" : {
                    #         "path_match" : "interpro.*",
                    #         "mapping" : {
                    #             "index" : "no"
                    #         }
                    #     }
                    # },
                    # {
                    #     "do_not_index_pdb" : {
                    #         "path_match" : "pdb.*",
                    #         "mapping" : {
                    #             "index" : "no"
                    #         }
                    #     }
                    # },
                    # {
                    #     "do_not_index_reactome" : {
                    #         "path_match" : "reactome.*",
                    #         "mapping" : {
                    #             "index" : "no"
                    #         }
                    #     }
                    # },
            #     ]
            },
        }
    }

    expression_data_mapping = {
        "settings": {
                "number_of_shards" : generic_shard_number,
                "number_of_replicas" : generic_replicas_number,
                "refresh_interval" : "60s"
                }

    }

    submission_audit_mapping = {
                "properties" : {
                    "md5" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                    "provider_id" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                    "data_source_name" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                    "filename" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                   "nb_submission" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "nb_records" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "nb_passed_validation" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "nb_errors" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "nb_duplicates" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "successfully_validated" : {
                        "type" : "boolean",
                        "index" : "not_analyzed"
                        },
                   "date_created" : {
                        "type" : "date",
                        "format" : "basic_date_time_no_millis",
                        "index" : "no"
                        },
                   "date_validated" : {
                        "type" : "date",
                        "format" : "basic_date_time_no_millis",
                        "index" : "no"
                        },
                   "date_modified" : {
                        "type" : "date",
                        "format" : "basic_date_time_no_millis",
                        "index" : "no"
                        }
                }
    }

    validated_data_mapping = {
                "properties" : {
                    "uniq_assoc_fields_hashdig" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                    "json_doc_hashdig" : {
                        "type" : "string",
                        "index" : "not_analyzed",
                        },
                    "evidence_string" : {
                        # "type" : "object",
                        "index": "no",
                    },
                    "target_id" : {
                        "type" : "string",
                        "index" : "not_analyzed"
                        },
                    "disease_id" : {
                        "type" : "string",
                        "index" : "not_analyzed"
                        },
                   "data_source_name" : {
                        "type" : "string",
                        "index": "not_analyzed"
                        },
                   "json_schema_version" : {
                        "type" : "string",
                        "index" : "not_analyzed"
                        },
                   "json_doc_version" : {
                        "type" : "integer",
                        "index" : "not_analyzed"
                        },
                   "release_date" : {
                        "type" : "date",
                        "format" : "basic_date_time_no_millis",
                        }
                }
    }

    evidence_mappings = {}
    for db in available_databases:
        evidence_mappings[Config.ELASTICSEARCH_DATA_DOC_NAME+'-'+db]= _get_evidence_string_generic_mapping()

    evidence_data_mapping = { "settings": {"number_of_shards" : evidence_shard_number,
                                           "number_of_replicas" : evidence_replicas_number,
                                           # "index.store.type": "memory",
                                           "refresh_interval" : "60s",
                                           },
                              "mappings": evidence_mappings,
                            }

    relation_mappings = {}
    for rt in [RelationType.SHARED_DISEASE,
               RelationType.SHARED_TARGET,
               ]:
        relation_mappings[Config.ELASTICSEARCH_RELATION_DOC_NAME + '-' + rt] = _get_relation_generic_mapping()

    relation_data_mapping = {"settings": {"number_of_shards": relation_shard_number,
                                          "number_of_replicas": relation_replicas_number,
                                          # "index.store.type": "memory",
                                          "refresh_interval": "60s",
                                          },
                             "mappings": relation_mappings,
                             }

    validated_data_datasource_mappings = {}
    for db in available_databases:
        validated_data_datasource_mappings[db]= validated_data_mapping

    validated_data_settings_and_mappings = { "settings": {"number_of_shards" : 1,
                                           "number_of_replicas" : 1,
                                           # "index.store.type": "memory",
                                           "refresh_interval" : "60s",
                                           },
                               "mappings": validated_data_datasource_mappings,
    }

    submission_audit_settings_and_mappings = { "settings": {"number_of_shards" : 1,
                                           "number_of_replicas" : 1,
                                           # "index.store.type": "memory",
                                           "refresh_interval" : "60s",
                                           },
                               "mappings": {
                                   Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME: submission_audit_mapping
                               }
    }

    score_data_mapping = { "settings": {"number_of_shards" : evidence_shard_number,
                                       "number_of_replicas" : evidence_replicas_number,
                                       # "index.store.type": "memory",
                                       "refresh_interval" : "60s",
                                       },
                            "mappings": {
                                Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME: {
                                        "_all" : {"enabled" : True},
                                        "_routing":{ "required":True,},
                                        "properties" : {
                                            "target" : {
                                                 "properties" : {
                                                     "id" : {
                                                          "type" : "string",
                                                          "index" : "not_analyzed",
                                                     },
                                                     "target_type" : {
                                                          "type" : "string",
                                                          "index" : "not_analyzed",
                                                     },
                                                     "activity" : {
                                                          "type" : "string",
                                                          "index" : "not_analyzed",
                                                     },

                                                 }
                                            },
                                            "disease" : {
                                                 "properties" : {
                                                     "id" : {
                                                          "type" : "string",
                                                          "index" : "not_analyzed",
                                                     }
                                                 }



                                            },
                                            "private" : {
                                                 "properties" : {
                                                     "efo_codes" : {
                                                          "type" : "string",
                                                          "index" : "not_analyzed",
                                                     },
                                                     "facets" : {
                                                        "properties" : {
                                                            "uniprot_keywords": {
                                                                "type" : "string",
                                                                "index" : "not_analyzed",
                                                            },
                                                            "reactome": {
                                                                      "properties" : {
                                                                           "pathway_type_code": {
                                                                                "type" : "string",
                                                                                "index" : "not_analyzed",
                                                                           },
                                                                           "pathway_code": {
                                                                                "type" : "string",
                                                                                "index" : "not_analyzed",
                                                                           },
                                                                      }
                                                            }
                                                        }
                                                     }
                                                 }
                                            },
                                        },
                                    }
                                }
                            }
    search_obj_data_mapping = {
         "settings": {
                "number_of_shards" : generic_shard_number,
                "number_of_replicas" : generic_replicas_number,
                "refresh_interval" : "60s",
                "analysis": {
                    "filter": {
                         "edgeNGram_filter": {
                            "type": "edgeNGram",
                            "min_gram": 2,
                            "max_gram": 20,
                            "token_chars": [
                               "letter",
                               "digit"
                            ]
                         },
                         "simple_filter": {
                            "type": "standard",
                            "token_chars": [
                               "letter",
                               "digit"
                            ]
                         }
                    },
                    "analyzer": {
                         "edgeNGram_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                               "lowercase",
                               "asciifolding",
                               "edgeNGram_filter"
                            ]
                         },
                        "whitespace_analyzer": {
                            "type": "custom",
                            "tokenizer": "whitespace",
                            "filter": [
                               "lowercase",
                               "asciifolding",
                               "simple_filter",
                            ]
                        }
                    }
                }
            },
        "mappings": {
           '_default_' : {
                "properties" : {
                    "private" : {
                        # "type" : "object",
                        "properties" : {
                            "suggestions" : {
                                "type" : "completion",
                                "index_analyzer" : "whitespace_analyzer",
                                "search_analyzer" : "whitespace_analyzer",
                                "payloads" : True
                                },


                            },
                        },
                    },
                },
            },
        }