__author__ = 'andreap'
import os
import ConfigParser

iniparser = ConfigParser.ConfigParser()
iniparser.read('db.ini')

class Config():

    ENV=os.environ.get('CTTV_EL_LOADER') or 'dev'
    ELASTICSEARCH_URL = 'http://'+iniparser.get(ENV, 'elurl')+':'+iniparser.get(ENV, 'elport')+'/'
    # ELASTICSEARCH_URL = [{"host": iniparser.get(ENV, 'elurl'), "port": iniparser.get(ENV, 'elport')}]
    ELASTICSEARCH_DATA_INDEX_NAME = 'evidence-data'
    ELASTICSEARCH_DATA_DOC_NAME = 'evidencestring'
    ELASTICSEARCH_EFO_LABEL_INDEX_NAME = 'efo-data'
    ELASTICSEARCH_EFO_LABEL_DOC_NAME = 'efo'
    ELASTICSEARCH_ECO_INDEX_NAME = 'eco-data'
    ELASTICSEARCH_ECO_DOC_NAME = 'eco'
    ELASTICSEARCH_GENE_NAME_INDEX_NAME = 'gene-data'
    ELASTICSEARCH_GENE_NAME_DOC_NAME = 'genedata'
    DEBUG = True
    PROFILE = False
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'C=41d6xo]4940NP,9jwF@@v0KDdTtO'
    PUBLIC_API_BASE_PATH = '/api/public/v'
    PRIVATE_API_BASE_PATH = '/api/private/v'
    API_VERSION = '0.2'
    SQLALCHEMY_COMMIT_ON_TEARDOWN = True
    SQLALCHEMY_RECORD_QUERIES = True
    ERROR_IDS_FILE = 'errors.txt'
    POSTGRES_DATABASE = {'drivername': 'postgres',
            'host': iniparser.get(ENV, 'host'),
            'port': iniparser.get(ENV, 'port'),
            'username': iniparser.get(ENV, 'username'),
            'password': iniparser.get(ENV, 'password'),
            'database': iniparser.get(ENV, 'database')}




def _get_evidence_string_generic_mapping():
    return {
            "properties" : {
                "biological_subject" : {
                    "type" : "object",
                     "properties" : {
                         "about" : {
                              "type" : "string",
                              "index" : "not_analyzed"
                         },
                         "properties":{
                             "type" : "object",
                                 "properties" : {
                                     "target_type" : {
                                          "type" : "string",
                                          "index" : "not_analyzed"
                                     },
                                     "activity" : {
                                          "type" : "string",
                                          "index" : "not_analyzed"
                                     },


                                 }
                         }
                     }
                },
                "biological_object" : {
                    "type" : "object",
                     "properties" : {
                         "about" : {
                              "type" : "string",
                              "index" : "not_analyzed"
                         }
                     }
                },
                "_private" : {
                    "type" : "object",
                     "properties" : {
                         "efo_codes" : {
                              "type" : "string",
                              "index" : "not_analyzed"
                         }
                     }
                },
                "biological_object.efo_info" : {
                    "type" : "object",
                     "properties" : {
                         "path" : {
                              "type" : "string",
                              "index" : "not_analyzed"
                         }
                     }
                },
                "evidence" : {
                    "type" : "object",
                     "properties" : {
                         "evidence_codes" : {
                              "type" : "string",
                              "index" : "not_analyzed"
                         },
                         "evidence_chain":{
                             "type" : "object",
                             "properties" : {
                                 "evidence" : {
                                     "type" : "object",
                                     "properties" : {
                                         "evidence_codes" : {
                                              "type" : "string",
                                              "index" : "not_analyzed"
                                         },
                                         "experiment_specific":{
                                             "enabled" : False
                                         },
                                         # "association_score":{
                                         #     "type" : "object",
                                         #     "properties" : {
                                         #         "probability" : {
                                         #             "type" : "object",
                                         #             "properties" : {
                                         #                 "value" : {
                                         #                    "type" : "double",
                                         #                 },
                                         #                 "method" : {
                                         #                    "type" : "string",
                                         #                 }
                                         #
                                         #             }
                                         #         }
                                         #     }
                                         # }
                                     }

                                 }
                             }
                         },
                         "association_score":{
                             "type" : "object",
                             "properties" : {
                                 "probability" : {
                                     "type" : "object",
                                     "properties" : {
                                         "value" : {
                                            "type" : "double",
                                         },
                                         "method" : {
                                            "type" : "string",
                                         }
                                     }
                                 }
                             }
                         },
                     }
                }
            }
       }



class ElasticSearchConfiguration():

    available_databases = ['expression_atlas',
                          'uniprot',
                          'reactome',
                          'eva',
                          'phenodigm',
                          'gwas',
                          'cancer_gene_census',
                          'chembl',
                          'other',
                          ]
    if os.environ.get('CTTV_EL_LOADER')== 'prod':
        generic_shard_number = 3
        generic_replicas_number = 1
        evidence_shard_number = 6
        evidence_replicas_number = 1

        bulk_load_chunk =1000
    else:
        generic_shard_number = 1
        generic_replicas_number = 0
        evidence_shard_number = 3
        evidence_replicas_number = 0
        bulk_load_chunk =25

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
                    "_private" : {
                        "type" : "object",
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
                        "type" : "object",
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



    evidence_mappings = {}
    for db in available_databases:
        evidence_mappings[Config.ELASTICSEARCH_DATA_DOC_NAME+'-'+db]= _get_evidence_string_generic_mapping()

    evidence_data_mapping = { "settings": {"number_of_shards" : evidence_shard_number,
                                           "number_of_replicas" : evidence_replicas_number,
                                           },
                              "mappings": evidence_mappings,
                            }
