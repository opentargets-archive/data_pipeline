import os

from settings import Config


def _get_evidence_string_generic_mapping():
    return {
        "_all": {"enabled": True},
        "_routing": {"required": True},
        "properties": {
            "target": {
                "properties": {
                    "id": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "target_type": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "activity": {
                        "type": "string",
                        "index": "not_analyzed",
                    },

                }
            },
            "disease": {
                "properties": {
                    "id": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "efo_info": {
                        "properties": {
                            "path": {
                                "type": "string",
                                "index": "not_analyzed",
                            }
                        }
                    }

                }
            },
            "private": {
                "properties": {
                    "efo_codes": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "facets": {
                        "properties": {
                            "uniprot_keywords": {
                                "type": "string",
                                "index": "not_analyzed",
                            },
                            "reactome": {
                                "properties": {
                                    "pathway_type_code": {
                                        "type": "string",
                                        "index": "not_analyzed",
                                    },
                                    "pathway_code": {
                                        "type": "string",
                                        "index": "not_analyzed",
                                    },
                                }
                            },
                            "literature": {
                                "properties": {
                                    "abstract_lemmas": {
                                        "properties": {
                                            "count": {
                                                "type": "long"
                                            },
                                            "value": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            }
                                        }
                                    },
                                    "noun_chunks": {
                                        "properties": {
                                            "count": {
                                                "type": "long"
                                            },
                                            "value": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            }
                                        }
                                    },
                                    "chemicals": {
                                        "properties": {
                                            "registryNumber": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            },
                                            "name": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            }
                                        }
                                    },
                                    "doi": {
                                        "type": "string",
                                        "index": "not_analyzed"
                                    },
                                    "pub_type": {
                                        "type": "string",
                                        "index": "not_analyzed"
                                    },
                                    "mesh_headings": {
                                        "properties": {
                                            "descriptorName": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            }
                                        }

                                    }
                    }
                }
            }
                    }
                }
            },
            "evidence": {
                "properties": {
                    "evidence_codes": {
                        "type": "string",
                        "index": "not_analyzed",
                    },
                    "provenance_type": {
                        "enabled": False,
                        "type": "object"
                    },
                }
            },
            "literature": {
                "properties": {
                    "references": {
                        "properties": {
                            "lit_id": {
                                "type": "string",
                                "index": "not_analyzed",
                            }
                        }
                    },
                    "abstract": {
                        "type": "string",
                        "analyzer": "english"

                    },
                    "title": {
                        "type": "string",
                        "analyzer": "english"

                    },
                    "year": {
                        "type": "date",
                        "format": "yyyy"

                    },
                    "journal_data": {
                        "properties": {
                            "journal": {
                                "properties": {
                                    "medlineAbbreviation": {
                                        "type": "string",
                                        "index": "not_analyzed"
                                    }
                                }
                            }

                        }
                    }
                }
            }
        },
        "dynamic_templates": [
            {
                "scores": {
                    "path_match": "scores.*",
                    "mapping": {
                        "type": "double",
                    }
                }
            },

            {
                "do_not_index_evidence": {
                    "path_match": "evidence.*",
                    "path_unmatch": "evidence.evidence_codes*",
                    "mapping": {
                        "enabled": False,
                    }
                }
            },

            {
                "do_not_index_drug": {
                    "path_match": "drug.*",
                    "mapping": {
                        "enabled": False,
                    }
                }
            },
            {
                "do_not_index_unique_ass": {
                    "path_match": "unique_association_fields.*",
                    "mapping": {
                        "enabled": False,
                    }
                }
            },

        ]
    }


def _get_relation_generic_mapping():
    return {
        "_all": {"enabled": True},
        "_routing": {"required": True},
        "properties": {
            "subject": {
                "properties": {
                    "id": {
                        "type": "string",
                        "index": "not_analyzed",
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
            "object": {
                "properties": {
                    "id": {
                        "type": "string",
                        "index": "not_analyzed",
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
        "dynamic_templates": [
            {
                "scores": {
                    "path_match": "scores.*",
                    "mapping": {
                        "type": "double",
                    }
                }
            },

        ]
    }


class ElasticSearchConfiguration():
    available_databases = Config().DATASOURCE_TO_DATATYPE_MAPPING.keys()
    available_databases.append('other')

    if os.environ.get('CTTV_EL_LOADER') == 'prod' or \
                    os.environ.get('CTTV_EL_LOADER') == 'stag':
        generic_shard_number = '3'
        generic_replicas_number = '0'
        evidence_shard_number = '3'
        evidence_replicas_number = '1'
        relation_shard_number = '10'
        relation_replicas_number = '0'
        validation_shard_number = '1'
        validation_replicas_number = '1'
        submission_audit_shard_number = '1'
        submission_audit_replicas_number = '1'
        bulk_load_chunk = 1000
    else:
        generic_shard_number = '3'
        generic_replicas_number = '0'
        evidence_shard_number = '3'
        evidence_replicas_number = '0'
        relation_shard_number = '6'
        relation_replicas_number = '0'
        validation_shard_number = '1'
        validation_replicas_number = '1'
        submission_audit_shard_number = '1'
        submission_audit_replicas_number = '1'
        bulk_load_chunk = 1000

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
        "settings": {"number_of_shards": '1',
                     "number_of_replicas": '1',
                     "refresh_interval": "60s",
                     },
    }

    eco_data_mapping = {"mappings": {
        Config.ELASTICSEARCH_ECO_DOC_NAME: {
            "properties": {
                "code": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "path_codes": {
                    "type": "string",
                    "index": "not_analyzed"
                },
                "path": {
                    "properties": {
                        "url": {
                            "type": "string",
                            "index": "not_analyzed"
                        },
                    },
                },
            }
        }
    },
        "settings": {"number_of_shards": generic_shard_number,
                     "number_of_replicas": generic_replicas_number,
                     "refresh_interval": "60s",
                     },
    }

    efo_data_mapping = {
        "mappings": {
            Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME: {
                "properties": {
                    "code": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "path_codes": {
                        "type": "string",
                        "index": "not_analyzed"
                    },
                    "path": {
                        "properties": {
                            "url": {
                                "type": "string",
                                "index": "not_analyzed"
                            },
                        },
                    },
                    # "private": {
                    #     # "type" : "object",
                    #     "properties": {
                    #         "suggestions": {
                    #             "type": "completion",
                    #             "analyzer": "whitespace_analyzer",
                    #             "search_analyzer": "edgeNGram_analyzer",
                    #             "payloads": True
                    #         },
                    #     },
                    # },
                }
            }
        },
        "settings": {
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "60s",
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
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "60s",
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
            Config.ELASTICSEARCH_GENE_NAME_DOC_NAME: {
                "properties": {
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
                    "protein_class": {
                        "properties": {
                            "label": {
                                "type": "string",
                                "index" : "not_analyzed"
                            }
                        }
                    },
                    "_private": {
                        # "type" : "object",
                        "properties": {
                            # "suggestions": {
                            #     "type": "completion",
                            #     "analyzer": "whitespace_analyzer",
                            #     "search_analyzer": "whitespace_analyzer",
                            #     "payloads": True
                            # },
                            "facets": {
                                # "type" : "object",
                                "properties": {
                                    "reactome": {
                                        # "type" : "object",
                                        "properties": {
                                            "pathway_type_code": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            },
                                            "pathway_code": {
                                                "type": "string",
                                                "index": "not_analyzed"
                                            }
                                        }
                                    },
                                    "uniprot_keywords": {
                                        "type": "string",
                                        "index": "not_analyzed"
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
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "60s"
        }

    }

    submission_audit_mapping = {
        "properties": {
            "md5": {
                "type": "string",
                "index": "not_analyzed",
            },
            "provider_id": {
                "type": "string",
                "index": "not_analyzed",
            },
            "data_source_name": {
                "type": "string",
                "index": "not_analyzed",
            },
            "filename": {
                "type": "string",
                "index": "not_analyzed",
            },
            "nb_submission": {
                "type": "integer",
            },
            "nb_records": {
                "type": "integer",
            },
            "nb_passed_validation": {
                "type": "integer",
            },
            "nb_errors": {
                "type": "integer",
            },
            "nb_duplicates": {
                "type": "integer",
            },
            "successfully_validated": {
                "type": "boolean",
            },
            "date_created": {
                "type": "date",
                "format": "basic_date_time_no_millis",
            },
            "date_validated": {
                "type": "date",
                "format": "basic_date_time_no_millis",
            },
            "date_modified": {
                "type": "date",
                "format": "basic_date_time_no_millis",
            }
        }
    }

    validated_data_mapping = {
        "dynamic_templates": [
            {
                "evidence_string_template": {
                    "path_match": "evidence_string.*",
                    "mapping": {
                        "index": "no"
                    }
                }
            },
        ],
        "properties": {
            "uniq_assoc_fields_hashdig": {
                "type": "string",
                "index": "not_analyzed",
            },
            "json_doc_hashdig": {
                "type": "string",
                "index": "not_analyzed",
            },
            "target_id": {
                "type": "string",
                "index": "not_analyzed"
            },
            "disease_id": {
                "type": "string",
                "index": "not_analyzed"
            },
            "data_source_name": {
                "type": "string",
                "index": "not_analyzed"
            },
            "json_schema_version": {
                "type": "string",
                "index": "not_analyzed"
            },
            "json_doc_version": {
                "type": "integer",
            },
            "release_date": {
                "type": "date",
                "format": "basic_date_time_no_millis",
            }
        }
    }

    evidence_data_mapping = {"settings": {"number_of_shards": evidence_shard_number,
                                          "number_of_replicas": evidence_replicas_number,
                                          # "index.store.type": "memory",
                                          "refresh_interval": "60s",
                                          "max_result_window": str(int(10e6)),
                                          },
                             "mappings": {"_default_": _get_evidence_string_generic_mapping()},
                             }

    relation_mappings = {}
    from common.DataStructure import RelationType
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

    validated_data_settings_and_mappings = {"settings": {"number_of_shards": validation_shard_number,
                                                         "number_of_replicas": validation_replicas_number,
                                                         # "index.store.type": "memory",
                                                         "refresh_interval": "60s",
                                                         },
                                            "mappings": {"_default_": validated_data_mapping},

                                            }

    submission_audit_settings_and_mappings = {"settings": {"number_of_shards": submission_audit_shard_number,
                                                           "number_of_replicas": submission_audit_replicas_number,
                                                           # "index.store.type": "memory",
                                                           "refresh_interval": "60s",
                                                           },
                                              "mappings": {
                                                  Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_DOC_NAME:
                                                      submission_audit_mapping
                                              }
                                              }

    score_data_mapping = {"settings": {"number_of_shards": evidence_shard_number,
                                       "number_of_replicas": evidence_replicas_number,
                                       # "index.store.type": "memory",
                                       "refresh_interval": "60s",
                                       "max_result_window": str(int(5e6)),
                                       },
                          "mappings": {
                              Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME: {
                                  "_all": {"enabled": True},
                                  # "_routing": {"required": True,},
                                  "properties": {
                                      "target": {
                                          "properties": {
                                              "id": {
                                                  "type": "string",
                                                  "index": "not_analyzed",
                                              },
                                              "target_type": {
                                                  "type": "string",
                                                  "index": "not_analyzed",
                                              },
                                              "activity": {
                                                  "type": "string",
                                                  "index": "not_analyzed",
                                              },

                                          }
                                      },
                                      "disease": {
                                          "properties": {
                                              "id": {
                                                  "type": "string",
                                                  "index": "not_analyzed",
                                              }
                                          }

                                      },
                                      "private": {
                                          "properties": {
                                              "efo_codes": {
                                                  "type": "string",
                                                  "index": "not_analyzed",
                                              },
                                              "facets": {
                                                  "properties": {
                                                      "uniprot_keywords": {
                                                          "type": "string",
                                                          "index": "not_analyzed",
                                                      },
                                                      "reactome": {
                                                          "properties": {
                                                              "pathway_type_code": {
                                                                  "type": "string",
                                                                  "index": "not_analyzed",
                                                              },
                                                              "pathway_code": {
                                                                  "type": "string",
                                                                  "index": "not_analyzed",
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
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "60s",
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
            '_default_': {
                "properties": {
                    "private": {
                        # "type" : "object",
                        "properties": {
                            "suggestions": {
                                "type": "completion",
                                "analyzer": "whitespace_analyzer",
                                "search_analyzer": "whitespace_analyzer",
                                "payloads": True
                            }
                        }
                    }
                },
                "dynamic_templates": [
                    {
                        "do_not_analyze_ortholog": {
                            "match_mapping_type": "string",
                            "path_match": "ortholog*symbol",
                            "mapping": {
                                "index": "not_analyzed"
                            }
                        }
                    }
                ]
            },
        },
    }

    publication_data_mapping = {
        "settings": {
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "1s",
        },
        "mappings": {
                Config.ELASTICSEARCH_PUBLICATION_DOC_NAME: {
                    "properties": {
                        "date_of_revision": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis",

                        },
                        "date": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis",
                        }
                    }
                },
                Config.ELASTICSEARCH_PUBLICATION_DOC_ANALYSIS_SPACY_NAME: {
                    "_parent": {
                        "type": Config.ELASTICSEARCH_PUBLICATION_DOC_NAME,
                        "fielddata": {
                            "loading": "eager_global_ordinals"
                        },
                    },
                    "_routing": {
                        "required": True
                    }
                }
            }
        }


    INDEX_MAPPPINGS = {Config.ELASTICSEARCH_DATA_INDEX_NAME: evidence_data_mapping,
                       Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME: score_data_mapping,
                       Config.ELASTICSEARCH_EFO_LABEL_INDEX_NAME: efo_data_mapping,
                       Config.ELASTICSEARCH_ECO_INDEX_NAME: eco_data_mapping,
                       Config.ELASTICSEARCH_GENE_NAME_INDEX_NAME: gene_data_mapping,
                       Config.ELASTICSEARCH_EXPRESSION_INDEX_NAME: expression_data_mapping,
                       Config.ELASTICSEARCH_DATA_SEARCH_INDEX_NAME: search_obj_data_mapping,
                       Config.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME: validated_data_settings_and_mappings,
                       Config.ELASTICSEARCH_DATA_SUBMISSION_AUDIT_INDEX_NAME: submission_audit_settings_and_mappings,
                       Config.ELASTICSEARCH_UNIPROT_INDEX_NAME: uniprot_data_mapping,
                       Config.ELASTICSEARCH_RELATION_INDEX_NAME: relation_data_mapping,
                       Config.ELASTICSEARCH_PUBLICATION_INDEX_NAME: publication_data_mapping,
                       }
