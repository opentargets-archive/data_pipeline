from mrtarget.Settings import Config
from addict import Dict


def _get_evidence_string_generic_mapping():
    mmap = Dict()
    mmap._routing.required = True
    mmap.properties.target.properties.id.type = 'keyword'
    mmap.properties.target.properties.target_type.type = 'keyword'
    mmap.properties.target.properties.activity.type = 'keyword'

    mmap.properties.disease.properties.id.type = 'keyword'
    mmap.properties.disease.properties.efo_info.properties.path.type = 'keyword'

    mmap.properties.private.properties.efo_codes.type = 'keyword'

    facets = Dict()
    facets.properties.uniprot_keywords.type = 'keyword'

    facets.properties.reactome.properties.pathway_type_code.type = 'keyword'
    facets.properties.reactome.properties.pathway_code.type = 'keyword'

    facets.properties.literature.properties.abstract_lemmas.properties.count.type = 'long'
    facets.properties.literature.properties.abstract_lemmas.properties.value.type = 'keyword'
    facets.properties.literature.properties.noun_chunks.type = 'keyword'
    facets.properties.literature.properties.chemicals.properties.registryNumber.type = 'keyword'
    facets.properties.literature.properties.chemicals.properties.name.type = 'keyword'
    facets.properties.literature.properties.doi.type = 'keyword'
    facets.properties.literature.properties.pub_type.type = 'keyword'
    facets.properties.literature.properties.mesh_heading.properties.id.type = 'keyword'
    facets.properties.literature.properties.mesh_heading.properties.label.type = 'keyword'

    mmap.properties.private.properties.facets = facets

    mmap.properties.evidence.properties.evidence_codes.type = 'keyword'
    mmap.properties.evidence.properties.provenance_type.properties.database.properties.version.type = 'keyword'

    mmap.properties.literature.properties.references.properties.lit_id.type = 'keyword'
    mmap.properties.literature.properties.abstract.type = 'text'
    mmap.properties.literature.properties.abstract.analyzer = 'english'
    mmap.properties.literature.properties.title.type = 'text'
    mmap.properties.literature.properties.title.analyzer = 'english'
    mmap.properties.literature.properties.year.type = 'date'
    mmap.properties.literature.properties.year.format = 'yyyy'
    mmap.properties.literature.properties.journal_data.properties.medlineAbbreviation.type = 'keyword'

    dscores = Dict()
    dscores.scores.path_match = 'scores.*'
    dscores.scores.mapping.type = 'float'

    locib = Dict()
    locib.loci_begin.path_match = "loci.gene.*.begin"
    locib.loci_begin.mapping.type = "long_range"

    locie = Dict()
    locie.loci_end.path_match = "loci.gene.*.end"
    locie.loci_end.mapping.type = "long_range"

    locibv = Dict()
    locibv.loci_begin.path_match = "loci.variant.*.begin"
    locibv.loci_begin.mapping.type = "long_range"

    lociev = Dict()
    lociev.loci_end.path_match = "loci.variant.*.end"
    lociev.loci_end.mapping.type = "long_range"

    devs = Dict()
    devs.do_not_index_evidence.path_match = 'evidence.*'
    devs.do_not_index_evidence.path_unmatch = 'evidence.evidence_codes*'
    devs.do_not_index_evidence.mapping.enabled = False

    ddrug = Dict()
    ddrug.do_not_index_drug.path_match = 'drug.*'
    ddrug.do_not_index_drug.mapping.enabled = False

    dass = Dict()
    dass.do_not_index_unique_ass.path_match = 'unique_association_fields.*'
    dass.do_not_index_unique_ass.mapping.enabled = False

    mmap.dynamic_templates = [
        dscores,
        devs,
        ddrug,
        dass,
        locib,
        locie,
        locibv,
        lociev
        ]

    return mmap.to_dict()

def _get_relation_generic_mapping():
    mmap = Dict()
    mmap._routing.required = True
    mmap.properties.subject.properties.id.type = 'keyword'
    mmap.properties.object.properties.id.type = 'keyword'
    mmap.properties.type.type = 'keyword'
    mmap.properties.id.type = 'keyword'
    mmap.properties.shared_targets.type = 'keyword'
    mmap.properties.shared_diseases.type = 'keyword'

    dscores = Dict()
    dscores.scores.path_match = 'scores.*'
    dscores.scores.mapping.type = 'float'

    mmap.dynamic_templates = [dscores]
    return mmap.to_dict()


class ElasticSearchConfiguration():
    available_databases = Config().DATASOURCE_TO_DATATYPE_MAPPING.keys()
    available_databases.append('other')

    generic_shard_number = '1'
    generic_replicas_number = '0'
    evidence_shard_number = '1'
    evidence_replicas_number = '0'
    relation_shard_number = '1'
    relation_replicas_number = '0'
    publication_shard_number = '5'
    publication_replicas_number = '0'
    validation_shard_number = '1'
    validation_replicas_number = '0'
    submission_audit_shard_number = '1'
    submission_audit_replicas_number = '0'
    bulk_load_chunk = 1000

    unip = Dict()
    unip.mappings[Config.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.type = 'binary'
    unip.mappings[Config.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.index = False
    unip.mappings[Config.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.store = True
    unip.settings.number_of_shards = '1'
    unip.settings.number_of_replicas = generic_replicas_number
    unip.settings.refresh_interval = '60s'

    uniprot_data_mapping = unip.to_dict()

    ecom = Dict()

    ecom.mappings[Config.ELASTICSEARCH_ECO_DOC_NAME].properties.code.type = 'keyword'
    ecom.mappings[Config.ELASTICSEARCH_ECO_DOC_NAME].properties.path_codes.type = 'keyword'
    ecom.mappings[Config.ELASTICSEARCH_ECO_DOC_NAME].properties.path.properties.uri.type = 'keyword'
    ecom.settings.number_of_shards = generic_shard_number
    ecom.settings.number_of_replicas = generic_replicas_number
    ecom.settings.refresh_interval = '60s'

    eco_data_mapping = ecom.to_dict()

    efom = Dict()
    efomp = Dict()
    efomp.code.type = 'keyword'
    efomp.phenotypes.properties.uri.type = 'keyword'
    efomp.path_codes.type = 'keyword'
    efomp.path.properties.uri.type = 'keyword'
    efom.mappings[Config.ELASTICSEARCH_EFO_LABEL_DOC_NAME].properties = efomp

    efom.settings.number_of_shards = generic_shard_number
    efom.settings.number_of_replicas = generic_replicas_number
    efom.settings.refresh_interval = '60s'
    efom.settings.analysis.filter.edgeNGram_filter.type = 'edgeNGram'
    efom.settings.analysis.filter.edgeNGram_filter.min_gram = '2'
    efom.settings.analysis.filter.edgeNGram_filter.max_gram = '20'
    efom.settings.analysis.filter.edgeNGram_filter.token_chars = ['letter', 'digit']
    efom.settings.analysis.filter.simple_filter.type = 'standard'
    efom.settings.analysis.filter.simple_filter.token_chars = ['letter', 'digit']
    efom.settings.analysis.analyzer.edgeNGram_analyzer.type = 'custom'
    efom.settings.analysis.analyzer.edgeNGram_analyzer.tokenizer = 'whitespace'
    efom.settings.analysis.analyzer.edgeNGram_analyzer.filter = ["lowercase",
                                                                 "asciifolding",
                                                                 "edgeNGram_filter"]
    efom.settings.analysis.analyzer.whitespace_analyzer.type = 'custom'
    efom.settings.analysis.analyzer.whitespace_analyzer.tokenizer = 'whitespace'
    efom.settings.analysis.analyzer.whitespace_analyzer.filter = ["lowercase",
                                                                 "asciifolding",
                                                                 "simple_filter"]

    efo_data_mapping = efom.to_dict()

    gene_data_mapping = {
        "settings": {
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            "refresh_interval": "60s",
            "analysis": {
                "filter": {
                    "edgeNGram_filter": {
                        "type": "edgeNGram",
                        "min_gram": "2",
                        "max_gram": "20",
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
                                "type": "keyword"
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
                                                "type": "keyword"
                                            },
                                            "pathway_code": {
                                                "type": "keyword"
                                            }
                                        }
                                    },
                                    "uniprot_keywords": {
                                        "type": "keyword"
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

    expm = Dict()
    expm.settings.number_of_shards = generic_shard_number
    expm.settings.number_of_replicas = generic_replicas_number
    expm.settings.refresh_interval = '60s'
    expm.mappings[Config.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.gene.type = 'keyword'
    expm.mappings[Config.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.tissues.properties.efo_code.type = 'keyword'
    expm.mappings[Config.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.tissues.properties.rna.properties.value.type = 'float'

    expression_data_mapping = expm.to_dict()

    submission_audit_mapping = {
        "properties": {
            "md5": {
                "type": "keyword"
            },
            "provider_id": {
                "type": "keyword"
            },
            "data_source_name": {
                "type": "keyword"
            },
            "filename": {
                "type": "keyword"
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
                        "type": "binary",
                        "store": True
                    }
                }
            },
        ],
        "properties": {
            "uniq_assoc_fields_hashdig": {
                "type": "keyword"
            },
            "json_doc_hashdig": {
                "type": "keyword"
            },
            "target_id": {
                "type": "keyword"
            },
            "disease_id": {
                "type": "keyword"
            },
            "data_source_name": {
                "type": "keyword"
            },
            "json_schema_version": {
                "type": "keyword"
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
    from mrtarget.common.DataStructure import RelationType
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

    association_data_mapping = {"settings": {"number_of_shards": evidence_shard_number,
                                             "number_of_replicas": evidence_replicas_number,
                                             # "index.store.type": "memory",
                                             "refresh_interval": "60s",
                                             "max_result_window": str(int(5e6)),
                                             },
                                "mappings": {
                                    Config.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME: {
                                        # "_routing": {"required": True,},
                                        "properties": {
                                            "target": {
                                                "properties": {
                                                    "id": {
                                                        "type": "keyword"
                                                    },
                                                    "gene_info": {
                                                        "properties": {
                                                            "name": {
                                                                "type": "keyword"
                                                            },
                                                            "symbol": {
                                                                "type": "keyword"
                                                            },
                                                        }
                                                    },
                                                    "activity": {
                                                        "type": "keyword"
                                                    },
                                                    "activity": {
                                                        "type": "keyword"
                                                    },

                                                }
                                            },
                                            "disease": {
                                                "properties": {
                                                    "id": {
                                                        "type": "keyword"
                                                    },
                                                    "efo_info": {
                                                        "properties": {
                                                            "label": {
                                                                "type": "keyword"
                                                            },
                                                            "therapeutic_area": {
                                                                "properties": {
                                                                    "label": {
                                                                        "type": "keyword"
                                                                    },
                                                                },
                                                            },
                                                        },
                                                    },
                                                }

                                            },
                                            "private": {
                                                "properties": {
                                                    "efo_codes": {
                                                        "type": "keyword"
                                                    },
                                                    "facets": {
                                                        "properties": {
                                                            "uniprot_keywords": {
                                                                "type": "keyword"
                                                            },
                                                            "reactome": {
                                                                "properties": {
                                                                    "pathway_type_code": {
                                                                        "type": "keyword"
                                                                    },
                                                                    "pathway_code": {
                                                                        "type": "keyword"
                                                                    },
                                                                }
                                                            },
                                                            "target_class": {
                                                                "properties": {
                                                                    "level1": {
                                                                        "properties": {
                                                                            "label": {
                                                                                "type": "keyword"
                                                                            },
                                                                        },
                                                                    },
                                                                    "level2": {
                                                                        "properties": {
                                                                            "label": {
                                                                                "type": "keyword"
                                                                            },
                                                                        },
                                                                    },

                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            },
                                        },
                                        "dynamic_templates": [{
                                            "label_not_indexed": {
                                                "match_mapping_type": "text",
                                                "path_match": "private.facets.expression_tissues.*.label",
                                                "mapping": {
                                                    "type": "keyword",
                                                    "index": "not_analyzed"
                                                }
                                            }
                                        }]
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
                        "min_gram": "2",
                        "max_gram": "20",
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
                # "properties": {
                #     "private": {
                #         # "type" : "object",
                #         "properties": {
                #             "suggestions": {
                #                 "type": "completion",
                #                 "analyzer": "whitespace_analyzer",
                #                 "search_analyzer": "whitespace_analyzer",
                #                 "payloads": True
                #             }
                #         }
                #     }
                # },
                "dynamic_templates": [
                    {
                        "do_not_analyze_ortholog": {
                            "match_mapping_type": "text",
                            "path_match": "ortholog*symbol",
                            "mapping": {
                                "type": "keyword",
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
            "number_of_shards": publication_shard_number,
            "number_of_replicas": publication_replicas_number,
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
                    },
                    "pub_date": {
                        "type": "date",
                        "format": "strict_date_optional_time||epoch_millis",
                    },
                    "title": {
                        "type": "text"

                    },
                    "abstract": {
                        "type": "text"
                    },
                },
                "dynamic_templates": [
                    {
                        "string_fields": {
                            "match": "*",
                            "match_mapping_type": "text",
                            "mapping": {
                                "omit_norms": True,
                                "type": "keyword",
                                "index": "not_analyzed"
                            }
                        }
                    }
                ],
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
            },
            "_default_": {
                "_all": {
                    "enabled": True
                },
                "dynamic_templates": [
                    {
                        "string_fields": {
                            "match": "*",
                            "match_mapping_type": "text",
                            "mapping": {
                                "index": "not_analyzed",
                                "omit_norms": True,
                                "type": "keyword"
                            }
                        }
                    }
                ],
            }
        }
    }

    literature_ent_mapping = {
        "settings": {
            "number_of_shards": publication_shard_number,
            "number_of_replicas": publication_replicas_number,
            "refresh_interval": "1s",
        },
        "mappings": {
            Config.ELASTICSEARCH_LITERATURE_ENTITY_DOC_NAME: {
                "properties": {
                                "id": {
                                    "type": "integer"
                                },
                                "label": {
                                    "type": "keyword"
                                },
                                "ent_type": {
                                    "type": "keyword"
                                },
                                "label": {
                                    "type": "keyword"
                                },
                                "matched_word": {
                                    "type": "keyword"
                                },
                                "start_pos": {
                                    "type": "integer"
                                },
                                "end_pos": {
                                    "type": "integer"
                                },
                                "doc_id": {
                                    "type": "integer"
                                }

                            }
                }
            }
    }

    INDEX_MAPPPINGS = {Config.ELASTICSEARCH_DATA_INDEX_NAME: evidence_data_mapping,
                       Config.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME: association_data_mapping,
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
                       Config.ELASTICSEARCH_LITERATURE_ENTITY_INDEX_NAME: literature_ent_mapping,
                       }
