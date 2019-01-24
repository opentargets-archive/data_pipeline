from mrtarget.Settings import Config
from mrtarget.constants import Const
from addict import Dict


def _get_evidence_string_generic_mapping():
    mmap = Dict()
    # it was previously required True
    # better approach
    # mmap._routing.required = True
    # mmap._routing.path = "<target_path>" # to change to a real path

    mmap._routing.required = False
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
        dass]

    return mmap.to_dict()

def _get_relation_generic_mapping():
    mmap = Dict()
    mmap._routing.required = False
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


def _generate_ngram_filter():
    '''generate a edge ngram filter and return the dictionary to add
    as a filter for an analyzer filter list
    '''
    ngram = Dict()
    # ngram.type = 'edgeNGram'
    ngram.type = 'ngram'
    ngram.min_gram = '4'
    ngram.max_gram = '10'
    ngram.token_chars = ['letter', 'digit', 'punctuation', 'symbol']

    return ngram.to_dict()


def _generate_word_delimiter_filter():
    '''generate a edge ngram filter and return the dictionary to add
    as a filter for an analyzer filter list
    '''
    wordd = Dict()
    wordd.type = 'word_delimiter'
    wordd.catenate_words = False
    wordd.catenate_numbers = False
    wordd.generate_word_parts = True
    wordd.generate_number_parts = True
    wordd.catenate_all = False
    wordd.split_on_case_change = False
    wordd.split_on_numerics = False
    wordd.preserve_original = True
    wordd.stem_english_possessive = True

    return wordd.to_dict()


def _generate_ngram_analyzer(withFilters=[]):
    '''generate a custom analyzer ready to inject in a dict of
    analizers
    '''
    ngram = Dict()
    ngram.type = 'custom'
    ngram.tokenizer = 'whitespace'
    ngram.filter = withFilters

    return ngram.to_dict()


def _generate_1chunk_analyzer():
    '''generate a custom analyzer ready to inject in a dict of
    analizers
    '''
    ochunk = Dict()
    ochunk.type = 'custom'
    ochunk.tokenizer = 'keyword'
    ochunk.filter = ["lowercase", "asciifolding", "simple_filter", "fingerprint"]

    return ochunk.to_dict()


class ElasticSearchConfiguration():
    available_databases = Config().DATASOURCE_TO_DATATYPE_MAPPING.keys()
    available_databases.append('other')

    generic_shard_number = '9'
    generic_replicas_number = '0'
    evidence_shard_number = '9'
    evidence_replicas_number = '0'
    relation_shard_number = '9'
    relation_replicas_number = '0'
    publication_shard_number = '9'
    publication_replicas_number = '0'
    validation_shard_number = '1'
    validation_replicas_number = '0'
    index_storage_type = "niofs"
    bulk_load_chunk = 1000

    unip = Dict()
    unip.mappings[Const.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.type = 'binary'
    unip.mappings[Const.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.index = False
    unip.mappings[Const.ELASTICSEARCH_UNIPROT_DOC_NAME].properties.entry.store = True
    unip.settings.number_of_shards = generic_shard_number
    unip.settings.number_of_replicas = generic_replicas_number
    unip.settings.refresh_interval = '60s'

    uniprot_data_mapping = unip.to_dict()

    ecom = Dict()

    ecom.mappings[Const.ELASTICSEARCH_ECO_DOC_NAME].properties.code.type = 'keyword'
    ecom.mappings[Const.ELASTICSEARCH_ECO_DOC_NAME].properties.path_codes.type = 'keyword'
    ecom.mappings[Const.ELASTICSEARCH_ECO_DOC_NAME].properties.path.properties.uri.type = 'keyword'
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
    efom.mappings[Const.ELASTICSEARCH_EFO_LABEL_DOC_NAME].properties = efomp

    efom.settings.number_of_shards = generic_shard_number
    efom.settings.number_of_replicas = generic_replicas_number
    efom.settings.refresh_interval = '60s'
    efom.settings.analysis.filter.edgeNGram_filter = _generate_ngram_filter()
    efom.settings.analysis.filter.wordDelimiter_filter = _generate_word_delimiter_filter()

    efom.settings.analysis.filter.simple_filter.type = 'standard'
    efom.settings.analysis.filter.simple_filter.token_chars = ['letter', 'digit']

    efom.settings.analysis.analyzer.edgeNGram_analyzer = \
        _generate_ngram_analyzer(withFilters=['lowercase', 'wordDelimiter_filter', 'edgeNGram_filter'])

    efom.settings.analysis.analyzer.onechunk_analyzer = _generate_1chunk_analyzer()
    efom.settings.analysis.analyzer.whitespace_analyzer.type = 'custom'
    efom.settings.analysis.analyzer.whitespace_analyzer.tokenizer = 'whitespace'
    efom.settings.analysis.analyzer.whitespace_analyzer.filter = ["lowercase", "wordDelimiter_filter"]
                                                                 #"simple_filter"]

    efo_data_mapping = efom.to_dict()

    gene_data_mapping = {
        "settings": {
            "number_of_shards": generic_shard_number,
            "number_of_replicas": generic_replicas_number,
            # "index.store.type": index_storage_type,
            "refresh_interval": "60s",
            "analysis": {
                "filter": {
                    "edgeNGram_filter": _generate_ngram_filter(),
                    "simple_filter": {
                        "type": "standard",
                        "token_chars": [
                            "letter",
                            "digit"
                        ]
                    },
                    "wordDelimiter_filter": _generate_word_delimiter_filter()
                },
                "analyzer": {
                    "edgeNGram_analyzer":
                        _generate_ngram_analyzer(withFilters=['lowercase', 'asciifolding',
                                                              'wordDelimiter_filter', 'edgeNGram_filter']),
                    "whitespace_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "asciifolding",
                            "wordDelimiter_filter"]
                            # "simple_filter"]
                    },
                    "onechunk_analyzer": _generate_1chunk_analyzer()
                }
            }
        },
        "mappings": {
            Const.ELASTICSEARCH_GENE_NAME_DOC_NAME: {
                "properties": {
                    "protein_class": {
                        "properties": {
                            "label": {
                                "type": "keyword"
                            }
                        }
                    },
                    "_private": {
                        "properties": {
                            "facets": {
                                "properties": {
                                    "reactome": {
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
            },
        }
    }

    expm = Dict()
    expm.settings.number_of_shards = generic_shard_number
    expm.settings.number_of_replicas = generic_replicas_number
    expm.settings.refresh_interval = '60s'
    expm.mappings[Const.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.gene.type = 'keyword'
    expm.mappings[Const.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.tissues.properties.efo_code.type = 'keyword'
    expm.mappings[Const.ELASTICSEARCH_EXPRESSION_DOC_NAME].properties.tissues.properties.rna.properties.value.type = 'float'

    expression_data_mapping = expm.to_dict()


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
                                          # "index.store.type": index_storage_type,
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
        relation_mappings[Const.ELASTICSEARCH_RELATION_DOC_NAME + '-' + rt] = _get_relation_generic_mapping()

    relation_data_mapping = {"settings": {"number_of_shards": relation_shard_number,
                                          "number_of_replicas": relation_replicas_number,
                                          "refresh_interval": "60s",
                                          },
                             "mappings": relation_mappings,
                             }

    validated_data_settings_and_mappings = {"settings": {"number_of_shards": validation_shard_number,
                                                         "number_of_replicas": validation_replicas_number,
                                                         "refresh_interval": "60s",
                                                         },
                                            "mappings": {"_default_": validated_data_mapping},

                                            }

    association_data_mapping = {"settings": {"number_of_shards": evidence_shard_number,
                                             "number_of_replicas": evidence_replicas_number,
                                             "refresh_interval": "60s",
                                             "max_result_window": str(int(5e6)),
                                             },
                                "mappings": {
                                    Const.ELASTICSEARCH_DATA_ASSOCIATION_DOC_NAME: {
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
                                                    }
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
                                                            "tractability": {
                                                                "properties": {
                                                                    "antibody": {
                                                                        "type": "keyword"
                                                                    },
                                                                    "smallmolecule": {
                                                                        "type": "keyword"
                                                                    },
                                                                    "combined": {
                                                                        "type": "keyword"
                                                                    }
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
            # "index.store.type": index_storage_type,
            "refresh_interval": "60s",
            "analysis": {
                "filter": {
                    "edgeNGram_filter": _generate_ngram_filter(),
                    "wordDelimiter_filter": _generate_word_delimiter_filter()
                    }
                ,
                "analyzer": {
                    "edgeNGram_analyzer": _generate_ngram_analyzer(withFilters=['lowercase', #'asciifolding',
                                                                                'wordDelimiter_filter', 'edgeNGram_filter']),
                    "whitespace_analyzer": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            # "asciifolding",
                            "wordDelimiter_filter"]
                    },
                    "default": {
                        "type": "custom",
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "wordDelimiter_filter"]
                    }
                }
            }
        },
        "mappings": {
            '_default_': {
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
            }
        }
    }
    INDEX_MAPPPINGS = {Const.ELASTICSEARCH_DATA_INDEX_NAME: evidence_data_mapping,
                       Const.ELASTICSEARCH_DATA_ASSOCIATION_INDEX_NAME: association_data_mapping,
                       Const.ELASTICSEARCH_EFO_LABEL_INDEX_NAME: efo_data_mapping,
                       Const.ELASTICSEARCH_ECO_INDEX_NAME: eco_data_mapping,
                       Const.ELASTICSEARCH_GENE_NAME_INDEX_NAME: gene_data_mapping,
                       Const.ELASTICSEARCH_EXPRESSION_INDEX_NAME: expression_data_mapping,
                       Const.ELASTICSEARCH_DATA_SEARCH_INDEX_NAME: search_obj_data_mapping,
                       Const.ELASTICSEARCH_VALIDATED_DATA_INDEX_NAME: validated_data_settings_and_mappings,
                       Const.ELASTICSEARCH_UNIPROT_INDEX_NAME: uniprot_data_mapping,
                       Const.ELASTICSEARCH_RELATION_INDEX_NAME: relation_data_mapping,
                       }
