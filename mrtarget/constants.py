'''
This file is a place to list constants *that should never be changed* by a user.

If anything is more complicated than a simple variable (e.g. a function or object)
then it should not go here either.
'''


class Const:
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