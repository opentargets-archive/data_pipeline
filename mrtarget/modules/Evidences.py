import hashlib
import logging
import os
import simplejson as json
import pypeln.process as pr
import addict
import codecs
import functools
import itertools

import elasticsearch

import opentargets_validator.helpers
import mrtarget.common.IO as IO

from mrtarget.common.connection import new_es_client
from mrtarget.common.esutil import ElasticsearchBulkIndexManager
from mrtarget.common.EvidenceJsonUtils import DatatStructureFlattener
from mrtarget.common.EvidenceString import EvidenceManager, Evidence
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType
from opentargets_urlzsource import URLZSource

def make_validated_evs_obj(filename, hash, line, line_n, is_valid=False, explanation_type='', explanation_str='',
                           target_id=None, efo_id=None, data_type=None, id=None):
    return addict.Dict(is_valid=is_valid, explanation_type=explanation_type, explanation_str=explanation_str,
                       target_id=target_id, efo_id=efo_id, data_type=data_type, id=id, line=line, line_n=line_n,
                       filename=filename, hash=hash)


def fix_and_score_evidence(validated_evs, datasources_to_datatypes, evidence_manager):
    """take line as a dict, convert into an evidence object and apply a list of modifiers:
    fix_evidence, and if valid then score_evidence, extend data and inject loci
    """
    left, right = None, None
    ev = Evidence(validated_evs.line, datasources_to_datatypes)

    (fixed_ev, _) = evidence_manager.fix_evidence(ev)

    (is_valid, problem_str) = evidence_manager.check_is_valid_evs(fixed_ev, 
        datasource=fixed_ev.datasource)
    if is_valid:
        # add scoring to evidence string
        fixed_ev.score_evidence(evidence_manager.score_modifiers)

        # extend data in evidencestring
        fixed_ev_ext = evidence_manager.get_extended_evidence(fixed_ev)

        validated_evs.is_valid = True
        validated_evs.line = fixed_ev_ext.to_json()
        right = validated_evs

    else:
        validated_evs.explanation_type = 'invalid_fixed_evidence'
        validated_evs.explanation_str = problem_str
        validated_evs.is_valid = False
        left = validated_evs

    # return either left or right
    return left, right


def process_evidence(line, logger, validator, luts, datasources_to_datatypes, evidence_manager):
    # validate evidence
    (left, right) = validate_evidence(line, logger, validator, luts, datasources_to_datatypes)

    # fix evidence 
    if right is not None:
        # ev comes as addict.Dict
        # too much code at the moment to move evidences to addict
        (left, right) = fix_and_score_evidence(right, datasources_to_datatypes, evidence_manager)

    return left, right


"""
This function is called once in each child process to do local setup for 
validation
"""
def validation_on_start(luts, eco_scores_uri, schema_uri, excluded_biotypes, 
        datasources_to_datatypes):
    logger = logging.getLogger(__name__)

    validator = opentargets_validator.helpers.generate_validator_from_schema(schema_uri)

    luts = luts
    datasources_to_datatypes = datasources_to_datatypes
    evidence_manager = EvidenceManager(luts, eco_scores_uri, 
        excluded_biotypes, datasources_to_datatypes)

    return logger, validator, luts, datasources_to_datatypes, evidence_manager

def validate_evidence(line, logger, validator, luts, datasources_to_datatypes):
    """this function is called once per line until number of lines is exhausted. 

    It returns a tuple with (left, right) where left is the faulty line and the
    right is the fully validated and processed. There is a specific case where you
    get (None, None) which means we are not quetting the right expected input
    """
    if not line or line is None or len(line) != 2:
        logger.error('line != triple and this is weird as if any line you must have a triple')
        return None, None

    (filename, (line_n, l)) = line
    decoded_line = codecs.decode(l, 'utf-8', 'replace')
    validated_evs = make_validated_evs_obj(filename=filename, hash='', line=decoded_line, line_n=line_n)

    try:
        data_type = None
        data_source = None
        parsed_line = None

        try:
            parsed_line = json.loads(decoded_line)
            validated_evs['id'] = str(DatatStructureFlattener(parsed_line).get_hexdigest())
        except Exception as e:
            validated_evs.explanation_type = 'unparseable_json'
            validated_evs['id'] = str(hashlib.md5(decoded_line).hexdigest())
            return validated_evs, None

        if 'label' in parsed_line or 'type' in parsed_line:
            # setting type from label in case we have label??
            if 'label' in parsed_line:
                parsed_line['type'] = parsed_line.pop('label', None)

            data_type = parsed_line['type']
            validated_evs.data_type = data_type

        else:
            validated_evs.explanation_type = 'key_fields_missing'
            return validated_evs, None

        if data_type is None:
            validated_evs.explanation_type = 'missing_datatype'
            return validated_evs, None

        if 'sourceID' not in parsed_line:
            validated_evs.explanation_type = 'missing_datasource'
            return validated_evs, None

        data_source = parsed_line['sourceID']
        validated_evs.data_source = data_source

        if data_source not in datasources_to_datatypes:
            validated_evs.explanation_type = 'unsupported_datasource'
            validated_evs.explanation_str = data_source
            return validated_evs, None

        # validate line
        validation_errors = \
            [str(e) for e in validator.iter_errors(parsed_line)]

        if validation_errors:
            # here I have to log all fails to logger and elastic
            error_messages = ' '.join(validation_errors).replace('\n', ' ; ').replace('\r', '')

            validated_evs.explanation_type = 'validation_error'
            validated_evs.explanation_str = error_messages

            return validated_evs, None

        target_id = None
        efo_id = None
        # generate fantabulous dict from addict
        evidence_obj = addict.Dict(parsed_line)
        evidence_obj.unique_association_fields['datasource'] = data_source

        if evidence_obj.target.id:
            target_id = evidence_obj.target.id
            validated_evs.target_id = evidence_obj.target.id
        if evidence_obj.disease.id:
            efo_id = evidence_obj.disease.id
            validated_evs.efo_id = evidence_obj.disease.id

        # flatten but is it always valid unique_association_fields?
        validated_evs.hash = \
            DatatStructureFlattener(evidence_obj.unique_association_fields).get_hexdigest()
        evidence_obj['id'] = str(validated_evs.hash)

        disease_failed = False
        target_failed = False

        if efo_id:
            # Check disease term or phenotype term
            #redis/elasticsearch is based on short ontology id, not full iri
            if '/' in efo_id:
                short_efo_id = luts.available_efos.get_ontology_code_from_url(efo_id)
            else:
                #handle being given a short id to start with
                short_efo_id = efo_id

            #if its not in the efo lookup table, fail
            if short_efo_id not in luts.available_efos:
                validated_evs.explanation_type = 'invalid_disease'
                validated_evs.explanation_str = efo_id
                disease_failed = True
        else:
            #disease is missing entirely
            #should never happen because it will fail validation, but...
            validated_evs.explanation_type = 'missing_disease'
            disease_failed = True

        # CHECK GENE/PROTEIN IDENTIFIER Check Ensembl ID, UniProt ID
        # and UniProt ID mapping to a Gene ID
        # http://identifiers.org/ensembl/ENSG00000178573
        if target_id:
            if 'ensembl' in target_id:
                ensembl_id = target_id.split('/')[-1]
                if not ensembl_id in luts.available_genes:
                    validated_evs.explanation_type = 'invalid_target'
                    validated_evs.explanation_str = ensembl_id
                    target_failed = True

                elif ensembl_id in luts.non_reference_genes:
                    logger.warning('nonref ensembl gene found %s line_n %d filename %s',
                                                   ensembl_id, line_n, filename)

            elif 'uniprot' in target_id:
                uniprot_id = target_id.split('/')[-1]
                if uniprot_id not in luts.uni2ens:
                    validated_evs.explanation_type = 'unknown_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True

                elif not luts.uni2ens[uniprot_id]:
                    validated_evs.explanation_type = 'missing_ensembl_xref_for_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True

                elif (uniprot_id in luts.uni2ens) and \
                        luts.uni2ens[uniprot_id] in luts.available_genes and \
                        'is_reference' in luts.available_genes[luts.uni2ens[uniprot_id]] and \
                        (not luts.available_genes[luts.uni2ens[uniprot_id]]['is_reference'] is True):
                    validated_evs.explanation_type = 'nonref_ensembl_xref_for_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True
                else:
                    try:
                        reference_target_list = luts.available_genes[luts.uni2ens[uniprot_id]]['is_reference'] is True
                    except KeyError:
                        reference_target_list = []

                    if reference_target_list:
                        target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[0]
                    else:
                        target_id =luts.uni2ens[uniprot_id]
                    if target_id is None:
                        validated_evs.explanation_type = 'missing_target_id_for_protein'
                        validated_evs.explanation_str = uniprot_id
                        target_failed = True

        # If there is no target id after the processing step
        if target_id is None:
            validated_evs.explanation_type = 'missing_target_id'
            target_failed = True

        if target_failed or disease_failed:

            if target_failed and disease_failed:
                validated_evs.explanation_type = 'target_id_and_disease_id'
                validated_evs.explanation_str = ''

            return validated_evs, None

        validated_evs.line = json.dumps(evidence_obj.to_dict())
        validated_evs.is_valid = True
        return None, validated_evs

    except Exception as e:
        validated_evs.explanation_type = 'exception'
        validated_evs.explanation_str = str(e)
        return validated_evs, None

"""
Generates elasticsearch action objects from the results iterator

Output suitable for use with elasticsearch.helpers 
"""
def elasticsearch_actions(lines, index_valid, index_invalid, doc_valid, doc_invalid):
    for line in lines:
        (left, right) = line
        if right is not None:
            #valid
            action = {}
            action["_index"] = index_valid
            action["_type"] = doc_valid
            action["_id"] = right['hash']
            action["_source"] = right['line']
            #print("  valid %s" % action["_id"])
            yield action
        elif left is not None:
            #invalid
            action = {}
            action["_index"] = index_invalid
            action["_type"] = doc_invalid
            action["_id"] = left['id']
            action["_source"] = left
            #print("invalid %s" % action["_id"])
            yield action

def process_evidences_pipeline(filenames, first_n, 
        es_hosts, es_index_valid, es_index_invalid, es_doc_valid, es_doc_invalid, 
        es_mappings_valid, es_mappings_invalid, 
        es_settings_valid, es_settings_invalid, 
        es_index_gene, es_index_eco, es_index_efo,
        redis_client,
        dry_run, workers_validation, queue_validation, workers_write, queue_write, 
        eco_scores_uri, schema_uri, excluded_biotypes, 
        datasources_to_datatypes):

    logger = logging.getLogger(__name__)
    es = new_es_client(es_hosts)

    if not filenames:
        logger.error('tried to run with no filenames at all')
        raise RuntimeError("Must specify at least one filename of evidence")

    # files that are not fetchable
    failed_filenames = list(itertools.ifilterfalse(IO.check_to_open, filenames))

    for uri in failed_filenames:
        logger.warning('failed to fetch uri %s', uri)

    # get the filenames that are properly fetchable
    #sort the list for consistent behaviour
    checked_filenames = sorted((set(filenames) - set(failed_filenames)))

    logger.info('start evidence processing pipeline')

    #load lookup tables
    lookup_data = LookUpDataRetriever(es, redis_client, 
        ( 
            LookUpDataType.TARGET, 
            LookUpDataType.DISEASE, 
            LookUpDataType.ECO 
        ),
        gene_index=es_index_gene,
        eco_index=es_index_eco,
        efo_index=es_index_efo).lookup

    #create a iterable of lines from all file handles
    evs = IO.make_iter_lines(checked_filenames, first_n)

    #create functions with pre-baked arguments
    validation_on_start_baked = functools.partial(validation_on_start, 
        lookup_data, eco_scores_uri, schema_uri, excluded_biotypes, datasources_to_datatypes)

    #here is the pipeline definition
    pl_stage = pr.map(process_evidence, evs, 
        workers=workers_validation, maxsize=queue_validation,
        on_start=validation_on_start_baked)

    logger.info('stages created, running scoring and writing')

    with URLZSource(es_mappings_valid).open() as mappings_file:
        mappings_valid = json.load(mappings_file)

    with URLZSource(es_mappings_invalid).open() as mappings_file:
        mappings_invalid = json.load(mappings_file)

    with URLZSource(es_settings_valid).open() as settings_file:
        settings_valid = json.load(settings_file)

    with URLZSource(es_settings_invalid).open() as settings_file:
        settings_invalid = json.load(settings_file)

    with ElasticsearchBulkIndexManager(es, es_index_valid, settings_valid, mappings_valid):
        with ElasticsearchBulkIndexManager(es, es_index_invalid, settings_invalid, mappings_invalid):

            #load into elasticsearch
            chunk_size = 1000 #TODO make configurable
            actions = elasticsearch_actions(pl_stage, 
                es_index_valid, es_index_invalid, 
                es_doc_valid, es_doc_invalid)
            failcount = 0

            if not dry_run:
                results = None
                if workers_write > 0:
                    # this can silently crash ?
                    results = elasticsearch.helpers.parallel_bulk(es, actions,
                            thread_count=workers_write,
                            queue_size=queue_write, 
                            chunk_size=chunk_size)
                else:
                    results = elasticsearch.helpers.streaming_bulk(es, actions,
                            chunk_size=chunk_size)
                for success, details in results:
                    if not success:
                        failcount += 1

                if failcount:
                    raise RuntimeError("%s relations failed to index" % failcount)

            logger.info('stages created, ran scoring and writing')


    if failed_filenames:
        raise RuntimeError('unable to handle %s', str(failed_filenames))


