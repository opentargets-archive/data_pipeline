import hashlib
import logging
import os
import json
import pypeln.process as pr
import addict
import codecs
import functools
import itertools

import opentargets_validator.helpers
import mrtarget.common.IO as IO

from mrtarget.Settings import Config

from mrtarget.common.EvidenceJsonUtils import DatatStructureFlattener
from mrtarget.common.EvidencesHelpers import make_validated_evs_obj, reduce_tuple_with_sum, setup_writers
from mrtarget.common.EvidenceString import EvidenceManager, Evidence
from mrtarget.common.LookupHelpers import LookUpDataRetriever, LookUpDataType


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
    logger = logging.getLogger(__name__ + '_' + str(os.getpid()))

    logger.debug("called validate_evidence on_start from %s", str(os.getpid()))

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


def process_evidences_pipeline(filenames, first_n, es_client, redis_client,
        dry_run, output_folder,
        num_workers, num_writers, max_queued_events, 
        eco_scores_uri, schema_uri, es_hosts, excluded_biotypes, 
        datasources_to_datatypes):
    logger = logging.getLogger(__name__)

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
    lookup_data = LookUpDataRetriever(es_client,
        redis_client, [], 
        ( LookUpDataType.TARGET, LookUpDataType.DISEASE,LookUpDataType.ECO)).lookup

    #create a iterable of lines from all file handles
    evs = IO.make_iter_lines(checked_filenames, first_n)

    #create functions with pre-baked arguments
    validation_on_start_baked = functools.partial(validation_on_start, 
        lookup_data, eco_scores_uri, schema_uri, excluded_biotypes, datasources_to_datatypes)

    writer_global_init, writer_local_init, writer_main, writer_local_shutdown, writer_global_shutdown = setup_writers(
        dry_run, es_hosts, output_folder)
    if writer_global_init:
        writer_global_init()

    #here is the pipeline definition
    pl_stage = pr.map(process_evidence, evs, 
        workers=num_workers, maxsize=max_queued_events,
        on_start=validation_on_start_baked)

    pl_stage = pr.map(writer_main, pl_stage, 
        workers=num_writers, maxsize=max_queued_events,
        on_start=writer_local_init,
        on_done=writer_local_shutdown)

    logger.info('run evidence processing pipeline')
    results = reduce_tuple_with_sum(pr.to_iterable(pl_stage))

    #perform any single-thread cleanup
    if writer_global_shutdown:
        writer_global_shutdown()

    logger.info("results (failed: %s, succeed: %s)", results[0], results[1])
    if failed_filenames:
        raise RuntimeError('unable to handle %s', str(failed_filenames))

    if not results[1]:
        raise RuntimeError("No evidence was sucessful!")

