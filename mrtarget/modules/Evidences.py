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
from mrtarget.common.EvidencesHelpers import (ProcessContext, make_lookup_data,
                                              make_validated_evs_obj, open_writers_on_start,
                                              close_writers_on_done, reduce_tuple_with_sum)
from mrtarget.common.connection import new_redis_client
from mrtarget.modules.EvidenceString import EvidenceManager, Evidence


def fix_and_score_evidence(validated_evs, process_context):
    """take line as a dict, convert into an evidence object and apply a list of modifiers:
    fix_evidence, and if valid then score_evidence, extend data and inject loci
    """
    left, right = None, None
    ev = Evidence(validated_evs.line)

    (fixed_ev, _) = process_context.kwargs.evidence_manager.fix_evidence(ev)

    (is_valid, problem_str) = \
        process_context.kwargs.evidence_manager.check_is_valid_evs(fixed_ev, datasource=fixed_ev.datasource)
    if is_valid:
        # add scoring to evidence string
        fixed_ev.score_evidence(process_context.kwargs.evidence_manager.score_modifiers)

        # extend data in evidencestring
        fixed_ev_ext = process_context.kwargs.evidence_manager.get_extended_evidence(fixed_ev)

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


def process_evidence(line, process_context):
    # validate evidence
    (left, right) = validate_evidence(line, process_context)

    # fix evidence
    if right is not None:
        # ev comes as addict.Dict
        # too much code at the moment to move evidences to addict
        (left, right) = fix_and_score_evidence(right, process_context)

    return left, right


def process_evidence_on_start(luts):
    """this function is called once per started process and return a ProcessContext per process."""
    pc = ProcessContext()
    pc.logger.debug("called validate_evidence on_start from %s", str(os.getpid()))
    pc.logger.debug("creating schema validators")

    schemas_map = Config.EVIDENCEVALIDATION_VALIDATOR_SCHEMAS
    for schema_name, schema_uri in schemas_map.iteritems():
        # per kv we create the validator and instantiate it
        pc.logger.info('generate_validator_from_schema %s using the uri %s', schema_name, schema_uri)
        pc.kwargs.validators[schema_name] = \
            opentargets_validator.helpers.generate_validator_from_schema(schema_uri)

    pc.kwargs.luts = luts
    pc.kwargs.redis_c = new_redis_client()
    pc.kwargs.luts.set_r_server(pc.kwargs.redis_c)
    pc.kwargs.evidence_manager = EvidenceManager(pc.kwargs.luts)
    return pc


def process_evidence_on_done(_status, process_context):
    """this is called once when the process is finished computing. It is
    potentially useful for closing sockets or fds"""
    process_context.logger.debug("called validate_evidence on_done from %s", str(os.getpid()))


def write_evidences_on_done(_status, process_context):
    process_context.close()


def validate_evidence(line, process_context):
    """this function is called once per line until number of lines is exhausted. process_context
    is the object built and returned by its start function

    It returns a tuple with (left, right) where left is the faulty line and the
    right is the fully validated and processed. There is a specific case where you
    get (None, None) which means we are not quetting the right expected input
    """
    if not line or line is None or len(line) != 2:
        process_context.logger.error('line != triple and this is weird as if any line you must have a triple')
        return None, None

    (filename, (line_n, l)) = line
    decoded_line = codecs.decode(l, 'utf-8', 'replace')
    validated_evs = make_validated_evs_obj(filename=filename, hash='', line=decoded_line, line_n=line_n)

    try:
        data_type = None
        data_source = None
        parsed_line = None
        parsed_line_bad = None

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

        if data_type not in Config.EVIDENCEVALIDATION_DATATYPES:
            validated_evs.explanation_type = 'unsupported_datatype'
            validated_evs.explanation_str = data_type
            return validated_evs, None

        if 'sourceID' not in parsed_line:
            validated_evs.explanation_type = 'missing_datasource'
            return validated_evs, None

        data_source = parsed_line['sourceID']
        validated_evs.data_source = data_source

        if data_source not in Config.DATASOURCE_TO_DATATYPE_MAPPING:
            validated_evs.explanation_type = 'unsupported_datasource'
            validated_evs.explanation_str = data_source
            return validated_evs, None

        # validate line
        validation_errors = \
            [str(e) for e in process_context.kwargs.validators[data_type].iter_errors(parsed_line)]

        if validation_errors:
            # here I have to log all fails to logger and elastic
            error_messages = ' '.join(validation_errors).replace('\n', ' ; ').replace('\r', '')

            error_messages_len = len(error_messages)

            # capping error message to 2048
            error_messages = error_messages if error_messages_len <= 2048 \
                else error_messages[:2048] + ' ; ...'

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
            if efo_id not in process_context.kwargs.luts.available_efos:
                validated_evs.explanation_type = 'invalid_disease'
                validated_evs.explanation_str = efo_id
                disease_failed = True
        else:
            validated_evs.explanation_type = 'missing_disease'
            disease_failed = True

        # CHECK GENE/PROTEIN IDENTIFIER Check Ensembl ID, UniProt ID
        # and UniProt ID mapping to a Gene ID
        # http://identifiers.org/ensembl/ENSG00000178573
        if target_id:
            if 'ensembl' in target_id:
                ensembl_id = target_id.split('/')[-1]
                if not ensembl_id in process_context.kwargs.luts.available_genes:
                    validated_evs.explanation_type = 'invalid_target'
                    validated_evs.explanation_str = ensembl_id
                    target_failed = True

                elif ensembl_id in process_context.kwargs.luts.non_reference_genes:
                    process_context.logger.warning('nonref ensembl gene found %s line_n %d filename %s',
                                                   ensembl_id, line_n, filename)

            elif 'uniprot' in target_id:
                uniprot_id = target_id.split('/')[-1]
                if uniprot_id not in process_context.kwargs.luts.uni2ens:
                    validated_evs.explanation_type = 'unknown_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True

                elif not process_context.kwargs.luts.uni2ens[uniprot_id]:
                    validated_evs.explanation_type = 'missing_ensembl_xref_for_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True

                elif (uniprot_id in process_context.kwargs.luts.uni2ens) and \
                        process_context.kwargs.luts.uni2ens[uniprot_id] in process_context.kwargs.luts.available_genes and \
                        'is_reference' in process_context.kwargs.luts.available_genes[process_context.kwargs.luts.uni2ens[uniprot_id]] and \
                        (not process_context.kwargs.luts.available_genes[process_context.kwargs.luts.uni2ens[uniprot_id]]['is_reference'] is True):
                    validated_evs.explanation_type = 'nonref_ensembl_xref_for_uniprot_entry'
                    validated_evs.explanation_str = uniprot_id
                    target_failed = True
                else:
                    try:
                        reference_target_list = \
                            process_context.kwargs.luts.available_genes[process_context.kwargs.luts.uni2ens[uniprot_id]]['is_reference'] is True
                    except KeyError:
                        reference_target_list = []

                    if reference_target_list:
                        target_id = 'http://identifiers.org/ensembl/%s' % reference_target_list[0]
                    else:
                        target_id = process_context.kwargs.luts.uni2ens[uniprot_id]
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


def write_evidences(x, process_context):

    is_right = 0
    is_left = 0

    try:
        (left, right) = x
        if right is not None:
            process_context.put(x)
            is_right = 1
        elif left is not None:
            process_context.put(x)
            is_left = 1
    except Exception as e:
        process_context.logger.exception(e)
    finally:
        return is_left, is_right


def process_evidences_pipeline(filenames, first_n, es_client, redis_client,
                               dry_run, enable_output_to_es, output_folder,
                               num_workers, num_writers, max_queued_events):
    logger = logging.getLogger(__name__)

    if not filenames:
        logger.error('tried to run with no filenames at all')
        raise RuntimeError("Must specify at least one filename of evidence")

    # files that are not fetchable
    failed_filenames = list(itertools.ifilterfalse(IO.check_to_open, filenames))

    for uri in failed_filenames:
        logger.warning('failed to fetch uri %s', uri)

    # get the filenames that are properly fetchable
    checked_filenames = list(set(filenames) - set(failed_filenames))

    logger.info('start evidence processing pipeline')

    logger.debug('load LUTs')
    lookup_data = make_lookup_data(es_client, redis_client)

    logger.debug('create a iterable of lines from all file handles')
    evs = IO.make_iter_lines(checked_filenames, first_n)

    logger.info('declare pipeline to run')
    write_evidences_on_start_f = functools.partial(open_writers_on_start, enable_output_to_es, output_folder, dry_run)
    validate_evidence_on_start_f = functools.partial(process_evidence_on_start, lookup_data)

    # here the pipeline definition
    pl_stage = pr.map(process_evidence, evs, workers=num_workers, maxsize=max_queued_events,
                      on_start=validate_evidence_on_start_f, on_done=process_evidence_on_done)
    pl_stage = pr.map(write_evidences, pl_stage, workers=num_writers, maxsize=max_queued_events,
                      on_start=write_evidences_on_start_f,
                      on_done=close_writers_on_done)

    logger.info('run evidence processing pipeline')
    results = reduce_tuple_with_sum(pr.to_iterable(pl_stage))

    logger.info("results (failed: %s, succeed: %s)", results[0], results[1])
    if failed_filenames:
        logger.warning('some filenames were missing or were not properly fetched %s', str(failed_filenames))

    if not results[1]:
        raise RuntimeError("No evidence was sucessful!")

